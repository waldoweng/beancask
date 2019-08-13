package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	beancaskError "github.com/waldoweng/beancask/errors"
)

const activeFileName string = "activeFile.dat"
const dataFileNamePattern string = "dataFile.%d.dat"
const dataFileNameMatchPattern string = "dataFile.*.dat"
const compationFileName string = "compactionFile.dat"

// HashItem struct of hash table entry
type HashItem struct {
	Wal     WALFile
	Len     int
	Offset  int64
	Tmstamp int64
}

// BHashTable interface for bitcask hash table. it's used to find the location of
// the record in current version in wal file on disk, and to support the read
// request with only one disk seek and one disk read per request
type BHashTable interface {
	Get(key string) (h HashItem, err error)
	Set(key string, h HashItem) error
	Del(key string) error
	Exists(key string) (exists bool)
	IteratorItem() <-chan struct {
		key   string
		value HashItem
	}
}

// Record struct of record of the bitcask wal file
type Record struct {
	Crc     uint32
	Tmstamp int64
	Ksz     int64
	ValSz   int64
	Key     []byte
	Value   []byte
}

// CreateTomStoneRecord create a tomb stone record for key
func CreateTomStoneRecord(key string) Record {
	return Record{
		Crc:     0,
		Tmstamp: time.Now().UnixNano(),
		Ksz:     int64(len(key)),
		ValSz:   0,
		Key:     []byte(key),
		Value:   []byte(""),
	}
}

func (r *Record) fromBuffer(buf *bytes.Buffer) error {
	err := binary.Read(buf, binary.LittleEndian, &r.Crc)
	err = binary.Read(buf, binary.LittleEndian, &r.Tmstamp)
	err = binary.Read(buf, binary.LittleEndian, &r.Ksz)
	err = binary.Read(buf, binary.LittleEndian, &r.ValSz)

	r.Key = make([]byte, r.Ksz)
	r.Value = make([]byte, r.ValSz)
	err = binary.Read(buf, binary.LittleEndian, &r.Key)
	err = binary.Read(buf, binary.LittleEndian, &r.Value)

	if err != nil {
		return err
	}
	return nil
}

func (r *Record) toBuffer(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, r.Crc)
	err = binary.Write(buf, binary.LittleEndian, r.Tmstamp)
	err = binary.Write(buf, binary.LittleEndian, r.Ksz)
	err = binary.Write(buf, binary.LittleEndian, r.ValSz)
	err = binary.Write(buf, binary.LittleEndian, r.Key[0:r.Ksz])
	err = binary.Write(buf, binary.LittleEndian, r.Value[0:r.ValSz])

	if err != nil {
		return err
	}
	return nil
}

func (r *Record) size() int64 {
	return 28 + r.Ksz + r.ValSz
}

func (r *Record) isTomeStone() bool {
	return r.ValSz == 0
}

// WALFile interface for bitcask wal file. It's used to contain the record
// on the disk to support durability and large writing throughput by appending
// new value to the end of itself without modifing the old value. when it gets
// large enough, it becomes read-only and records will be written to a
// new wal file (which is called the active one).
type WALFile interface {
	RenameFile(name string) error
	RemoveFile() error
	CloseFile(remove bool) error
	Deactivate() error
	Sync() error
	AppendRecord(r Record, sync bool) (off int64, len int, err error)
	ReadRecord(off int64, len int, r *Record) error
	IteratorRecord() <-chan struct {
		offset int
		r      Record
	}
}

// BitcaskInstance struct of a bitcask instance. bitcask consist of many instances where
// only one instance is the active instance (allow reading and writing both) and others
// are record archive where not active (allow reading only)
type bitcaskInstance struct {
	hashTable BHashTable
	walFile   WALFile
}

// loadData construct a BitcaskInstance struct by reading a wal file on disk when system
// starting up. return nil if success
func (b *bitcaskInstance) loadData() error {
	for re := range b.walFile.IteratorRecord() {
		offset, r := re.offset, re.r
		if r.isTomeStone() {
			continue
		}
		h := HashItem{
			Wal:     b.walFile,
			Len:     int(r.size()),
			Offset:  int64(offset),
			Tmstamp: r.Tmstamp,
		}
		err := b.hashTable.Set(string(r.Key), h)
		if err != nil {
			log.Fatalf("build hash table fail, system memory maybe insufficient")
			return beancaskError.ErrorSystemInternal
		}
	}
	return nil
}

// mergeHash merge hash tables of two BitcaskInstance by discarding entries of obsoletely record
// if success return the new instance and nil
func (b *bitcaskInstance) mergeHash(ins *bitcaskInstance) error {
	var err error
	for item := range ins.hashTable.IteratorItem() {
		if !b.hashTable.Exists(item.key) {
			err = b.hashTable.Set(item.key, item.value)
			if err != nil {
				log.Fatalf("set hash table fail err:%s\n", err.Error())
				return err
			}
		} else {
			value, err := b.hashTable.Get(item.key)
			if err != nil {
				log.Fatalf("get hash table fail err:%s\n", err.Error())
				return err
			}

			if value.Tmstamp < item.value.Tmstamp {
				err = b.hashTable.Set(item.key, item.value)
				if err != nil {
					log.Fatalf("set hash table err:%s\n", err.Error())
					return err
				}
			}
		}
	}
	return nil
}

// dumpHashToFile dump all record referenced by hashTable to wal file
// if success return the new instance and nil
func (b *bitcaskInstance) dumpHashToFile() error {
	var record Record
	for item := range b.hashTable.IteratorItem() {
		err := item.value.Wal.ReadRecord(item.value.Offset, item.value.Len, &record)
		if err != nil {
			log.Fatalf("read record fail %s\n", err.Error())
			return err
		}

		offset, len, err := b.walFile.AppendRecord(record, false)
		if err != nil {
			log.Fatalf("write record fail %s\n", err.Error())
			return err
		}

		item.value.Wal, item.value.Offset, item.value.Len = b.walFile, offset, len
		err = b.hashTable.Set(item.key, item.value)
		if err != nil {
			log.Fatalf("update hash fail %s\n", err.Error())
			return err
		}
	}

	b.walFile.Sync()
	return nil
}

// CreateBitcaskInstance create a new BitcaskInstance by the name of the wal file
// if success return the new instance and nil
// if failed, return nil and error
func createBitcaskInstance(name string) (*bitcaskInstance, error) {
	hashTable := CreateSimpleHashTable()
	if hashTable == nil {
		log.Fatal("create hash table fail, system memory maybe insufficient")
		return nil, beancaskError.ErrorSystemInternal
	}

	walFile := CreateBitcaskLogFile(name)
	if walFile == nil {
		log.Fatal("create wal file fail, system disk maybe disfunctioning")
		return nil, beancaskError.ErrorSystemInternal
	}

	return &bitcaskInstance{hashTable, walFile}, nil
}

// Bitcask the bitcask storage struct
type Bitcask struct {
	wchan chan struct {
		key    string
		record Record
		result chan error
	}
	mutex          *sync.RWMutex
	compacting     int32
	activeInstance *bitcaskInstance
	instances      []*bitcaskInstance
}

// NewBitcask create a new Bitcask struct
func NewBitcask() *Bitcask {
	b := Bitcask{
		mutex:          new(sync.RWMutex),
		compacting:     0,
		activeInstance: nil,
	}
	b.loadData()

	b.wchan = make(chan struct {
		key    string
		record Record
		result chan error
	}, 1024)

	go b.realSet()

	return &b
}

// LoadData load all the data on disk and construct the Bitcask struct
// if success return nil
func (b *Bitcask) loadData() error {
	var err error

	// load active wal file
	b.activeInstance, err = createBitcaskInstance(activeFileName)
	if err != nil {
		log.Fatal("create active instance fail, system maybe disfunctioning")
		return nil
	}

	err = b.activeInstance.loadData()
	if err != nil {
		log.Fatalln("load active instance fail, system maybe disfunctioning")
		return nil
	}

	// load data files
	f, _ := os.Open(".")
	names, _ := f.Readdirnames(-1)
	var dataFileName []string
	for _, name := range names {
		matched, _ := regexp.Match(dataFileNameMatchPattern, []byte(name))
		if matched {
			dataFileName = append(dataFileName, name)
		}
	}

	for _, name := range dataFileName {
		ins, err := createBitcaskInstance(name)
		if err != nil {
			log.Fatal("create data instance fail, system maybe disfunctioning")
			return nil
		}
		err = ins.loadData()
		if err != nil {
			log.Fatal("create data instance fail, system maybe disfunctioning")
			return nil
		}
		b.instances = append(b.instances, ins)
	}

	return nil
}

// Get get the value of key
// if success return nil
// if data not exists, return beancask.errors.ErrorDataNotFound
func (b *Bitcask) Get(key string) (string, error) {
	b.mutex.RLock()
	defer func() { b.mutex.RUnlock() }()

	if b.wchan == nil {
		return "", beancaskError.ErrorSystemShuttingDown
	}

	if item, err := b.activeInstance.hashTable.Get(key); err == nil {
		var r Record
		err = b.activeInstance.walFile.ReadRecord(item.Offset, item.Len, &r)
		if err != nil {
			log.Fatal("read key [", key, "] from active file fail, data may be corrupted!")
			return "", beancaskError.ErrorSystemInternal
		}
		return string(r.Value[:]), nil
	}

	for i := len(b.instances) - 1; i >= 0; i-- {
		if item, err := b.instances[i].hashTable.Get(key); err == nil {
			var r Record
			err = b.instances[i].walFile.ReadRecord(item.Offset, item.Len, &r)
			if err != nil {
				log.Fatal("read key [", key, "] from active file fail, data may be corrupted!")
				return "", beancaskError.ErrorSystemInternal
			}
			return string(r.Value[:]), nil
		}
	}

	return "", beancaskError.ErrorDataNotFound
}

// Set the value of key
// if success return nil
func (b *Bitcask) Set(key string, value string) error {
	r := Record{
		Crc:     crc32.Checksum([]byte(value), crc32.MakeTable(crc32.Castagnoli)),
		Tmstamp: time.Now().UnixNano(),
		Ksz:     int64(len(key)),
		ValSz:   int64(len(value)),
		Key:     []byte(key),
		Value:   []byte(value),
	}

	resultChan := make(chan error, 1)
	defer func() { close(resultChan) }()

	err := func() error {
		b.mutex.Lock()
		defer func() { b.mutex.Unlock() }()

		if b.wchan == nil {
			return beancaskError.ErrorSystemShuttingDown
		}

		b.wchan <- struct {
			key    string
			record Record
			result chan error
		}{
			key:    key,
			record: r,
			result: resultChan,
		}

		return nil
	}()

	if err != nil {
		return err
	}

	return <-resultChan
}

// Delete the value of key
// if success return nil
func (b *Bitcask) Delete(key string) error {
	resultChan := make(chan error, 1)
	defer func() { close(resultChan) }()

	err := func() error {
		b.mutex.Lock()
		defer func() { b.mutex.Unlock() }()

		if b.wchan == nil {
			return beancaskError.ErrorSystemShuttingDown
		}

		if !b.exists(key) {
			return beancaskError.ErrorDataNotFound
		}

		r := CreateTomStoneRecord(key)
		b.wchan <- struct {
			key    string
			record Record
			result chan error
		}{
			key:    key,
			record: r,
			result: resultChan,
		}
		return nil
	}()

	if err != nil {
		return err
	}

	return <-resultChan
}

func (b *Bitcask) exists(key string) bool {
	if b.activeInstance.hashTable.Exists(key) {
		return true
	}

	for i := len(b.instances) - 1; i >= 0; i-- {
		if b.instances[i].hashTable.Exists(key) {
			return true
		}
	}

	return false
}

func (b *Bitcask) realSet() {
	timer := time.NewTimer(5 * time.Millisecond)
	var itemQ []struct {
		key    string
		record Record
		result chan error
	}
	var errorQ []error
	for {
		select {
		case witem, ok := <-b.wchan:
			if !ok {
				timer.Stop()
				break
			}
			itemQ = append(itemQ, witem)
		case <-timer.C:
			if len(itemQ) != 0 {
				func() {
					b.mutex.Lock()
					defer func() {
						for i, err := range errorQ {
							itemQ[i].result <- err
						}
						itemQ, errorQ = itemQ[:0], errorQ[:0]
						b.activeInstance.walFile.Sync()
						b.mutex.Unlock()
					}()

					var needCompact bool
					for _, witem := range itemQ {
						offset, len, err := b.activeInstance.walFile.AppendRecord(witem.record, false)
						if err != nil {
							log.Fatal("append key [", witem.key, "] to active file fail, data may be corrupted!")
							errorQ = append(errorQ, beancaskError.ErrorSystemInternal)
							continue
						}

						if witem.record.isTomeStone() {
							b.activeInstance.hashTable.Del(witem.key)
						} else {
							b.activeInstance.hashTable.Set(witem.key, HashItem{
								Wal:     b.activeInstance.walFile,
								Len:     len,
								Offset:  offset,
								Tmstamp: time.Now().UnixNano(),
							})
						}

						if int(offset)+len > 128*1024 {
							b.activeInstance.walFile.Sync()
							err = b.rotateInstance()
							if err != nil {
								log.Fatal("rotate instance fail, disk may be disfunctioning!")
								errorQ = append(errorQ, beancaskError.ErrorSystemInternal)
								continue
							}
							needCompact = true
						}

						errorQ = append(errorQ, nil)
						continue
					}

					if needCompact {
						needCompact = false
						ndataFile := len(b.instances)
						if ndataFile > 1 {
							go b.compact(ndataFile)
						}
					}
				}()
			}
			timer = time.NewTimer(5 * time.Millisecond)
		}
	}
}

func (b *Bitcask) rotateInstance() error {
	newDataFileName := fmt.Sprintf(dataFileNamePattern, len(b.instances))
	err := b.activeInstance.walFile.RenameFile(newDataFileName)
	if err != nil {
		log.Fatal("rename active file to data file fail, disk may be disfunctioning!")
		return beancaskError.ErrorSystemInternal
	}

	err = b.activeInstance.walFile.Deactivate()
	if err != nil {
		log.Fatal("diable write for active fail, disk may be disfunctioning!")
		return beancaskError.ErrorSystemInternal
	}

	b.instances = append(b.instances, b.activeInstance)
	b.activeInstance, err = createBitcaskInstance(activeFileName)
	if err != nil {
		log.Fatal("create active instance fail, system maybe disfunctioning")
		return nil
	}

	return nil
}

func (b *Bitcask) compact(ndataFile int) error {
	// ensure only one go-routine is compacting
	swaped := atomic.CompareAndSwapInt32(&b.compacting, 0, 1)
	if !swaped {
		return nil
	}
	defer func() { atomic.CompareAndSwapInt32(&b.compacting, 1, 0) }()

	ins, err := createBitcaskInstance(compationFileName)
	if err != nil {
		log.Fatal("create compation instance fail, system maybe disfunctioning")
		return nil
	}

	for i := ndataFile - 1; i >= 0; i-- {
		err = ins.mergeHash(b.instances[i])
		if err != nil {
			log.Fatal("compact instance fail, system maybe disfunctioning")
			return nil
		}
	}
	err = ins.dumpHashToFile()
	if err != nil {
		log.Fatalf("dump instance hash to file file fail:%s\n", err.Error())
		return nil
	}
	err = ins.walFile.Deactivate()
	if err != nil {
		log.Fatal("set compaction instance to read only fail, system maybe disfunctioning")
		return nil
	}

	func() {
		b.mutex.Lock()
		defer func() { b.mutex.Unlock() }()

		if b.wchan == nil {
			return
		}

		for i := 0; i < ndataFile; i++ {
			b.instances[i].walFile.RemoveFile()
		}

		var newDataFiles []*bitcaskInstance
		ins.walFile.RenameFile(fmt.Sprintf(dataFileNamePattern, 0))
		newDataFiles = append(newDataFiles, ins)
		for i := ndataFile; i < len(b.instances); i++ {
			b.instances[i].walFile.RenameFile(fmt.Sprintf(dataFileNamePattern, i-ndataFile+1))
			newDataFiles = append(newDataFiles, b.instances[i])
		}

		b.instances = newDataFiles
	}()

	return nil
}

func (b *Bitcask) waitAllForCloseAndExec(f func()) {
	// ensure compacting is done
	swaped := false
	for !swaped {
		swaped = atomic.CompareAndSwapInt32(&b.compacting, 0, 1)
		time.Sleep(time.Microsecond * 7)
	}
	b.mutex.Lock()
	defer func() {
		b.mutex.Unlock()
		atomic.CompareAndSwapInt32(&b.compacting, 1, 0)
	}()

	f()
}

// Destory destory the bitcask storage, with data on disk
func (b *Bitcask) Destory() {
	b.waitAllForCloseAndExec(func() {
		b.activeInstance.walFile.RemoveFile()
		for _, ins := range b.instances {
			ins.walFile.RemoveFile()
		}

		close(b.wchan)
	})
}

// Close close the bitcask storage, with data on disk remain
func (b *Bitcask) Close() {
	b.waitAllForCloseAndExec(func() {
		b.activeInstance.walFile.CloseFile(false)
		for _, ins := range b.instances {
			ins.walFile.CloseFile(false)
		}

		close(b.wchan)
	})
}
