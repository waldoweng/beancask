package storage

import (
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

// BHashTable for
type BHashTable interface {
	Get(key string) (h HashItem, err error)
	Set(key string, h HashItem) error
	Exists(key string) (exists bool)
	IteratorItem() <-chan struct {
		key   string
		value HashItem
	}
}

// WALFile for
type WALFile interface {
	RenameFile(name string) error
	RemoveFile() error
	Deactivate() error
	Sync() error
	AppendRecord(r Record, sync bool) (off int64, len int, err error)
	ReadRecord(off int64, len int, r *Record) error
	IteratorRecord() <-chan struct {
		offset int
		r      Record
	}
}

// BitcaskInstance for
type BitcaskInstance struct {
	hashTable BHashTable
	walFile   WALFile
}

// LoadData for
func (b *BitcaskInstance) LoadData() error {
	for re := range b.walFile.IteratorRecord() {
		offset, r := re.offset, re.r

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

// Compact for
func (b *BitcaskInstance) Compact(ins *BitcaskInstance) error {
	for kv := range ins.hashTable.IteratorItem() {
		k, v := kv.key, kv.value

		var r Record
		err := ins.walFile.ReadRecord(v.Offset, v.Len, &r)
		if err != nil {
			log.Fatalf("read wal file fail, system memory maybe insufficient")
			return beancaskError.ErrorSystemInternal
		}

		v.Offset, v.Len, err = b.walFile.AppendRecord(r, false)
		if err != nil {
			log.Fatalf("write wal file fail, system memory maybe insufficient")
			return beancaskError.ErrorSystemInternal
		}

		v.Wal = b.walFile
		err = b.hashTable.Set(k, v)
		if err != nil {
			log.Fatalf("set hash table fail, system memory maybe insufficient")
			return beancaskError.ErrorSystemInternal
		}
	}
	return nil
}

// CreateBitcaskInstance for
func CreateBitcaskInstance(name string) (*BitcaskInstance, error) {
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

	return &BitcaskInstance{hashTable, walFile}, nil
}

// Bitcask for
type Bitcask struct {
	wchan chan struct {
		key    string
		record Record
		result chan error
	}
	mutex          *sync.RWMutex
	compacting     int32
	activeInstance *BitcaskInstance
	instances      []*BitcaskInstance
}

// NewBitcask for
func NewBitcask() *Bitcask {
	b := Bitcask{
		mutex:          new(sync.RWMutex),
		compacting:     0,
		activeInstance: nil,
	}
	b.LoadData()

	b.wchan = make(chan struct {
		key    string
		record Record
		result chan error
	}, 1024)

	go b.RealSet()

	return &b
}

// LoadData for
func (b *Bitcask) LoadData() error {
	var err error

	// load active wal file
	b.activeInstance, err = CreateBitcaskInstance(activeFileName)
	if err != nil {
		log.Fatal("create active instance fail, system maybe disfunctioning")
		return nil
	}

	err = b.activeInstance.LoadData()
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
		ins, err := CreateBitcaskInstance(name)
		if err != nil {
			log.Fatal("create data instance fail, system maybe disfunctioning")
			return nil
		}
		err = ins.LoadData()
		if err != nil {
			log.Fatal("create data instance fail, system maybe disfunctioning")
			return nil
		}
		b.instances = append(b.instances, ins)
	}

	return nil
}

// Get for
func (b *Bitcask) Get(key string) (string, error) {
	b.mutex.RLock()
	defer func() { b.mutex.RUnlock() }()

	if item, err := b.activeInstance.hashTable.Get(key); err == nil {
		var r Record
		err = b.activeInstance.walFile.ReadRecord(item.Offset, item.Len, &r)
		if err != nil {
			log.Fatal("read key [", key, "] from active file fail, data may be corrupted!")
			return "", beancaskError.ErrorSystemInternal
		}
		return string(r.Value[:]), nil
	}

	for _, ins := range b.instances {
		if item, err := ins.hashTable.Get(key); err == nil {
			var r Record
			err = ins.walFile.ReadRecord(item.Offset, item.Len, &r)
			if err != nil {
				log.Fatal("read key [", key, "] from active file fail, data may be corrupted!")
				return "", beancaskError.ErrorSystemInternal
			}
			return string(r.Value[:]), nil
		}
	}

	return "", beancaskError.ErrorDataNotFound
}

// Set for
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
	b.wchan <- struct {
		key    string
		record Record
		result chan error
	}{
		key:    key,
		record: r,
		result: resultChan,
	}

	return <-resultChan
}

// RealSet for
func (b *Bitcask) RealSet() {
	ticker := time.NewTicker(5 * time.Millisecond)
	var itemQ []struct {
		key    string
		record Record
		result chan error
	}
	var errorQ []error
	for {
		select {
		case witem := <-b.wchan:
			itemQ = append(itemQ, witem)
		case <-ticker.C:
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

					for _, witem := range itemQ {
						offset, len, err := b.activeInstance.walFile.AppendRecord(witem.record, false)
						if err != nil {
							log.Fatal("append key [", witem.key, "] to active file fail, data may be corrupted!")
							errorQ = append(errorQ, beancaskError.ErrorSystemInternal)
							continue
						}

						b.activeInstance.hashTable.Set(witem.key, HashItem{
							Wal:     b.activeInstance.walFile,
							Len:     len,
							Offset:  offset,
							Tmstamp: time.Now().UnixNano(),
						})

						if int(offset)+len > 128*1024 {
							b.activeInstance.walFile.Sync()
							err = b.RotateInstance()
							if err != nil {
								log.Fatal("rotate instance fail, disk may be disfunctioning!")
								errorQ = append(errorQ, beancaskError.ErrorSystemInternal)
								continue
							}
							go b.Compact()
						}

						errorQ = append(errorQ, nil)
						continue
					}
				}()
			}
		}
	}
}

// RotateInstance for
func (b *Bitcask) RotateInstance() error {
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
	b.activeInstance, err = CreateBitcaskInstance(activeFileName)
	if err != nil {
		log.Fatal("create active instance fail, system maybe disfunctioning")
		return nil
	}

	return nil
}

// Compact for
func (b *Bitcask) Compact() error {
	ndataFile := len(b.instances)
	if ndataFile <= 1 {
		return nil
	}

	// ensure only one go-routine is compacting
	swaped := atomic.CompareAndSwapInt32(&b.compacting, 0, 1)
	if !swaped {
		return nil
	}
	defer func() { atomic.CompareAndSwapInt32(&b.compacting, 1, 0) }()

	ins, err := CreateBitcaskInstance(compationFileName)
	if err != nil {
		log.Fatal("create compation instance fail, system maybe disfunctioning")
		return nil
	}

	for _, curIns := range b.instances[:ndataFile] {
		err = ins.Compact(curIns)
		if err != nil {
			log.Fatal("compact instance fail, system maybe disfunctioning")
			return nil
		}
	}
	err = ins.walFile.Deactivate()
	if err != nil {
		log.Fatal("set compaction instance to read only fail, system maybe disfunctioning")
		return nil
	}

	{
		b.mutex.Lock()
		defer func() { b.mutex.Unlock() }()

		for i := 0; i < ndataFile; i++ {
			b.instances[i].walFile.RemoveFile()
		}

		var newDataFiles []*BitcaskInstance
		ins.walFile.RenameFile(fmt.Sprintf(dataFileNamePattern, 0))
		newDataFiles = append(newDataFiles, ins)
		for i := ndataFile; i < len(b.instances); i++ {
			b.instances[i].walFile.RenameFile(fmt.Sprintf(dataFileNamePattern, i-ndataFile+1))
			newDataFiles = append(newDataFiles, b.instances[i])
		}

		b.instances = newDataFiles
	}

	return nil
}

// Destory for
func (b *Bitcask) Destory() {
	// ensure compacting is done
	swaped := false
	for !swaped {
		swaped = atomic.CompareAndSwapInt32(&b.compacting, 0, 1)
	}
	b.mutex.Lock()
	defer func() {
		b.mutex.Unlock()
		atomic.CompareAndSwapInt32(&b.compacting, 1, 0)
	}()

	b.activeInstance.walFile.RemoveFile()
	for _, ins := range b.instances {
		ins.walFile.RemoveFile()
	}
}
