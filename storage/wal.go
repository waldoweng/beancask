package storage

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"os"
	"sync"

	beancaskError "github.com/waldoweng/beancask/errors"
)

// Record struct of record of the bitcask wal file
type Record struct {
	Crc     uint32
	Tmstamp int64
	Ksz     int64
	ValSz   int64
	Key     []byte
	Value   []byte
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

// BitCaskLogFile struct of the BitcaskLogFile
// implementation of WALFile interface
type BitCaskLogFile struct {
	FileName    string
	FileHandle  *os.File
	FileHandleR *os.File
	mutex       *sync.RWMutex
}

// CreateBitcaskLogFile create a new BitcaskLogFile struct
func CreateBitcaskLogFile(filename string) *BitCaskLogFile {

	fileHandle, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("open file %s for writing fail, disk maybe disfunctioning\n", filename)
		return nil
	}

	fileHandleR, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		log.Fatalf("open file %s for reading fail, disk maybe disfunctioning\n", filename)
		return nil
	}

	return &BitCaskLogFile{
		FileName:    filename,
		FileHandle:  fileHandle,
		FileHandleR: fileHandleR,
		mutex:       new(sync.RWMutex),
	}
}

// ReadRecord read a record from wal file offset and [len] bytes
// return nil if success
func (b *BitCaskLogFile) ReadRecord(offset int64, len int, r *Record) error {
	buf := make([]byte, len)
	n, err := b.FileHandleR.ReadAt(buf, offset)
	if n == 0 || err != nil {
		log.Printf("Read data fd:%d offset:%d len[%d] fail. n:%d err:%s", b.FileHandleR.Fd(), offset, len, n, err.Error())
		return err
	}

	buffer := bytes.NewBuffer(buf)
	err = r.fromBuffer(buffer)
	if err != nil {
		log.Printf("Read data fd:%d offset:%d len[%d] fail. n:%d\n", b.FileHandleR.Fd(), offset, len, n)
	}

	return err
}

// AppendRecord append a record to wal file
// return record offset and len
// return nil for error if success
func (b *BitCaskLogFile) AppendRecord(r Record, sync bool) (int64, int, error) {
	offset, _ := b.FileHandle.Seek(0, os.SEEK_CUR)
	var buf bytes.Buffer
	err := r.toBuffer(&buf)
	if err != nil {
		log.Fatalf("parse data to buffer fail, err:%s", err.Error())
		return -1, -1, beancaskError.ErrorParseFileData
	}

	bbuf := buf.Bytes()
	var blen = len(bbuf)
	for true {
		wlen, err := b.FileHandle.Write(bbuf)
		if err != nil {
			log.Fatalf("write data to file fail, err:%s", err.Error())
			return -1, -1, beancaskError.ErrorParseFileData
		}

		if wlen == len(bbuf) {
			break
		} else {
			bbuf = bbuf[wlen:]
		}
	}

	if sync {
		b.FileHandle.Sync()
	}

	return offset, blen, nil
}

// IteratorRecord return a channel of record for iterating
func (b *BitCaskLogFile) IteratorRecord() <-chan struct {
	offset int
	r      Record
} {
	const BufSize int32 = 8 * 1024
	var offset int64
	buf := make([]byte, BufSize)
	chnl := make(chan struct {
		offset int
		r      Record
	})

	go func() {
		for true {
			n, err := b.FileHandleR.ReadAt(buf, int64(offset))
			if err == io.EOF && n == 0 {
				close(chnl)
				return
			} else if err != io.EOF && err != nil {
				log.Fatalf("Read data fd:%d offset:%d len:%d fail, err:%s", b.FileHandleR.Fd(), offset, BufSize, err.Error())
				close(chnl)
				return
			}

			var r Record
			buffer := bytes.NewBuffer(buf)
			err = r.fromBuffer(buffer)
			if err != nil {
				log.Fatalf("parse data fd:%d offset:%d len:%d fail, err:%s", b.FileHandleR.Fd(), offset, BufSize, err.Error())
				close(chnl)
				return
			}

			chnl <- struct {
				offset int
				r      Record
			}{int(offset), r}
			offset += r.size()
		}
	}()

	return chnl
}

// RenameFile rename the wal file on disk
// return nil if success
func (b *BitCaskLogFile) RenameFile(name string) error {
	err := os.Rename(b.FileName, name)
	if err != nil {
		log.Fatalf("rename file %s to %s fail, err:%s", b.FileName, name, err.Error())
		return err
	}
	b.FileName = name
	return nil
}

// RemoveFile remove the wal file on disk
// return nil if success
func (b *BitCaskLogFile) RemoveFile() error {
	b.CloseFile(true)
	return nil
}

// CloseFile close the file handle of the wal file on disk
// return nil if success
func (b *BitCaskLogFile) CloseFile(remove bool) error {
	if b.FileHandle != nil {
		b.FileHandle.Close()
		b.FileHandle = nil
	}

	if b.FileHandleR != nil {
		b.FileHandleR.Close()
		b.FileHandleR = nil
	}

	if remove {
		os.Remove(b.FileName)
	}
	b.FileName = ""
	return nil
}

// Deactivate deactivate the BitcaskLogFile by closing the write handle of the wal file if it's open
// return nil if success
func (b *BitCaskLogFile) Deactivate() error {
	if b.FileHandle != nil {
		b.FileHandle.Close()
		b.FileHandle = nil
	}
	return nil
}

// Sync flush all data to disk
func (b *BitCaskLogFile) Sync() error {
	return b.FileHandle.Sync()
}
