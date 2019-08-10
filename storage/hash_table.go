package storage

import (
	"errors"
	"sync"
)

// HashItem for
type HashItem struct {
	Wal     WALFile
	Len     int
	Offset  int64
	Tmstamp int64
}

// CreateSimpleHashTable for
func CreateSimpleHashTable() *SimpleHashTable {
	return &SimpleHashTable{}
}

// SimpleHashTable for
type SimpleHashTable struct {
	hash sync.Map
}

// Get for
func (s *SimpleHashTable) Get(key string) (h HashItem, err error) {
	if h, ok := s.hash.Load(key); ok {
		return h.(interface{}).(HashItem), nil
	}
	return HashItem{}, errors.New("no found")
}

// Set for
func (s *SimpleHashTable) Set(key string, h HashItem) error {
	s.hash.Store(key, h)
	return nil
}

// Exists for
func (s *SimpleHashTable) Exists(key string) (exists bool) {
	_, ok := s.hash.Load(key)
	return ok
}

// IteratorItem for
func (s *SimpleHashTable) IteratorItem() <-chan struct {
	key   string
	value HashItem
} {
	chnl := make(chan struct {
		key   string
		value HashItem
	})
	go func() {
		s.hash.Range(func(k interface{}, v interface{}) bool {
			chnl <- struct {
				key   string
				value HashItem
			}{
				k.(interface{}).(string),
				v.(interface{}).(HashItem),
			}
			return true
		})

		// Ensure that at the end of the loop we close the channel!
		close(chnl)
	}()
	return chnl

}
