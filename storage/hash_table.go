package storage

import (
	"errors"
	"sync"
)

// CreateSimpleHashTable create a new hash table
func CreateSimpleHashTable() *SimpleHashTable {
	return &SimpleHashTable{}
}

// SimpleHashTable struct of hash table
type SimpleHashTable struct {
	hash sync.Map
}

// Get get value of key from the hash table
func (s *SimpleHashTable) Get(key string) (h HashItem, err error) {
	if h, ok := s.hash.Load(key); ok {
		return h.(interface{}).(HashItem), nil
	}
	return HashItem{}, errors.New("no found")
}

// Set set value of key to the hash table
func (s *SimpleHashTable) Set(key string, h HashItem) error {
	s.hash.Store(key, h)
	return nil
}

// Del delete value of key to the hash table
func (s *SimpleHashTable) Del(key string) error {
	s.hash.Delete(key)
	return nil
}

// Exists check whether a key exists on the hash table
func (s *SimpleHashTable) Exists(key string) (exists bool) {
	_, ok := s.hash.Load(key)
	return ok
}

// IteratorItem return a channel of the hash table for iterating
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
