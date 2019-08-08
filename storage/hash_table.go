package storage

import "errors"

// HashItem for
type HashItem struct {
	Wal     WALFile
	Len     int
	Offset  int64
	Tmstamp int64
}

// CreateSimpleHashTable for
func CreateSimpleHashTable() *SimpleHashTable {
	return &SimpleHashTable{
		hash: make(map[string]HashItem),
	}
}

// SimpleHashTable for
type SimpleHashTable struct {
	hash map[string]HashItem
}

// Get for
func (s *SimpleHashTable) Get(key string) (h HashItem, err error) {
	if h, ok := s.hash[key]; ok {
		return h, nil
	}
	return HashItem{}, errors.New("no found")
}

// Set for
func (s *SimpleHashTable) Set(key string, h HashItem) error {
	s.hash[key] = h
	return nil
}

// Exists for
func (s *SimpleHashTable) Exists(key string) (exists bool) {
	_, ok := s.hash[key]
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
		for k, v := range s.hash {
			chnl <- struct {
				key   string
				value HashItem
			}{k, v}
		}

		// Ensure that at the end of the loop we close the channel!
		close(chnl)
	}()
	return chnl

}
