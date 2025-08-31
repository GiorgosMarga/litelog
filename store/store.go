package store

import "fmt"

type Store struct {
	log        *Log
	memHashMap map[string]int64
}

func NewStore() *Store {
	return &Store{
		log:        NewLog(),
		memHashMap: make(map[string]int64),
	}
}

func (s *Store) Write(k, v []byte) error {
	offset, err := s.log.write(k, v)
	if err != nil {
		return err
	}
	s.memHashMap[string(k)] = offset
	return nil
}

func (s *Store) Read(k []byte) ([]byte, error) {
	offset, ok := s.memHashMap[string(k)]
	if !ok {
		return nil, fmt.Errorf("key doesnt exist")
	}
	return s.log.read(offset)
}
