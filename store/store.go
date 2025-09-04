package store

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

type Store struct {
	log    *Log
	keyDir *KeyDir
	mtx    *sync.RWMutex
}

func NewStore() *Store {
	store := &Store{
		log:    NewLog(),
		keyDir: NewKeyDir(),
		mtx:    &sync.RWMutex{},
	}

	go func(s *Store) {
		timer := time.NewTicker(500 * time.Millisecond)

		for {
			select {
			case <-timer.C:
				store.merge()
			default:
				time.Sleep(10 * time.Millisecond)
			}
		}
	}(store)
	return store
}

func (s *Store) Write(k, v []byte) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	fileid := s.log.activeSegment.Name()
	offset, err := s.log.write(k, v)
	if err != nil {
		return err
	}
	s.keyDir.add(k, fileid, offset, int64(len(v)))
	return nil
}

func (s *Store) Read(k []byte) ([]byte, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	entry, err := s.keyDir.get(k)
	if err != nil {
		return nil, fmt.Errorf("key doesnt exist")
	}
	// offset is the position where the key starts
	// need to skip int64(keylen) + actual key value + int64(vallen)
	valOffset := entry.offset + 8 + int64(len(k)) + 8
	return s.log.read(entry.fileId, valOffset, entry.valSz)
}

func (s *Store) merge() {

	mergedSegment, err := s.log.createNewSegment()
	if err != nil {
		log.Println(err)
		return
	}
	defer mergedSegment.Close()

	// merge 50% files of the files

	// filename, err := s.log.createNewSegment()
	segments, err := os.ReadDir("db")
	if err != nil {
		return
	}

	mergeFactor := 0.5
	filesToMerge := int(float64(len(segments)-2) * mergeFactor) // dont merge the active file

	if filesToMerge < 2 {
		return
	}
	// you can still write but you cant read
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for i := range filesToMerge {
		segment := segments[i]
		fname := fmt.Sprintf("./db/%s", segment.Name())
		f, err := os.Open(fname)
		if err != nil {
			continue
		}
		defer f.Close()
		for {
			k, v, err := s.readRecord(f)
			if err != nil {
				break
			}
			entry, err := s.keyDir.get(k)
			if err != nil {
				continue
			}
			if entry.fileId != f.Name() {
				continue // this is an old one
			}

			offset, err := s.writeRecord(mergedSegment, k, v)
			if err != nil {
				continue
			}
			s.keyDir.add(k, mergedSegment.Name(), offset, int64(len(v)))
		}
		if err := os.Remove(fname); err != nil {
			log.Println(err)
		}
	}
}

func (s *Store) readRecord(f *os.File) ([]byte, []byte, error) {
	var (
		keySz int64
		valSz int64
	)
	if err := binary.Read(f, binary.LittleEndian, &keySz); err != nil {
		return nil, nil, err
	}
	key := make([]byte, keySz)
	if err := binary.Read(f, binary.LittleEndian, &key); err != nil {
		return nil, nil, err
	}

	if err := binary.Read(f, binary.LittleEndian, &valSz); err != nil {
		return nil, nil, err
	}
	val := make([]byte, valSz)
	if err := binary.Read(f, binary.LittleEndian, &val); err != nil {
		return nil, nil, err
	}

	return key, val, nil
}
func (s *Store) writeRecord(f *os.File, k, v []byte) (int64, error) {
	offset, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return -1, err
	}
	if err := binary.Write(f, binary.LittleEndian, int64(len(k))); err != nil {
		return -1, err
	}
	if err := binary.Write(f, binary.LittleEndian, k); err != nil {
		return -1, err
	}

	if err := binary.Write(f, binary.LittleEndian, int64(len(v))); err != nil {
		return -1, err
	}
	if err := binary.Write(f, binary.LittleEndian, v); err != nil {
		return -1, err
	}

	return offset, nil
}
