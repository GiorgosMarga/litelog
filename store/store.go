package store

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

type Store struct {
	log        *Log
	keyDir     *KeyDir
	mtx        *sync.RWMutex
	lru        *lru
	cancelChan chan struct{}
}

func NewStore() *Store {

	kd := NewKeyDir()
	if err := kd.load(); err != nil {
		log.Fatal(err)
	}
	store := &Store{
		log:        NewLog(),
		keyDir:     kd,
		mtx:        &sync.RWMutex{},
		cancelChan: make(chan struct{}),
		lru:        NewLRU(20),
	}

	go func(s *Store) {
		timer := time.NewTicker(500 * time.Millisecond)

		for {
			select {
			case <-timer.C:
				store.merge()
			case <-s.cancelChan:
				fmt.Println("Stop merging")
				return
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
	f := s.lru.get(entry.fileId)
	if f == nil {
		f, err = os.Open(entry.fileId)
		if err != nil {
			return nil, err
		}
		s.lru.add(entry.fileId, f)
	}
	return s.log.read(f, valOffset, entry.valSz)
}

func (s *Store) Stop() {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.cancelChan <- struct{}{}
	if err := s.keyDir.save(); err != nil {
		log.Fatal(err)
	}
	s.log.close()
}
func (s *Store) merge() {
	segments, err := os.ReadDir("db")
	if err != nil {
		return
	}

	totalSegments := 0
	for _, segment := range segments {
		if !strings.HasPrefix(segment.Name(), "kd") {
			totalSegments++
		}
	}
	mergeFactor := 0.5
	filesToMerge := int(float64(totalSegments-1) * mergeFactor) // dont merge the active file

	if filesToMerge < 2 {
		return
	}

	mergedSegment, err := s.log.createNewSegment()
	if err != nil {
		log.Println(err)
		return
	}
	defer mergedSegment.Close()

	// merge 50% files of the files

	// filename, err := s.log.createNewSegment()

	// you can still write but you cant read
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for i := range filesToMerge {
		segment := segments[i]
		if strings.HasPrefix(segment.Name(), "kd") {
			continue
		}
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
