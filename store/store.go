package store

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

var (
	ErrNotFound         error = errors.New("key not found")
	ErrInvalidKeySize   error = errors.New("invalid key size")
	ErrInvalidValueSize error = errors.New("invalid val size")
	ErrCorrupted        error = errors.New("corrupted file")
)

type Store struct {
	log        *Log
	keyDir     *KeyDir
	mtx        *sync.RWMutex
	lru        *lru
	cancelChan chan struct{}
}

func NewStore() *Store {

	if err := ensureDir("db"); err != nil {
		panic(err)
	}

	if err := ensureDir("hint"); err != nil {
		panic(err)
	}

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
				store.merge(false)
			case <-s.cancelChan:
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

	offset, err := s.log.write(k, v)
	if err != nil {
		return err
	}
	s.keyDir.add(k, s.log.activeSegment.id, offset, uint32(len(v)))
	return nil
}

func (s *Store) Delete(k []byte) error {
	return s.Write(k, []byte{})
}

func (s *Store) Read(k []byte) ([]byte, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	entry, err := s.keyDir.get(k)
	if err != nil {
		return nil, ErrNotFound
	}

	// value is deleted
	if entry.valSz == 0 {
		return nil, ErrNotFound
	}
	// offset is the position where the key starts
	// need to skip headers
	valOffset := entry.offset + HEADERS_SIZE + int64(len(k))
	if entry.fileId == s.log.activeSegment.id {
		return s.log.read(valOffset, int32(entry.valSz))
	}

	segment := s.lru.get(entry.fileId)
	if segment == nil {
		f, err := os.Open(fmt.Sprintf("./db/%d", entry.fileId))
		if err != nil {
			return nil, err
		}
		segment, err = segmentFromFile(f)
		if err != nil {
			return nil, err
		}
		go s.lru.add(entry.fileId, segment)
	}
	return segment.read(valOffset, int32(entry.valSz))
}

func (s *Store) Stop() {
	s.cancelChan <- struct{}{}
	s.merge(true)
	s.mtx.Lock()
	s.log.activeSegment.close()
	s.mtx.Unlock()
}

func (s *Store) Sync() error {
	return s.log.activeSegment.sync()
}
func (s *Store) merge(mergeAll bool) {
	segments, err := os.ReadDir("db")
	if err != nil {
		return
	}
	totalSegments := len(segments)
	// if merge all, merge was called from close
	if !mergeAll {
		// if merge is called from a routine, dont merge the active file
		totalSegments--
	}

	if !mergeAll && totalSegments < 2 {
		return
	}
	mergedSegment, err := createNewSegment()
	if err != nil {
		log.Println(err)
		return
	}
	defer mergedSegment.close()
	hintF, err := os.OpenFile(fmt.Sprintf("./hint/%d", mergedSegment.id), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Println(err)
		return
	}

	hbf := bufio.NewWriter(hintF)
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for i := range totalSegments {
		dirEntry := segments[i]
		segment, err := segmentFromFileName(dirEntry.Name())
		if err != nil {
			log.Println(err)
			continue
		}
		for {

			record, err := segment.readRecord()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				log.Println(err)
				return
			}
			entry, err := s.keyDir.get(record.key)
			if err != nil {
				continue
			}
			if entry.fileId != segment.id {
				continue // this is an old one
			}
			if entry.valSz == 0 {
				continue // this is a deleted key
			}

			offset, err := mergedSegment.writeRecord(record)
			if err != nil {
				log.Println(err)
				continue
			}
			newEntry := s.keyDir.add(record.key, mergedSegment.id, offset, uint32(len(record.val)))
			newEntry.save(hbf, record.key)
		}
		hbf.Flush()
		if err := segment.remove(); err != nil {
			log.Println(err)
		}
	}
	s.lru.reset()
}

func ensureDir(dirName string) error {
	err := os.Mkdir(dirName, 0755)
	if err == nil {
		return nil
	}
	if os.IsExist(err) {
		// check that the existing path is a directory
		info, err := os.Stat(dirName)
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return errors.New("path exists but is not a directory")
		}
		return nil
	}
	return err
}
