package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"
)

type KeyDir struct {
	memHashMap map[string]*KeyDirEntry
}

type KeyDirEntry struct {
	tstamp int64
	fileId int64
	offset int64
	valSz  int64
}

func NewKeyDir() *KeyDir {
	return &KeyDir{
		memHashMap: make(map[string]*KeyDirEntry),
	}
}

func (kd *KeyDir) add(k []byte, fileId int64, offset, valSz int64) *KeyDirEntry {
	entry := KeyDirEntry{
		tstamp: time.Now().Unix(),
		fileId: fileId,
		offset: offset,
		valSz:  valSz,
	}
	kd.memHashMap[string(k)] = &entry
	return &entry
}

func (kd *KeyDir) get(k []byte) (*KeyDirEntry, error) {
	entry, ok := kd.memHashMap[string(k)]
	if !ok {
		return nil, fmt.Errorf("key doesnt exist")
	}
	return entry, nil
}

func (entry *KeyDirEntry) save(f *os.File, k []byte) error {
	binary.Write(f, binary.LittleEndian, entry.tstamp)
	binary.Write(f, binary.LittleEndian, int64(len(k)))
	binary.Write(f, binary.LittleEndian, []byte(k))
	binary.Write(f, binary.LittleEndian, entry.valSz)
	binary.Write(f, binary.LittleEndian, entry.offset)
	return nil
}

func (kd *KeyDir) load() error {
	segments, err := os.ReadDir("./hint")
	if err != nil {
		return err
	}
	// first start no kd file available
	if len(segments) == 0 {
		return nil
	}
	latestKDFile := segments[len(segments)-1]

	fId, err := strconv.ParseInt(latestKDFile.Name(), 10, 64)
	if err != nil {
		return fmt.Errorf("invalid hint file")
	}

	f, err := os.Open(fmt.Sprintf("./hint/%s", latestKDFile.Name()))
	if err != nil {
		return err
	}
	log.Printf("Load from: %s\n", f.Name())

	defer f.Close()
	var keyLen int64
	for {
		entry := KeyDirEntry{
			fileId: fId,
		}
		if err := binary.Read(f, binary.LittleEndian, &entry.tstamp); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		binary.Read(f, binary.LittleEndian, &keyLen)
		key := make([]byte, keyLen)
		binary.Read(f, binary.LittleEndian, key)
		binary.Read(f, binary.LittleEndian, &entry.valSz)

		binary.Read(f, binary.LittleEndian, &entry.offset)
		kd.memHashMap[string(key)] = &entry
	}
	return nil
}
