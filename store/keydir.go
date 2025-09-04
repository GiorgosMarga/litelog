package store

import (
	"fmt"
	"time"
)

type KeyDir struct {
	memHashMap map[string]*KeyDirEntry
}

type KeyDirEntry struct {
	tstamp time.Time
	fileId string
	offset int64
	valSz  int64
}

func NewKeyDir() *KeyDir {
	return &KeyDir{
		memHashMap: make(map[string]*KeyDirEntry),
	}
}

func (kd *KeyDir) add(k []byte, fileId string, offset, valSz int64) {
	entry := KeyDirEntry{
		tstamp: time.Now(),
		fileId: fileId,
		offset: offset,
		valSz:  valSz,
	}
	kd.memHashMap[string(k)] = &entry
}

func (kd *KeyDir) get(k []byte) (*KeyDirEntry, error) {
	entry, ok := kd.memHashMap[string(k)]
	if !ok {
		return nil, fmt.Errorf("key doesnt exist")
	}
	return entry, nil
}
