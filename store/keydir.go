package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

type KeyDir struct {
	memHashMap map[string]*KeyDirEntry
}

type KeyDirEntry struct {
	tstamp int64
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
		tstamp: time.Now().Unix(),
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

func (kd *KeyDir) save() error {
	filename := fmt.Sprintf("./db/kd_%d", time.Now().UnixMicro())
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	for key, entry := range kd.memHashMap {
		binary.Write(f, binary.LittleEndian, int64(len(key)))
		binary.Write(f, binary.LittleEndian, []byte(key))
		binary.Write(f, binary.LittleEndian, entry.tstamp)
		binary.Write(f, binary.LittleEndian, entry.offset)
		binary.Write(f, binary.LittleEndian, entry.valSz)
		binary.Write(f, binary.LittleEndian, int64(len(entry.fileId)))
		binary.Write(f, binary.LittleEndian, []byte(entry.fileId))
	}
	return nil
}

func (kd *KeyDir) load() error {
	segments, err := os.ReadDir("./db")
	if err != nil {
		return err
	}

	var latestKDFile string
	for i := len(segments) - 1; i >= 0; i-- {
		if strings.HasPrefix(segments[i].Name(), "kd") {
			latestKDFile = segments[i].Name()
			break
		}
	}

	// first start no kd file available
	if latestKDFile == "" {
		return nil
	}

	f, err := os.Open(fmt.Sprintf("./db/%s", latestKDFile))
	if err != nil {
		return err
	}
	defer f.Close()
	var (
		keyLen int64
		fIdLen int64
	)

	for {
		if err := binary.Read(f, binary.LittleEndian, &keyLen); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		entry := KeyDirEntry{}
		key := make([]byte, keyLen)

		binary.Read(f, binary.LittleEndian, key)
		binary.Read(f, binary.LittleEndian, &entry.tstamp)
		binary.Read(f, binary.LittleEndian, &entry.offset)
		binary.Read(f, binary.LittleEndian, &entry.valSz)
		binary.Read(f, binary.LittleEndian, &fIdLen)
		fId := make([]byte, fIdLen)
		binary.Read(f, binary.LittleEndian, fId)
		entry.fileId = string(fId)
		kd.memHashMap[string(key)] = &entry
	}
	return nil
}
