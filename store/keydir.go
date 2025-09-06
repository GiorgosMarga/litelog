package store

import (
	"bufio"
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
	valSz  uint32
}

func NewKeyDir() *KeyDir {
	return &KeyDir{
		memHashMap: make(map[string]*KeyDirEntry),
	}
}

func (kd *KeyDir) add(k []byte, fileId, offset int64, valSz uint32) *KeyDirEntry {
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

func (entry *KeyDirEntry) save(f *bufio.Writer, k []byte) error {
	// tstamp_size + key_size + len(k) + value_size + offset_size
	buf := make([]byte, TSTAMP_SIZE+KEY_SIZE+len(k)+VALUE_SIZE+OFFSET_SIZE)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(entry.tstamp))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(len(k)))
	binary.LittleEndian.PutUint32(buf[12:16], entry.valSz)
	binary.LittleEndian.PutUint64(buf[16:24], uint64(entry.offset))
	copy(buf[24:], k)
	_, err := f.Write(buf)
	return err
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

	// maxEntrySize := TSTAMP_SIZE + KEY_SIZE + math.MaxUint32 +
	header := make([]byte, TSTAMP_SIZE+KEY_SIZE+VALUE_SIZE+OFFSET_SIZE)
	for {
		entry := KeyDirEntry{
			fileId: fId,
		}

		n, err := f.Read(header)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if n != len(header) {
			return fmt.Errorf("truncated header")
		}

		entry.tstamp = int64(binary.LittleEndian.Uint64(header[0:8]))
		keySz := binary.LittleEndian.Uint32(header[8:12])
		if keySz >= MAX_KEY_SIZE {
			return ErrInvalidKeySize
		}
		entry.valSz = binary.LittleEndian.Uint32(header[12:16])
		if entry.valSz >= MAX_VALUE_SIZE {
			return ErrInvalidValueSize
		}
		entry.offset = int64(binary.LittleEndian.Uint64(header[16:24]))
		key := make([]byte, keySz)
		n, err = f.Read(key)
		if err != nil {
			return err
		}
		if n != int(keySz) {
			return ErrCorrupted
		}

		kd.memHashMap[string(key)] = &entry
	}
	return nil
}
