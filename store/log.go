package store

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type Log struct {
	currentSegment *os.File
}

func NewLog() *Log {
	filename := fmt.Sprintf("./db/%s", rand.Text())

	newSegment, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	return &Log{
		currentSegment: newSegment,
	}
}

func (l *Log) write(k, v []byte) (int64, error) {
	var (
		keyLen int64 = int64(len(k))
		valLen int64 = int64(len(v))
	)

	offset, err := l.currentSegment.Seek(0, io.SeekCurrent)
	if err != nil {
		return -1, err
	}
	if err := binary.Write(l.currentSegment, binary.LittleEndian, keyLen); err != nil {
		return -1, err
	}
	if err := binary.Write(l.currentSegment, binary.LittleEndian, k); err != nil {
		return -1, err
	}
	if err := binary.Write(l.currentSegment, binary.LittleEndian, valLen); err != nil {
		return -1, err
	}
	if err := binary.Write(l.currentSegment, binary.LittleEndian, v); err != nil {
		return -1, err
	}
	return offset, nil
}

func (l *Log) read(pos int64) ([]byte, error) {
	_, err := l.currentSegment.Seek(pos, io.SeekStart)
	if err != nil {
		return nil, err
	}

	var (
		keyLen int64
		valLen int64
	)

	if err := binary.Read(l.currentSegment, binary.LittleEndian, &keyLen); err != nil {
		return nil, err
	}
	key := make([]byte, keyLen)
	if err := binary.Read(l.currentSegment, binary.LittleEndian, &key); err != nil {
		return nil, err
	}
	if err := binary.Read(l.currentSegment, binary.LittleEndian, &valLen); err != nil {
		return nil, err
	}
	val := make([]byte, valLen)
	if err := binary.Read(l.currentSegment, binary.LittleEndian, &val); err != nil {
		return nil, err
	}
	return val, nil
}
