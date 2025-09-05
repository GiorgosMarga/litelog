package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

type Log struct {
	activeSegment *os.File
	totalSegments int
}

func NewLog() *Log {

	if err := ensureDir("db"); err != nil {
		panic(err)
	}
	dir, err := os.ReadDir("db")
	if err != nil {
		panic(err)
	}
	var (
		filename      string
		totalSegments int
	)
	if len(dir) == 0 {
		filename = fmt.Sprintf("./db/%d", time.Now().UnixMicro())
	} else {
		for i := len(dir) - 1; i >= 0; i-- {
			if !strings.HasPrefix(dir[i].Name(), "kd") {
				filename = fmt.Sprintf("./db/%s", dir[i].Name())
				break
			}
		}
	}
	newSegment, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	return &Log{
		activeSegment: newSegment,
		totalSegments: totalSegments,
	}
}

func (l *Log) close() {
	if l.activeSegment != nil {
		l.activeSegment.Close()
	}
}

// func createSegment()
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
func (l *Log) write(k, v []byte) (int64, error) {

	var (
		keyLen int64 = int64(len(k))
		valLen int64 = int64(len(v))
	)

	offset, err := l.activeSegment.Seek(0, io.SeekCurrent)
	if err != nil {
		return -1, err
	}
	if err := binary.Write(l.activeSegment, binary.LittleEndian, keyLen); err != nil {
		return -1, err
	}
	if err := binary.Write(l.activeSegment, binary.LittleEndian, k); err != nil {
		return -1, err
	}
	if err := binary.Write(l.activeSegment, binary.LittleEndian, valLen); err != nil {
		return -1, err
	}
	if err := binary.Write(l.activeSegment, binary.LittleEndian, v); err != nil {
		return -1, err
	}
	size := l.getFileSz()
	if size >= 2<<16 {
		f, err := l.createNewSegment()
		if err != nil {
			return -1, err
		}
		l.activeSegment.Close()
		l.activeSegment = f
	}

	return offset, nil
}

func (l *Log) read(f *os.File, pos, valSz int64) ([]byte, error) {

	val := make([]byte, valSz)
	_, err := f.ReadAt(val, pos)
	if err != nil {
		return nil, err
	}
	if err := binary.Read(bytes.NewReader(val), binary.LittleEndian, &val); err != nil {
		return nil, err
	}
	return val, nil
}

func (l *Log) getFileSz() int64 {
	info, err := l.activeSegment.Stat()
	if err != nil {
		return -1
	}
	return info.Size()
}

func (l *Log) createNewSegment() (*os.File, error) {
	filename := fmt.Sprintf("./db/%d", time.Now().UnixMicro())
	newSegment, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return newSegment, err
}
