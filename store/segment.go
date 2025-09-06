package store

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

const STORE_FOLDER = "db"

type Segment struct {
	f  *os.File
	id int64
}

func (s *Segment) close() {
	s.f.Close()
}
func segmentFromFileName(fName string) (*Segment, error) {
	f, err := os.OpenFile(fmt.Sprintf("./%s/%s", STORE_FOLDER, fName), os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return segmentFromFile(f)
}
func segmentFromFile(f *os.File) (*Segment, error) {
	id, err := getIdFromName(f.Name())
	if err != nil {
		return nil, err
	}
	return &Segment{
		f:  f,
		id: id,
	}, nil
}
func createNewSegment() (*Segment, error) {
	fileId := time.Now().UnixMicro()
	f, err := os.OpenFile(getNameFromId(fileId), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &Segment{
		f:  f,
		id: fileId,
	}, nil
}

func getNameFromId(fileId int64) string {
	return fmt.Sprintf("./%s/%d", STORE_FOLDER, fileId)
}
func getIdFromName(fname string) (int64, error) {
	sId := strings.Split(fname, "/")
	if len(sId) == 0 {
		return -1, fmt.Errorf("invalid id")
	}
	return strconv.ParseInt(sId[len(sId)-1], 10, 64)
}
func (s *Segment) getSize() (int64, error) {
	info, err := s.f.Stat()
	if err != nil {
		return -1, err
	}
	return info.Size(), nil
}
func (s *Segment) write(k, v []byte) (int64, error) {
	offset, err := s.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return -1, err
	}

	tstamp := time.Now().UnixMicro()
	data := make([]byte, HEADERS_SIZE+len(k)+len(v))
	binary.LittleEndian.PutUint64(data[4:12], uint64(tstamp))
	binary.LittleEndian.PutUint32(data[12:16], uint32(len(k)))
	binary.LittleEndian.PutUint32(data[16:20], uint32(len(v)))
	copy(data[20:], k)
	copy(data[20+len(k):], v)

	// write CRC
	crc := crc32.ChecksumIEEE(data)
	binary.LittleEndian.PutUint32(data[0:4], uint32(crc))
	n, err := s.f.Write(data)
	if err != nil {
		return -1, err
	}
	if n != len(data) {
		return -1, ErrCorrupted
	}
	return offset, nil
}

func (s *Segment) read(offset int64, size int32) ([]byte, error) {
	b := make([]byte, size)
	n, err := s.f.ReadAt(b, offset)
	if err != nil {
		return nil, err
	}
	if int32(n) != size {
		return nil, ErrCorrupted
	}
	return b, nil
}

func (s *Segment) readRecord() (*Record, error) {
	var (
		keySz int32
		valSz int32
	)
	rec := Record{}
	header := make([]byte, HEADERS_SIZE)
	n, err := s.f.Read(header)
	if err != nil {
		return nil, err
	}
	if n != len(header) {
		return nil, ErrCorrupted
	}
	rec.crc = int32(binary.LittleEndian.Uint32(header[0:4]))
	rec.tstamp = int64(binary.LittleEndian.Uint64(header[4:12]))
	keySz = int32(binary.LittleEndian.Uint32(header[12:16]))
	if keySz > MAX_KEY_SIZE {
		return nil, ErrInvalidKeySize
	}
	valSz = int32(binary.LittleEndian.Uint32(header[16:20]))
	if valSz > MAX_VALUE_SIZE {
		return nil, ErrInvalidValueSize
	}
	kv := make([]byte, keySz+valSz)
	n, err = s.f.Read(kv)
	if err != nil {
		return nil, err
	}
	if n != int(keySz)+int(valSz) {
		return nil, ErrCorrupted
	}
	rec.key = kv[:keySz]
	rec.val = kv[keySz:]

	return &rec, nil
}

func (s *Segment) writeRecord(rec *Record) (int64, error) {
	offset, err := s.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return -1, err
	}
	header := make([]byte, HEADERS_SIZE+len(rec.key)+len(rec.val))
	binary.LittleEndian.PutUint32(header[0:4], uint32(rec.crc))
	binary.LittleEndian.PutUint64(header[4:12], uint64(rec.tstamp))
	binary.LittleEndian.PutUint32(header[12:16], uint32(len(rec.key)))
	binary.LittleEndian.PutUint32(header[16:20], uint32(len(rec.val)))
	copy(header[20:], rec.key)
	copy(header[20+len(rec.key):], rec.val)

	n, err := s.f.Write(header)
	if err != nil {
		return -1, err
	}
	if n != len(header) {
		return -1, ErrCorrupted
	}
	return offset, nil
}
func (s *Segment) sync() error {
	return s.f.Sync()
}
func (s *Segment) remove() error {
	s.close()
	// remove its hint file if exists
	if err := os.Remove(fmt.Sprintf("./hint/%d", s.id)); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}

	return os.Remove(s.f.Name())

}
