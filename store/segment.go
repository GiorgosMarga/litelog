package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
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

	// write CRC
	//TODO: crc
	if err := binary.Write(s.f, binary.LittleEndian, int64(10)); err != nil {
		return -1, err
	}
	// write ts_stamp
	if err := binary.Write(s.f, binary.LittleEndian, time.Now().UnixMicro()); err != nil {
		return -1, err
	}
	// write k_sz
	if err := binary.Write(s.f, binary.LittleEndian, int64(len(k))); err != nil {
		return -1, err
	}
	// write k
	if err := binary.Write(s.f, binary.LittleEndian, k); err != nil {
		return -1, err
	}

	// write v_sz
	if err := binary.Write(s.f, binary.LittleEndian, int64(len(v))); err != nil {
		return -1, err
	}
	// write v
	if err := binary.Write(s.f, binary.LittleEndian, v); err != nil {
		return -1, err
	}
	return offset, nil
}

func (s *Segment) read(offset, size int64) ([]byte, error) {
	b := make([]byte, size)
	_, err := s.f.ReadAt(b, offset)
	if err != nil {
		return nil, err
	}
	v := make([]byte, size)
	if err := binary.Read(bytes.NewReader(b), binary.LittleEndian, v); err != nil {
		return nil, err
	}
	return v, nil
}

func (s *Segment) readRecord() (*Record, error) {
	var (
		crc   int64
		keySz int64
		valSz int64
	)
	rec := Record{}
	if err := binary.Read(s.f, binary.LittleEndian, &crc); err != nil {
		return nil, err
	}
	// if crc != 10 {
	// 	fmt.Println("here")
	// 	return nil, fmt.Errorf("wrong crc")
	// }

	if err := binary.Read(s.f, binary.LittleEndian, &rec.tstamp); err != nil {
		return nil, err
	}

	if err := binary.Read(s.f, binary.LittleEndian, &keySz); err != nil {
		return nil, err
	}
	key := make([]byte, keySz)
	if err := binary.Read(s.f, binary.LittleEndian, &key); err != nil {
		return nil, err
	}
	rec.key = key
	if err := binary.Read(s.f, binary.LittleEndian, &valSz); err != nil {
		return nil, err
	}
	val := make([]byte, valSz)
	if err := binary.Read(s.f, binary.LittleEndian, &val); err != nil {
		return nil, err
	}
	rec.val = val

	return &rec, nil
}

func (s *Segment) writeRecord(rec *Record) (int64, error) {
	offset, err := s.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return -1, err
	}
	// TODO: crc
	if err := binary.Write(s.f, binary.LittleEndian, int64(10)); err != nil {
		return -1, err
	}

	if err := binary.Write(s.f, binary.LittleEndian, rec.tstamp); err != nil {
		return -1, err
	}

	if err := binary.Write(s.f, binary.LittleEndian, int64(len(rec.key))); err != nil {
		return -1, err
	}
	if err := binary.Write(s.f, binary.LittleEndian, rec.key); err != nil {
		return -1, err
	}

	if err := binary.Write(s.f, binary.LittleEndian, int64(len(rec.val))); err != nil {
		return -1, err
	}
	if err := binary.Write(s.f, binary.LittleEndian, rec.val); err != nil {
		return -1, err
	}

	return offset, nil
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
