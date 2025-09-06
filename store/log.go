package store

// headers: crc + tstamp + ksz + vsz + ( k + v )
const (
	CRC_SIZE       = 4
	TSTAMP_SIZE    = 8
	KEY_SIZE       = 4
	VALUE_SIZE     = 4
	OFFSET_SIZE    = 8
	MAX_KEY_SIZE   = 64 * 1024        // 64 KB
	MAX_VALUE_SIZE = 16 * 1024 * 1024 // 16 MB
	HEADERS_SIZE   = CRC_SIZE + TSTAMP_SIZE + KEY_SIZE + VALUE_SIZE
)

type Log struct {
	activeSegment *Segment
}

func NewLog() *Log {
	segment, err := createNewSegment()
	if err != nil {
		panic(err)
	}
	return &Log{
		activeSegment: segment,
	}

}

func (l *Log) write(k []byte, v []byte) (int64, error) {
	if len(k) > MAX_KEY_SIZE {
		return -1, ErrInvalidKeySize
	}
	if len(v) > MAX_VALUE_SIZE {
		return -1, ErrInvalidValueSize
	}
	// total bytes: crc + tstamp_length + key_size + value_size + k + v
	totalSize := HEADERS_SIZE + len(k) + len(v)
	sz, err := l.activeSegment.getSize()
	if err != nil {
		return -1, err
	}
	if sz+int64(totalSize) >= 4<<16 {
		segment, err := createNewSegment()
		if err != nil {
			return -1, err
		}
		l.activeSegment = segment
	}
	return l.activeSegment.write(k, v)
}

func (l *Log) read(pos int64 , valSz int32) ([]byte, error) {
	return l.activeSegment.read(pos, valSz)
}
