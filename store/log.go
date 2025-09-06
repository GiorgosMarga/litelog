package store

// headers: crc + tstamp + ksz + vsz + ( k + v )
const (
	CRC_SIZE     = 8
	TSTAMP_SIZE  = 8
	KEY_SIZE     = 8
	VALUES_SIZE  = 8
	HEADERS_SIZE = CRC_SIZE + TSTAMP_SIZE + KEY_SIZE + VALUES_SIZE
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
	// total bytes: tstamp_length + key_size + value_size + k + v
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

func (l *Log) read(pos, valSz int64) ([]byte, error) {
	return l.activeSegment.read(pos, valSz)
}
