package store

type Record struct {
	crc    int32
	tstamp int64
	key    []byte
	val    []byte
}
