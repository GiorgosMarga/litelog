package store

type Record struct {
	tstamp int64
	key    []byte
	val    []byte
}
