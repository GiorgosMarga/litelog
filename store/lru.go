package store

import "sync"

type node struct {
	key  int64
	val  *Segment
	next *node
}

type lru struct {
	maxSz     int
	currentSz int
	head      *node
	tail      *node
	kv        map[int64]*Segment
	mtx       *sync.Mutex
}

func NewLRU(maxSz int) *lru {
	return &lru{
		maxSz: maxSz,
		kv:    make(map[int64]*Segment),
		mtx:   &sync.Mutex{},
	}
}
func (l *lru) reset() {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	l.kv = make(map[int64]*Segment)
	l.head = nil
	l.tail = nil
	l.currentSz = 0
}
func (l *lru) add(k int64, f *Segment) {
	n := node{
		val: f,
		key: k,
	}

	l.mtx.Lock()
	defer l.mtx.Unlock()
	l.kv[k] = f
	if l.head == nil {
		l.head = &n
		l.tail = &n
		l.currentSz++
		return
	}
	if l.currentSz == l.maxSz {
		l.head.val.close()
		delete(l.kv, l.head.key)
		l.head = l.head.next
		l.currentSz--
	} else {
		l.tail.next = &n
		l.tail = &n
	}

	l.currentSz++
}

func (l *lru) get(k int64) (f *Segment) {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	var prev *node = nil
	for curr := l.head; curr != nil; curr = curr.next {
		if curr.key == k {
			if curr != l.head {
				prev.next = curr.next
				curr.next = l.head
				l.head = curr
				if curr == l.tail {
					l.tail = prev
				}
			}
			break
		}
		prev = curr
	}
	s, ok := l.kv[k]
	if !ok {
		return nil
	}
	return s
}
