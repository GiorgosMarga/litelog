package store

import (
	"os"
)

type node struct {
	key  string
	val  *os.File
	next *node
}

type lru struct {
	maxSz     int
	currentSz int
	head      *node
	tail      *node
}

func NewLRU(maxSz int) *lru {
	return &lru{
		maxSz: maxSz,
	}
}

func (l *lru) add(k string, f *os.File) {
	n := node{
		val: f,
		key: k,
	}
	if l.head == nil {
		l.head = &n
		l.tail = &n
		l.currentSz++
		return
	}
	if l.currentSz == l.maxSz {
		l.head.val.Close()
		l.head = l.head.next
		l.currentSz--
	} else {
		l.tail.next = &n
		l.tail = &n
	}
	l.currentSz++
}

func (l *lru) get(k string) (f *os.File) {
	curr := l.head
	for curr != nil {
		if curr.key == k {
			l.tail.next = curr
			l.tail = curr
			curr.next = nil
			l.head = l.head.next
			return curr.val
		}
		curr = curr.next
	}
	return nil
}
