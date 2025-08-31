package store

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

func TestSimpleLog(t *testing.T) {
	log := NewLog()

	key := []byte("Hello")
	val := []byte("World")

	log.write(key, val)

	v, err := log.read(0)
	if err != nil {
		t.FailNow()
	}

	fmt.Println(string(v))
}

func TestMultipleWritesLog(t *testing.T) {
	n := 100_000
	store := NewStore()

	for i := range n {
		err := store.Write(fmt.Appendf(nil, "key_%d", i), fmt.Appendf(nil, "val_%d", i))
		if err != nil {
			t.FailNow()
		}
	}

	for range n {
		i := rand.Intn(n)
		expectedVal := fmt.Appendf(nil, "val_%d", i)
		key := fmt.Appendf(nil, "key_%d", i)
		val, err := store.Read(key)
		if err != nil {
			t.FailNow()
		}
		if !bytes.Equal(expectedVal, val) {
			fmt.Printf("Different val. Expected %s got %s\n", string(expectedVal), string(val))
			t.FailNow()
		}
	}

}
