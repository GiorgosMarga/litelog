package store

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

func clearDB() {
	os.RemoveAll("./db")
	os.RemoveAll("./hint")

}
func TestSimpleStore(t *testing.T) {
	clearDB()
	s := NewStore()

	key := []byte("Hello")
	val := []byte("World")
	s.Write(key, val)

	v, err := s.Read(key)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	if !bytes.Equal(v, val) {
		fmt.Printf("Expected %s got %s\n", string(val), string(v))
	}
	// clearDB()
}

func TestMultipleWritesLog(t *testing.T) {
	clearDB()
	n := 100_000
	store := NewStore()

	for i := range n {
		err := store.Write(fmt.Appendf(nil, "key_%d", i), fmt.Appendf(nil, "val_%d", i))
		if err != nil {
			fmt.Println(err)
			t.FailNow()
		}
	}

	time.Sleep(1 * time.Second)
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
	// clearDB()
}

func TestMultipleWritesParallel(t *testing.T) {
	n := 100_000
	clearDB()
	store := NewStore()

	wg := &sync.WaitGroup{}

	for i := range n {
		wg.Go(
			func() {
				store.Write(fmt.Appendf(nil, "key_%d", i), fmt.Appendf(nil, "val_%d", i))
			})
	}
	wg.Wait()
	for range n {
		wg.Go(
			func() {
				i := rand.Intn(n)
				expectedVal := fmt.Appendf(nil, "val_%d", i)
				key := fmt.Appendf(nil, "key_%d", i)
				val, err := store.Read(key)
				if err != nil {
					fmt.Println(err)
					return
				}
				if !bytes.Equal(expectedVal, val) {
					fmt.Printf("Different val. Expected %s got %s\n", string(expectedVal), string(val))
					return
				}
			},
		)
	}
	wg.Wait()
	// store.merge()
	clearDB()
}

func TestMerge(t *testing.T) {
	clearDB()
	s := NewStore()
	n := 100_000
	wg := &sync.WaitGroup{}
	for i := range n {
		wg.Go(func() {
			time.Sleep(time.Duration(rand.Intn(1000) * int(time.Millisecond)))
			key := fmt.Appendf(nil, "key_%d", i)
			val := fmt.Appendf(nil, "val_%d", i)
			s.Write(key, val)
			time.Sleep(time.Duration(rand.Intn(2000) * int(time.Millisecond)))
			v, err := s.Read(key)
			if err != nil {
				log.Println(err)
			}
			if !bytes.Equal(v, val) {
				fmt.Printf("expected: %s got %s\n", string(val), string(v))
			}
		})
	}

	wg.Wait()
}

func TestStoreSaveLoad(t *testing.T) {
	clearDB()
	s := NewStore()
	n := 100_000
	for i := range n {
		key := fmt.Appendf(nil, "key_%d", i)
		val := fmt.Appendf(nil, "val_%d", i)
		s.Write(key, val)
	}
	s.Stop()
	s = NewStore()
	for i := range n {
		key := fmt.Appendf(nil, "key_%d", i)
		val := fmt.Appendf(nil, "val_%d", i)
		v, err := s.Read(key)
		if err != nil {
			fmt.Printf("[%s]: %s\n", string(key), err.Error())
			t.FailNow()
		}
		if !bytes.Equal(v, val) {
			fmt.Printf("Expected %s, got %s\n", string(val), string(v))
		}
	}

}

func TestDelete(t *testing.T) {
	clearDB()
	s := NewStore()
	n := 100_000
	for i := range n {
		key := fmt.Appendf(nil, "key_%d", i)
		val := fmt.Appendf(nil, "val_%d", i)
		if err := s.Write(key, val); err != nil {
			fmt.Println(err)
			t.FailNow()
		}
		if err := s.Delete(key); err != nil {
			fmt.Println(err)
			t.FailNow()
		}
	}
	fmt.Println("Deleted all")
	for i := range n {
		key := fmt.Appendf(nil, "key_%d", i)
		v, err := s.Read(key)
		if err == nil {
			fmt.Printf("Key: %s val: %s\n", string(key), string(v))
			t.FailNow()
		}
		if !errors.Is(err, ErrNotFound) {
			fmt.Println(err)
			t.FailNow()
		}
	}

}

func TestDeleteWithStop(t *testing.T) {
	clearDB()
	s := NewStore()
	n := 100_000
	for i := range n {
		key := fmt.Appendf(nil, "key_%d", i)
		val := fmt.Appendf(nil, "val_%d", i)
		if err := s.Write(key, val); err != nil {
			fmt.Println(err)
			t.FailNow()
		}
		if err := s.Delete(key); err != nil {
			fmt.Println(err)
			t.FailNow()
		}
	}
	fmt.Println("Deleted all")
	s.Stop()
	s = NewStore()
	for i := range n {
		key := fmt.Appendf(nil, "key_%d", i)
		v, err := s.Read(key)
		if err == nil {
			fmt.Printf("Key: %s val: %s\n", string(key), string(v))
			t.FailNow()
		}
		if !errors.Is(err, ErrNotFound) {
			fmt.Println(err)
			t.FailNow()
		}
	}

}

func TestLru(t *testing.T) {
	lru := NewLRU(20)
	for range 1_000_000 {
		c := rand.Intn(10)
		n := rand.Intn(2)
		if n == 0 {
			lru.add(int64(c), &Segment{})
		} else {
			lru.get(int64(c))
		}
	}
}
