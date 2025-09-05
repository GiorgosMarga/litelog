package store

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

func clearDB() {
	entries, err := os.ReadDir("db")
	if err != nil {
		return
	}
	for _, entry := range entries {
		os.Remove(fmt.Sprintf("./db/%s", entry.Name()))
	}

}
func TestSimpleLog(t *testing.T) {
	log := NewLog()

	key := []byte("Hello")
	val := []byte("World")
	log.write(key, val)

	v, err := log.read(log.activeSegment, 0, int64(len(val)))
	if err != nil {
		t.FailNow()
	}

	fmt.Println(string(v))
	clearDB()
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
	clearDB()
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
	// time.Sleep(500 * time.Millisecond)
	// for range n {
	// 	i := rand.Intn(n)
	// 	key := fmt.Appendf(nil, "key_%d", i)
	// 	val := fmt.Appendf(nil, "val_%d", i)

	// }
	// clearDB()
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
	fmt.Println("Stopped")
	s = NewStore()
	for i := range n {
		key := fmt.Appendf(nil, "key_%d", i)
		val := fmt.Appendf(nil, "val_%d", i)
		v, err := s.Read(key)
		if err != nil {
			fmt.Println(err)
			t.FailNow()
		}
		if !bytes.Equal(v, val) {
			fmt.Printf("Expected %s, got %s\n", string(val), string(v))
		}
	}

}
