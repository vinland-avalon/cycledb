package tsm_test

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"cycledb/pkg/tsdb"
	"cycledb/pkg/tsdb/engine/tsm"
)

func TestCacheCheckConcurrentReadsAreSafe(t *testing.T) {
	values := make(tsm.Values, 1000)
	timestamps := make([]int64, len(values))
	series := make([][]byte, 100)
	for i := range timestamps {
		timestamps[i] = int64(rand.Int63n(int64(len(values))))
	}

	for i := range values {
		values[i] = tsm.NewValue(timestamps[i*len(timestamps)/len(values)], float64(i))
	}

	for i := range series {
		series[i] = []byte(fmt.Sprintf("series%d", i))
	}

	wg := sync.WaitGroup{}
	c := tsm.NewCache(1000000, tsdb.EngineTags{})

	ch := make(chan struct{})
	for _, s := range series {
		for _, v := range values {
			c.Write(s, tsm.Values{v})
		}
		wg.Add(3)
		go func(s []byte) {
			defer wg.Done()
			<-ch
			c.Values(s)
		}(s)
		go func(s []byte) {
			defer wg.Done()
			<-ch
			c.Values(s)
		}(s)
		go func(s []byte) {
			defer wg.Done()
			<-ch
			c.Values(s)
		}(s)
	}
	close(ch)
	wg.Wait()
}

func TestCacheRace(t *testing.T) {
	values := make(tsm.Values, 1000)
	timestamps := make([]int64, len(values))
	series := make([][]byte, 100)
	for i := range timestamps {
		timestamps[i] = int64(rand.Int63n(int64(len(values))))
	}

	for i := range values {
		values[i] = tsm.NewValue(timestamps[i*len(timestamps)/len(values)], float64(i))
	}

	for i := range series {
		series[i] = []byte(fmt.Sprintf("series%d", i))
	}

	wg := sync.WaitGroup{}
	c := tsm.NewCache(1000000, tsdb.EngineTags{})

	ch := make(chan struct{})
	for _, s := range series {
		for _, v := range values {
			c.Write(s, tsm.Values{v})
		}
		wg.Add(1)
		go func(s []byte) {
			defer wg.Done()
			<-ch
			c.Values(s)
		}(s)
	}

	errC := make(chan error)
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ch
		s, err := c.Snapshot()
		if err == tsm.ErrSnapshotInProgress {
			return
		}

		if err != nil {
			errC <- fmt.Errorf("failed to snapshot cache: %v", err)
			return
		}

		s.Deduplicate()
		c.ClearSnapshot(true)
	}()

	close(ch)

	go func() {
		wg.Wait()
		close(errC)
	}()

	for err := range errC {
		if err != nil {
			t.Error(err)
		}
	}
}

func TestCacheRace2Compacters(t *testing.T) {
	values := make(tsm.Values, 1000)
	timestamps := make([]int64, len(values))
	series := make([][]byte, 100)
	for i := range timestamps {
		timestamps[i] = int64(rand.Int63n(int64(len(values))))
	}

	for i := range values {
		values[i] = tsm.NewValue(timestamps[i*len(timestamps)/len(values)], float64(i))
	}

	for i := range series {
		series[i] = []byte(fmt.Sprintf("series%d", i))
	}

	wg := sync.WaitGroup{}
	c := tsm.NewCache(1000000, tsdb.EngineTags{})

	ch := make(chan struct{})
	for _, s := range series {
		for _, v := range values {
			c.Write(s, tsm.Values{v})
		}
		wg.Add(1)
		go func(s []byte) {
			defer wg.Done()
			<-ch
			c.Values(s)
		}(s)
	}
	fileCounter := 0
	mapFiles := map[int]bool{}
	mu := sync.Mutex{}
	errC := make(chan error)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-ch
			s, err := c.Snapshot()
			if err == tsm.ErrSnapshotInProgress {
				return
			}

			if err != nil {
				errC <- fmt.Errorf("failed to snapshot cache: %v", err)
				return
			}

			mu.Lock()
			mapFiles[fileCounter] = true
			fileCounter++
			myFiles := map[int]bool{}
			for k, e := range mapFiles {
				myFiles[k] = e
			}
			mu.Unlock()
			s.Deduplicate()
			c.ClearSnapshot(true)
			mu.Lock()
			defer mu.Unlock()
			for k := range myFiles {
				if _, ok := mapFiles[k]; !ok {
					errC <- fmt.Errorf("something else deleted one of my files")
					return
				} else {
					delete(mapFiles, k)
				}
			}
		}()
	}
	close(ch)

	go func() {
		wg.Wait()
		close(errC)
	}()

	for err := range errC {
		if err != nil {
			t.Error(err)
		}
	}
}
