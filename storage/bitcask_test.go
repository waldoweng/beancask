package storage

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	beancaskError "github.com/waldoweng/beancask/errors"
)

var b *Bitcask

func TestBitcask_Set(t *testing.T) {
	err := b.Set("test key", "test value")
	if err != nil {
		t.Error("TestBitcask_Set fail")
	}
}

func TestBitcask_Get(t *testing.T) {
	value, err := b.Get("test empty value key")
	if err != beancaskError.ErrorDataNotFound {
		t.Errorf("TestBitcask_Get get value fail err:%s\n", err.Error())
	}

	if value != "" {
		t.Errorf("TestBitcask_Get get value get[%s] want [%s]\n", value, "")
	}
}

func TestBitcask_Delete(t *testing.T) {
	testKey := "test delete key"
	testValue := "test delete value"

	value, err := b.Get(testKey)
	if err != beancaskError.ErrorDataNotFound {
		t.Errorf("TestBitcask_Delete get value fail err:%s\n", err.Error())
	}
	if value != "" {
		t.Errorf("TestBitcask_Delete get value get[%s] want [%s]\n", value, "")
	}

	if err = b.Set(testKey, testValue); err != nil {
		t.Error("TestBitcask_Delete set value fail")
	}

	value, err = b.Get(testKey)
	if err != nil {
		t.Errorf("TestBitcask_Delete get value fail:[%s]\n", err.Error())
	}
	if value != testValue {
		t.Errorf("TestBitcask_Delete get value got[%s] want[%s]\n", value, testValue)
	}

	if err = b.Delete(testKey); err != nil {
		t.Errorf("TestBitcask_Delete delete fail [%s]\n", err.Error())
	}

	value, err = b.Get(testKey)
	if err != beancaskError.ErrorDataNotFound {
		t.Errorf("TestBitcask_Delete get value found\n")
	}
}

func TestBitcask_SimpleGet(t *testing.T) {
	err := b.Set("test key", "test value of get")
	if err != nil {
		t.Error("TestSimpleGet set value fail")
	}

	value, err := b.Get("test key")
	if err != nil {
		t.Errorf("TestSimpleGet get value err:%s\n", err.Error())
	} else {
		if value != "test value of get" {
			t.Errorf("TestSimpleGet get value got [%s] want [%s]\n", value, "test value of get")
		}
	}
}

func TestBitcask_AllSet(t *testing.T) {
	for i := 0; i < 5000; i++ {
		err := b.Set(fmt.Sprintf("test all set key %d", i), fmt.Sprintf("test all set value %d", i))
		if err != nil {
			t.Errorf("TestAllSet write %d fail\n", i)
		}
	}
}

func TestBitcask_AllGet(t *testing.T) {
	localMap := make(map[string]string)
	var localQueue []struct {
		key   string
		value string
	}
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("test all read key %d", rand.Intn(1000))
		value := fmt.Sprintf("test all read value %d", rand.Intn(1000000000))
		localMap[key] = value
		localQueue = append(localQueue, struct {
			key   string
			value string
		}{
			key:   key,
			value: value,
		})
	}

	var wg sync.WaitGroup
	var concurrency int
	for i := 0; i < len(localQueue); i++ {
		if concurrency%20 == 0 {
			wg.Wait()
		}

		concurrency++
		key := localQueue[i].key
		value := localQueue[i].value
		if localMap[key] != value {
			continue
		}

		go func() {
			b.Set(key, value)
			wg.Done()
		}()
		wg.Add(1)
	}
	wg.Wait()

	for k, v := range localMap {
		value, err := b.Get(k)
		if err != nil {
			t.Errorf("TestAllGet read key[%s] fail[%s]\n", k, err.Error())
		}
		if value != v {
			t.Errorf("TestAllGet read key[%s] got[%s] want[%s]\n", k, value, v)
		}
	}
}

func BenchmarkBitcask_AllWrite(benchmark *testing.B) {
	for i := 0; i < benchmark.N; i++ {
		err := b.Set(fmt.Sprintf("benchmark all write key %d", i), fmt.Sprintf("benchmark all write value %d", i))
		if err != nil {
			benchmark.Errorf("BenchmarkAllWrite write %d fail\n", i)
		}
	}
}

func BenchmarkBitcask_AllRead(benchmark *testing.B) {
	benchmark.StopTimer()
	localMap := make(map[string]string)
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("benchmark all read key %d", rand.Intn(1000000))
		value := fmt.Sprintf("benchmark all read value %d", rand.Intn(1000000))
		localMap[key] = value
		err := b.Set(key, value)
		if err != nil {
			benchmark.Errorf("BenchmarkAllRead prepare data %d fail\n", i)
		}
	}

	benchmark.StartTimer()
	for i := 0; i < benchmark.N; {
		for k, v := range localMap {
			value, err := b.Get(k)
			if err != nil {
				benchmark.Errorf("BenchmarkAllRead read key[%s] fail\n", k)
			}
			if value != v {
				benchmark.Errorf("BenchmarkAllRead read key[%s] got[%s] want[%s]\n", k, value, v)
			}
			i++
			if i >= benchmark.N {
				break
			}
		}
	}
}

func BenchmarkBitcask_RandomReadWrite(benchmark *testing.B) {
	for i := 0; i < benchmark.N; i++ {
		switch rand.Intn(2) {
		case 0:
			randn := rand.Intn(100000)
			key := fmt.Sprintf("benchmark random read write key %d", randn)
			_, err := b.Get(key)
			if err != nil && err != beancaskError.ErrorDataNotFound {
				benchmark.Errorf("BenchmarkRandomReadWrite read key[%s] fail\n", key)
			}
		case 1:
			randn := rand.Intn(100000)
			key := fmt.Sprintf("benchmark random read write key %d", randn)
			value := fmt.Sprintf("benchmark random read write value %d", randn)
			err := b.Set(key, value)
			if err != nil {
				benchmark.Errorf("BenchmarkRandomReadWrite write key[%s] value[%s] fail\n", key, value)
			}
		}
	}
}

func BenchmarkBitcask_ConcurrentWrite(benchmark *testing.B) {
	benchmark.SetParallelism(20)
	benchmark.RunParallel(func(pb *testing.PB) {
		var i int32
		for pb.Next() {
			atomic.AddInt32(&i, 1)
			b.Set(fmt.Sprintf("benchmark concurrent write key %d", i), fmt.Sprintf("benchmark concurrent write value %d", i))
		}
	})
}

func TestMain(m *testing.M) {
	log.SetFlags(log.Ltime | log.Lshortfile)
	b = NewBitcask(CreateOption{})
	result := m.Run()
	b.Destory()
	os.Exit(result)
}
