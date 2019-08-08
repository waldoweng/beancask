package storage

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	beancaskError "github.com/waldoweng/beancask/errors"
)

var b *Bitcask

func TestSimpleSet(t *testing.T) {
	err := b.Set("test key", "test value")
	if err != nil {
		t.Error("TestSimpleSet fail")
	}
}

func TestEmptyGet(t *testing.T) {
	value, err := b.Get("test empty value key")
	if err != beancaskError.ErrorDataNotFound {
		t.Errorf("TestEmptyGet get value fail err:%s\n", err.Error())
	}

	if value != "" {
		t.Errorf("TestEmptyGet get value get[%s] want [%s]\n", value, "")
	}
}

func TestSimpleGet(t *testing.T) {
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

func TestAllSet(t *testing.T) {
	for i := 0; i < 5000; i++ {
		err := b.Set(fmt.Sprintf("test all set key %d", i), fmt.Sprintf("test all set value %d", i))
		if err != nil {
			t.Errorf("TestAllSet write %d fail\n", i)
		}
	}
}

func TestAllGet(t *testing.T) {
	for i := 0; i < 5000; i++ {
		err := b.Set(fmt.Sprintf("test all get key %d", i), fmt.Sprintf("test all get value %d", i))
		if err != nil {
			t.Errorf("TestAllSet write %d fail\n", i)
		}
	}

	for i := 0; i < 5000; i++ {
		key := fmt.Sprintf("test all get key %d", i)
		expectValue := fmt.Sprintf("test all get value %d", i)

		value, err := b.Get(key)
		if err != nil {
			t.Errorf("TestAllGet read key[%s] fail\n", key)
		}
		if value != expectValue {
			t.Errorf("TestAllGet read key[%s] got[%s] want[%s]\n", key, value, expectValue)
		}
	}
}

func BenchmarkAllWrite(benchmark *testing.B) {
	for i := 0; i < benchmark.N; i++ {
		err := b.Set(fmt.Sprintf("benchmark all write key %d", i), fmt.Sprintf("benchmark all write value %d", i))
		if err != nil {
			benchmark.Errorf("BenchmarkAllWrite write %d fail\n", i)
		}
	}
}

func BenchmarkAllRead(benchmark *testing.B) {
	benchmark.StopTimer()
	for i := 0; i < 10000; i++ {
		err := b.Set(fmt.Sprintf("benchmark all read key %d", i), fmt.Sprintf("benchmark all read value %d", i))
		if err != nil {
			benchmark.Errorf("BenchmarkAllRead prepare data %d fail\n", i)
		}
	}

	benchmark.StartTimer()
	for i := 0; i < benchmark.N; i++ {
		randn := rand.Intn(10000)
		key := fmt.Sprintf("benchmark all read key %d", randn)
		expectValue := fmt.Sprintf("benchmark all read value %d", randn)

		value, err := b.Get(key)
		if err != nil {
			benchmark.Errorf("BenchmarkAllRead read key[%s] fail\n", key)
		}
		if value != expectValue {
			benchmark.Errorf("BenchmarkAllRead read key[%s] got[%s] want[%s]\n", key, value, expectValue)
		}
	}
}

func BenchmarkRandomReadWrite(benchmark *testing.B) {
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

func TestMain(m *testing.M) {
	b = NewBitcask()
	result := m.Run()
	b.Destory()
	os.Exit(result)
}
