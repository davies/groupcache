/*
Copyright 2013 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package consistenthash

import (
	"fmt"
	"log"
	"strconv"
	"testing"

	"github.com/spaolacci/murmur3"
)

func TestHashing(t *testing.T) {

	// Override the hash function to return easier to reason about values. Assumes
	// the keys can be converted to an integer.
	hash := New(3, func(key []byte) uint32 {
		i, err := strconv.Atoi(string(key))
		if err != nil {
			panic(err)
		}
		return uint32(i)
	})

	// Given the above hash function, this will give replicas with "hashes":
	// 2, 4, 6, 12, 14, 16, 22, 24, 26
	hash.Add("6", "4", "2")

	testCases := map[string]string{
		"2":  "2",
		"11": "2",
		"23": "4",
		"27": "2",
	}

	for k, v := range testCases {
		if hash.Get(k) != v {
			t.Errorf("Asking for %s, should have yielded %s", k, v)
		}
	}

	// Adds 8, 18, 28
	hash.Add("8")

	// 27 should now map to 8.
	testCases["27"] = "8"

	for k, v := range testCases {
		if hash.Get(k) != v {
			t.Errorf("Asking for %s, should have yielded %s", k, v)
		}
	}

}

func TestConsistency(t *testing.T) {
	hash1 := New(1, nil)
	hash2 := New(1, nil)

	hash1.Add("Bill", "Bob", "Bonny")
	hash2.Add("Bob", "Bonny", "Bill")

	if hash1.Get("Ben") != hash2.Get("Ben") {
		t.Errorf("Fetching 'Ben' from both hashes should be the same")
	}

	hash2.Add("Becky", "Ben", "Bobby")

	if hash1.Get("Ben") != hash2.Get("Ben") ||
		hash1.Get("Bob") != hash2.Get("Bob") ||
		hash1.Get("Bonny") != hash2.Get("Bonny") {
		t.Errorf("Direct matches should always return the same entry")
	}

}

func testDist(hash *Map, N int, tries int, scale float64) (int, int) {
	hash.adjust(tries, scale)
	st := make(map[string]int)
	result := make(map[int]string)
	for i := 0; i < N; i++ {
		h := hash.Get(fmt.Sprintf("key%d", i))
		result[i] = h
		st[h]++
	}
	//avg := N / len(st)
	var max, min = 0, N
	for _, v := range st {
		//	println(h, v-avg)
		if v > max {
			max = v
		}
		if v < min {
			min = v
		}
	}
	var changed int
	hash.Add("0.0.0.0")
	hash.adjust(tries, scale)
	for i := 0; i < N; i++ {
		h := hash.Get(fmt.Sprintf("key%d", i))
		if result[i] != h {
			changed++
		}
	}
	hash.Remove("0.0.0.0")
	return max - min, changed
}

func TestBalance(t *testing.T) {
	hash := New(100, murmur3.Sum32)
	M := 20
	for i := 0; i < M; i++ {
		hash.Add(fmt.Sprintf("192.168.%d.%d", i, i))
	}
	N := 100000
	scales := []float64{0, 0.5, 0.75, 0.9, 1}
	for _, s := range scales {
		for t := 1; t < 8; t++ {
			diff, moved := testDist(hash, N, t, s)
			log.Printf("balance %.1f %d %.1f%% %.1f%%", s, t, float64(diff)*(float64(M)/float64(N))*100, float64(moved)/float64(N)*100)
		}
	}
}

func BenchmarkGet8(b *testing.B)   { benchmarkGet(b, 8) }
func BenchmarkGet32(b *testing.B)  { benchmarkGet(b, 32) }
func BenchmarkGet128(b *testing.B) { benchmarkGet(b, 128) }
func BenchmarkGet512(b *testing.B) { benchmarkGet(b, 512) }

func benchmarkGet(b *testing.B, shards int) {

	hash := New(3000, nil)

	var buckets []string
	for i := 0; i < shards; i++ {
		buckets = append(buckets, fmt.Sprintf("shard-%d", i))
	}

	hash.Add(buckets...)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		hash.Get(buckets[i&(shards-1)])
	}
}
