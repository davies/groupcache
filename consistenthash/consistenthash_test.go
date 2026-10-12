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
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/spaolacci/murmur3"
	twmbmurmur3 "github.com/twmb/murmur3"
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
	M := 50
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

func TestStableHash(t *testing.T) {
	hash := New(100, murmur3.Sum32)
	M := 50
	for i := 0; i < M; i++ {
		hash.Add(fmt.Sprintf("192.168.%d.%d", i, i))
	}
	hash.Adjust()
	N := 10000000
	result := make(map[int]string)
	for i := 0; i < N; i++ {
		h := hash.Get(fmt.Sprintf("key%d", i))
		result[i] = h
	}
	hash = New(100, murmur3.Sum32)
	for i := M - 1; i >= 0; i-- {
		hash.Add(fmt.Sprintf("192.168.%d.%d", i, i))
	}
	hash.Adjust()
	for i := N - 1; i >= 0; i-- {
		h := hash.Get(fmt.Sprintf("key%d", i))
		if h != result[i] {
			t.Fatalf("hash changed %d %s %s", i, h, result[i])
		}
	}
}

func TestGet2(t *testing.T) {
	hash := New(100, murmur3.Sum32)
	hash.AddWithWeight("node1", 5)
	n1, n2 := hash.Get2("key1")
	if n1 != "node1" || n2 != "" {
		t.Fatalf("get2 %s %s", n1, n2)
	}
	hash.AddWithWeight("node2", 5)
	n1, n2 = hash.Get2("key1")
	if n1 != "node1" || n2 != "node2" {
		t.Fatalf("get2 %s %s", n1, n2)
	}
	n1, n2 = hash.Get2("key2")
	if n1 != "node2" || n2 != "node1" {
		t.Fatalf("get2 %s %s", n1, n2)
	}
}

func TestIncrementalPreservesActivePoints(test *testing.T) {
	ring := benchmarkRing(30, 100)
	keys := append([]int(nil), ring.keys...)
	owners := make(map[int]string)
	for hash, node := range ring.hashMap {
		owners[hash] = node
	}
	before := make(map[string]int)
	for node, hashes := range ring.hashs {
		before[node] = len(hashes)
	}
	ring.Add("added")
	for node, count := range before {
		if len(ring.hashs[node]) != count {
			test.Fatalf("adding a node changed %s from %d to %d points", node, count, len(ring.hashs[node]))
		}
	}
	ring.Remove("added")
	if !reflect.DeepEqual(keys, ring.keys) || !reflect.DeepEqual(owners, ring.hashMap) {
		test.Fatal("adding then removing a node changed existing routing")
	}
	for node, count := range before {
		if len(ring.hashs[node]) != count {
			test.Fatalf("removing a node changed %s", node)
		}
	}
	if !sort.IntsAreSorted(ring.keys) {
		test.Fatal("incremental update left unsorted points")
	}
}

func checkActiveRing(test *testing.T, ring *Map) {
	test.Helper()
	expected := make([]int, 0)
	owners := make(map[int]map[string]int)
	winners := make(map[int]string)
	for node, hashes := range ring.hashs {
		for _, hash := range hashes {
			expected = append(expected, hash)
			if owners[hash] == nil {
				owners[hash] = make(map[string]int)
				winners[hash] = node
			}
			owners[hash][node]++
			if node < winners[hash] {
				winners[hash] = node
			}
		}
	}
	sort.Ints(expected)
	if len(expected) != len(ring.keys) || len(expected) > 0 && !reflect.DeepEqual(expected, ring.keys) {
		test.Fatal("active point multiset differs from the sorted ring")
	}
	for hash, candidates := range owners {
		var count int
		for _, replicas := range candidates {
			count += replicas
		}
		if count == 1 {
			delete(owners, hash)
		}
	}
	if !reflect.DeepEqual(winners, ring.hashMap) || !reflect.DeepEqual(owners, ring.owners) {
		test.Fatal("point ownership differs from active points")
	}
}

func TestIncrementalWeightAndHashes(test *testing.T) {
	hashCalls := 0
	ring := New(3, func(key []byte) uint32 {
		hashCalls++
		return murmur3.Sum32(key)
	})
	ring.AddWithWeight("first", 12)
	ring.AddWithWeight("second", 8)
	ring.calc(map[string]int{"first": 4, "second": 6})
	if len(ring.hashs["first"]) != 4 || len(ring.hashs["second"]) != 6 {
		test.Fatal("calc did not set the active hash lengths")
	}
	checkHashes := func(expectedCalls int) {
		test.Helper()
		if hashCalls != expectedCalls {
			test.Fatalf("hash calls = %d, want %d", hashCalls, expectedCalls)
		}
		for node, hashes := range ring.hashs {
			for index, hash := range hashes {
				key := strconv.Itoa(index) + node
				if hash != int(murmur3.Sum32([]byte(key))) {
					test.Fatalf("active point %s changed value", key)
				}
			}
		}
	}
	checkHashes(20)
	for _, step := range []struct{ weight, calls int }{
		{9, 25}, {2, 25}, {15, 38}, {1, 38}, {15, 52},
		{16, 53}, {17, 54}, {33, 70}, {2, 70}, {32, 100},
	} {
		ring.AddWithWeight("first", step.weight)
		if len(ring.hashs["first"]) != step.weight || len(ring.hashs["second"]) != 6 {
			test.Fatal("weight update did not preserve the other node's adjusted count")
		}
		checkHashes(step.calls)
		checkActiveRing(test, ring)
	}
	for _, step := range []struct {
		counts map[string]int
		calls  int
	}{
		{map[string]int{"first": 40, "second": 6}, 108},
		{map[string]int{"first": 0, "second": -1}, 108},
		{map[string]int{"first": 5, "second": 6}, 119},
		{map[string]int{"first": 40, "second": 6}, 154},
		{map[string]int{"second": 6}, 154},
	} {
		ring.calc(step.counts)
		for node, hashes := range ring.hashs {
			count := step.counts[node]
			if count < 0 {
				count = 0
			}
			if len(hashes) != count {
				test.Fatalf("calc left %s with %d active points, want %d", node, len(hashes), count)
			}
		}
		checkHashes(step.calls)
		checkActiveRing(test, ring)
	}
	ring.Remove("first")
	checkActiveRing(test, ring)
	ring.AddWithWeight("first", 3)
	checkActiveRing(test, ring)
	ring.Adjust()
	if len(ring.hashs["second"]) != 8 {
		test.Fatal("Adjust did not restore configured weights")
	}
	checkHashes(159)
	checkActiveRing(test, ring)
	ring.Remove("first")
	ring.Remove("second")
	checkActiveRing(test, ring)
}

func TestIncrementalCollisions(test *testing.T) {
	ring := New(3, func([]byte) uint32 { return 17 })
	for _, node := range []string{"c", "a", "b"} {
		ring.Add(node)
		checkActiveRing(test, ring)
	}
	first, second := ring.Get2("key")
	if first != "a" || second != "b" {
		test.Fatalf("unexpected collision owners: %q, %q", first, second)
	}
	ring.AddWithWeight("a", 1)
	ring.Remove("a")
	checkActiveRing(test, ring)
	first, second = ring.Get2("key")
	if first != "b" || second != "c" {
		test.Fatalf("removed collision winner hid remaining owners: %q, %q", first, second)
	}
	ring.Remove("c")
	ring.Remove("b")
	checkActiveRing(test, ring)
	ring.Add("a", "c", "b")
	ring.Adjust()
	checkActiveRing(test, ring)
	first, second = ring.Get2("key")
	if first != "a" || second != "b" {
		test.Fatal("collision routing depends on insertion order")
	}
}

func TestDeferredAdjustCollisions(test *testing.T) {
	for _, nodes := range []int{3, 5} {
		test.Run(fmt.Sprintf("nodes=%d", nodes), func(test *testing.T) {
			ring := New(100, func([]byte) uint32 { return 17 })
			for index := 0; index < nodes; index++ {
				ring.Add(fmt.Sprintf("node-%d", index))
			}
			checkRouting := func(first, second string) {
				test.Helper()
				if !ring.Adjust() {
					test.Fatal("membership changes did not trigger adjustment")
				}
				checkActiveRing(test, ring)
				for node := range ring.replicas {
					if len(ring.hashs[node]) < 1 {
						test.Fatalf("adjustment removed every point of %s", node)
					}
				}
				if owner := ring.Get("key"); owner != first {
					test.Fatalf("primary owner = %q, want %q", owner, first)
				}
				if primary, backup := ring.Get2("key"); primary != first || backup != second {
					test.Fatalf("owners = (%q, %q), want (%q, %q)", primary, backup, first, second)
				}
			}
			checkRouting("node-0", "node-1")
			keys := append([]int(nil), ring.keys...)
			ring.Remove("node-0")
			checkRouting("node-1", "node-2")
			ring.Add("node-0")
			checkRouting("node-0", "node-1")
			if !reflect.DeepEqual(keys, ring.keys) {
				test.Fatal("collision ring depends on membership history")
			}
		})
	}
}

func TestDeferredAdjustSmallPeerWeights(test *testing.T) {
	nodes := []string{"172.31.0.0", "172.31.40.106", "172.31.45.195"}
	var canonical *Map
	for _, eager := range []bool{false, true} {
		test.Run(fmt.Sprintf("eager=%t", eager), func(test *testing.T) {
			ring := New(100, twmbmurmur3.Sum32)
			for _, node := range nodes {
				ring.AddWithWeight(node, 2)
				if eager {
					ring.Adjust()
				}
			}
			if len(ring.hashMap) != len(ring.keys) {
				test.Fatal("small-weight reproduction unexpectedly contains collisions")
			}
			ring.Adjust()
			checkActiveRing(test, ring)
			for _, node := range nodes {
				if len(ring.hashs[node]) < 1 || ring.replicas[node] != 2 {
					test.Fatalf("adjustment removed %s or changed its configured weight", node)
				}
			}
			for sample := 0; sample < 1000; sample++ {
				key := strconv.Itoa(sample)
				first, second := ring.Get2(key)
				if ring.replicas[first] != 2 || ring.replicas[second] != 2 || first == second || ring.Get(key) != first {
					test.Fatalf("invalid small-weight routing for %s: (%q, %q)", key, first, second)
				}
			}
			if canonical == nil {
				canonical = ring
			} else if !reflect.DeepEqual(canonical.keys, ring.keys) || !reflect.DeepEqual(canonical.hashMap, ring.hashMap) {
				test.Fatal("small-weight ring depends on adjustment history")
			}
		})
	}
}

func TestDeferredAdjustmentThreshold(test *testing.T) {
	for _, scenario := range []struct {
		adjustment int
		position   uint32
		first      int
		second     int
	}{
		{-2, 5 << 28, 8, 12},
		{-1, 6 << 28, 10, 10},
		{0, 8 << 28, 10, 10},
		{1, 10 << 28, 10, 10},
		{2, 11 << 28, 12, 8},
	} {
		test.Run(fmt.Sprintf("adjustment=%d", scenario.adjustment), func(test *testing.T) {
			ring := New(10, func(key []byte) uint32 {
				if strings.HasSuffix(string(key), "second") {
					return scenario.position
				}
				return 0
			})
			ring.Add("first", "second")
			ring.adjust(1, 0.75)
			if len(ring.hashs["first"]) != scenario.first || len(ring.hashs["second"]) != scenario.second {
				test.Fatalf("active counts = (%d, %d), want (%d, %d)", len(ring.hashs["first"]), len(ring.hashs["second"]), scenario.first, scenario.second)
			}
			checkActiveRing(test, ring)
			if scenario.adjustment >= -1 && scenario.adjustment <= 1 {
				var allocations [2]float64
				for index, tries := range []int{1, 5} {
					allocations[index] = testing.AllocsPerRun(10, func() {
						ring.dirty = true
						ring.adjust(tries, 0.75)
					})
				}
				if allocations[1] > allocations[0] {
					test.Fatalf("small adjustments caused redundant rebuilds: allocations %.0f -> %.0f", allocations[0], allocations[1])
				}
			}
		})
	}
}

func TestIncrementalSequences(test *testing.T) {
	for _, collisions := range []bool{false, true} {
		ring := New(3, func(key []byte) uint32 {
			hash := murmur3.Sum32(key)
			if collisions {
				return hash % 257
			}
			return hash
		})
		for index := 0; index < 200; index++ {
			node := fmt.Sprintf("node-%d", index*7%19)
			if index%3 == 0 {
				ring.Remove(node)
			} else {
				ring.AddWithWeight(node, 1+index*11%23)
			}
			if index%17 == 0 {
				ring.Adjust()
			}
			checkActiveRing(test, ring)
			if len(ring.replicas) > 1 {
				first, second := ring.Get2(node)
				if _, exists := ring.replicas[first]; !exists || first == second {
					test.Fatal("invalid primary owner")
				}
				if _, exists := ring.replicas[second]; !exists {
					test.Fatal("invalid backup owner")
				}
			}
		}
	}
}

func TestIncrementalMatchesFullRebuild(test *testing.T) {
	incremental, full := New(100, murmur3.Sum32), New(100, murmur3.Sum32)
	for index := 0; index < 60; index++ {
		node, weight := fmt.Sprintf("node-%d", index%30), 50+index*5
		if index%7 == 0 {
			weight = 0
		}
		membershipUpdate(incremental, "incremental-basic", node, weight)
		membershipUpdate(full, "full-basic", node, weight)
		if index%10 == 9 {
			incremental.Adjust()
			full.Adjust()
			checkActiveRing(test, incremental)
			if !reflect.DeepEqual(incremental.keys, full.keys) {
				test.Fatal("adjusted points differ from full membership rebuilding")
			}
			for sample := 0; sample < 10000; sample++ {
				key := strconv.Itoa(sample)
				first, second := incremental.Get2(key)
				wantFirst, wantSecond := full.Get2(key)
				if first != wantFirst || second != wantSecond {
					test.Fatalf("canonical routing differs for %s", key)
				}
			}
		}
	}
}

func TestIncrementalCanonicalLargeRing(test *testing.T) {
	forward, reverse := New(100, murmur3.Sum32), New(100, murmur3.Sum32)
	for index := 0; index < 2440; index++ {
		forward.Add(fmt.Sprintf("node-%d", index))
		reverse.Add(fmt.Sprintf("node-%d", 2439-index))
		if index%400 == 399 {
			forward.Adjust()
		}
	}
	forward.Adjust()
	reverse.Adjust()
	checkActiveRing(test, forward)
	checkActiveRing(test, reverse)
	if !reflect.DeepEqual(forward.keys, reverse.keys) || !reflect.DeepEqual(forward.hashMap, reverse.hashMap) {
		test.Fatal("large-ring routing depends on mutation or adjustment order")
	}
}

func TestDeferredAdjust(t *testing.T) {
	ring := New(100, murmur3.Sum32)
	if ring.Adjust() {
		t.Fatal("empty ring reported an adjustment")
	}
	if ring.Get("key") != "" {
		t.Fatal("empty ring returned a peer")
	}
	for _, node := range []string{"node1", "node2", "node3"} {
		ring.Add(node)
	}
	if !ring.dirty {
		t.Fatal("membership changes did not mark the ring dirty")
	}
	for index := 0; index < 1000; index++ {
		first, second := ring.Get2(strconv.Itoa(index))
		if _, exists := ring.replicas[first]; !exists || first == second || second == "" {
			t.Fatalf("invalid unadjusted lookup: %q, %q", first, second)
		}
	}
	if !ring.Adjust() {
		t.Fatal("dirty ring did not report an adjustment")
	}
	keys := append([]int(nil), ring.keys...)
	ring.Add("node1")
	ring.Remove("missing")
	if ring.Adjust() {
		t.Fatal("unchanged membership reported an adjustment")
	}
	if ring.dirty || !reflect.DeepEqual(keys, ring.keys) {
		t.Fatal("unchanged membership rebuilt the ring")
	}
	ring.Remove("node2")
	for index := 0; index < 1000; index++ {
		first, second := ring.Get2(strconv.Itoa(index))
		if first == "node2" || second == "node2" || first == second {
			t.Fatalf("removed peer still selected: %q, %q", first, second)
		}
	}
	ring.Remove("node1")
	ring.Remove("node3")
	if !ring.Adjust() {
		t.Fatal("removing the last peers did not report an adjustment")
	}
	first, second := ring.Get2("key")
	if !ring.IsEmpty() || first != "" || second != "" {
		t.Fatal("removing all peers did not empty the ring")
	}
}

func TestDeferredAdjustMatchesEager(t *testing.T) {
	eager := New(100, murmur3.Sum32)
	deferred := New(100, murmur3.Sum32)
	resized := New(100, murmur3.Sum32)
	for index := 0; index < 30; index++ {
		node := fmt.Sprintf("node-%d", index)
		weight := 50 + index*5
		eager.AddWithWeight(node, weight)
		eager.Adjust()
		deferred.AddWithWeight(node, weight)
		resized.AddWithWeight(node, weight+500)
		resized.AddWithWeight(node, weight)
	}
	for _, ring := range []*Map{eager, deferred, resized} {
		ring.Remove("node-3")
		if ring == eager {
			ring.Adjust()
		}
		ring.AddWithWeight("node-7", 250)
		ring.Adjust()
		checkActiveRing(t, ring)
	}
	for _, ring := range []*Map{deferred, resized} {
		if !reflect.DeepEqual(eager.keys, ring.keys) || !reflect.DeepEqual(eager.hashMap, ring.hashMap) || !reflect.DeepEqual(eager.owners, ring.owners) {
			t.Fatal("final ring depends on adjustment or weight history")
		}
		for index := 0; index < 10000; index++ {
			key := strconv.Itoa(index)
			first, second := eager.Get2(key)
			actualFirst, actualSecond := ring.Get2(key)
			if first != actualFirst || second != actualSecond {
				t.Fatalf("different final routing for %s: (%s, %s) != (%s, %s)", key, first, second, actualFirst, actualSecond)
			}
		}
	}
}

func BenchmarkUpdateRing(b *testing.B) {
	ring := New(100, murmur3.Sum32)
	for i := 0; i < 100; i++ {
		ring.Add(fmt.Sprintf("192.168.%d.%d", i, i))
	}
	for i := 0; i < b.N; i++ {
		ring.Add(fmt.Sprintf("192.169.%d.%d", i, i))
		ring.Get("")
		ring.Remove(fmt.Sprintf("192.169.%d.%d", i, i))
		ring.Get("")
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
	hash.Adjust()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		hash.Get(buckets[i&(shards-1)])
	}
}

func benchmarkRing(nodes, weight int) *Map {
	ring := New(100, murmur3.Sum32)
	for index := 0; index < nodes; index++ {
		ring.replicas[fmt.Sprintf("node-%d", index)] = weight
	}
	ring.dirty = true
	ring.Adjust()
	return ring
}

func fullRebuildCalc(ring *Map, replicas map[string]int) {
	ring.keys = ring.keys[:0]
	for node, count := range replicas {
		hashes := ring.hashs[node]
		for index := 0; index < count; index++ {
			var hash int
			if index < len(hashes) {
				hash = hashes[index]
			} else {
				hash = int(ring.hash([]byte(strconv.Itoa(index) + node)))
				hashes = append(hashes, hash)
			}
			ring.keys = append(ring.keys, hash)
			ring.hashMap[hash] = node
		}
		ring.hashs[node] = hashes
	}
	sort.Ints(ring.keys)
}

func fullRebuildAdjust(ring *Map) {
	if !ring.dirty {
		return
	}
	ring.dirty = false
	if len(ring.replicas) <= 1 || ring.replica < 10 {
		return
	}
	var total int
	counts := make(map[string]int, len(ring.replicas))
	for node, count := range ring.replicas {
		counts[node] = count
		total += count
	}
	for iteration := 0; iteration < 5; iteration++ {
		stat := make(map[string]int, len(ring.replicas))
		stat[ring.hashMap[ring.keys[0]]] = ring.keys[0] + int(1<<32) - ring.keys[len(ring.keys)-1]
		for index, hash := range ring.keys[1:] {
			stat[ring.hashMap[hash]] += hash - ring.keys[index]
		}
		var changed bool
		for node, space := range stat {
			actual := float64(space) / float64(1<<32)
			expected := float64(ring.replicas[node]) / float64(total)
			adjustment := int(float64(counts[node]) * (expected - actual) / expected * 0.75)
			if adjustment > 1 || adjustment < 1 {
				counts[node] += adjustment
				changed = true
			}
		}
		if !changed {
			return
		}
		fullRebuildCalc(ring, counts)
	}
}

func membershipUpdate(ring *Map, mode, node string, weight int) {
	if strings.HasPrefix(mode, "full-") {
		if ring.replicas[node] == weight {
			return
		}
		if weight == 0 {
			delete(ring.replicas, node)
			delete(ring.hashs, node)
		} else {
			ring.replicas[node] = weight
		}
		fullRebuildCalc(ring, ring.replicas)
		ring.dirty = true
	} else if weight == 0 {
		ring.Remove(node)
	} else {
		ring.AddWithWeight(node, weight)
	}
}

func membershipAdjust(ring *Map, mode string, durations *[]int64) {
	if !ring.dirty {
		return
	}
	started := time.Now()
	if strings.HasPrefix(mode, "full-") {
		fullRebuildAdjust(ring)
	} else {
		ring.Adjust()
	}
	*durations = append(*durations, time.Since(started).Nanoseconds())
}

var membershipModes = []string{"full-eager", "full-basic", "full-every100", "incremental-basic", "incremental-every100"}

func BenchmarkMembershipStartup(benchmark *testing.B) {
	for _, nodes := range []int{100, 600, 1000, 2440} {
		for _, mode := range membershipModes {
			benchmark.Run(fmt.Sprintf("nodes=%d/%s", nodes, mode), func(benchmark *testing.B) {
				var updates, adjustments []int64
				names := make([]string, nodes)
				for index := range names {
					names[index] = fmt.Sprintf("node-%d", index)
				}
				benchmark.ReportAllocs()
				benchmark.ResetTimer()
				for iteration := 0; iteration < benchmark.N; iteration++ {
					ring := New(100, murmur3.Sum32)
					for index, name := range names {
						started := time.Now()
						membershipUpdate(ring, mode, name, 100)
						updates = append(updates, time.Since(started).Nanoseconds())
						if mode == "full-eager" || strings.HasSuffix(mode, "every100") && (index+1)%100 == 0 {
							membershipAdjust(ring, mode, &adjustments)
						}
					}
					if strings.HasSuffix(mode, "every100") {
						membershipAdjust(ring, mode, &adjustments)
					}
				}
				benchmark.StopTimer()
				reportDurations(benchmark, "update", updates)
				reportDurations(benchmark, "adjust", adjustments)
			})
		}
	}
}

func BenchmarkMembershipChurn(benchmark *testing.B) {
	for _, nodes := range []int{100, 600, 1000, 2440} {
		for _, mode := range membershipModes {
			benchmark.Run(fmt.Sprintf("nodes=%d/%s", nodes, mode), func(benchmark *testing.B) {
				var updates, adjustments []int64
				benchmark.ReportAllocs()
				for iteration := 0; iteration < benchmark.N; iteration++ {
					benchmark.StopTimer()
					ring := New(100, murmur3.Sum32)
					for index := 0; index < nodes; index++ {
						ring.replicas[fmt.Sprintf("node-%d", index)] = 100
					}
					ring.dirty = true
					if strings.HasPrefix(mode, "full-") {
						fullRebuildCalc(ring, ring.replicas)
						fullRebuildAdjust(ring)
					} else {
						ring.Adjust()
					}
					oldNodes := make([]string, nodes/20)
					newNodes := make([]string, len(oldNodes))
					for index := range oldNodes {
						oldNodes[index] = fmt.Sprintf("node-%d", index)
						newNodes[index] = fmt.Sprintf("replacement-%d", index)
					}
					benchmark.StartTimer()
					for index := range oldNodes {
						for _, adding := range []bool{false, true} {
							started := time.Now()
							if adding {
								membershipUpdate(ring, mode, newNodes[index], 100)
							} else {
								membershipUpdate(ring, mode, oldNodes[index], 0)
							}
							updates = append(updates, time.Since(started).Nanoseconds())
							if mode == "full-eager" {
								membershipAdjust(ring, mode, &adjustments)
							}
						}
						if strings.HasSuffix(mode, "every100") && (index+1)%50 == 0 {
							membershipAdjust(ring, mode, &adjustments)
						}
					}
					if strings.HasSuffix(mode, "every100") {
						membershipAdjust(ring, mode, &adjustments)
					}
				}
				benchmark.StopTimer()
				reportDurations(benchmark, "update", updates)
				reportDurations(benchmark, "adjust", adjustments)
			})
		}
	}
}

func reportDurations(benchmark *testing.B, name string, durations []int64) {
	if len(durations) == 0 {
		return
	}
	sort.Slice(durations, func(left, right int) bool { return durations[left] < durations[right] })
	var total int64
	for _, duration := range durations {
		total += duration
	}
	benchmark.ReportMetric(float64(len(durations))/float64(benchmark.N), name+"-count/op")
	benchmark.ReportMetric(float64(total)/float64(benchmark.N), name+"-total-ns/op")
	benchmark.ReportMetric(float64(total)/float64(len(durations)), name+"-mean-ns")
	benchmark.ReportMetric(float64(durations[(len(durations)*99+99)/100-1]), name+"-p99-ns")
	benchmark.ReportMetric(float64(durations[len(durations)-1]), name+"-max-ns")
}

func BenchmarkMembershipAdjust(benchmark *testing.B) {
	for _, weight := range []int{20, 100, 500} {
		for _, operation := range []string{"calc", "adjust", "clean-adjust"} {
			benchmark.Run(fmt.Sprintf("nodes=2440/weight=%d/%s", weight, operation), func(benchmark *testing.B) {
				ring := benchmarkRing(2440, weight)
				benchmark.ReportAllocs()
				benchmark.ResetTimer()
				for iteration := 0; iteration < benchmark.N; iteration++ {
					if operation == "calc" {
						ring.calc(ring.replicas)
					} else if operation == "adjust" {
						benchmark.StopTimer()
						ring.dirty = true
						benchmark.StartTimer()
						ring.Adjust()
					} else {
						ring.Adjust()
					}
				}
			})
		}
	}
}

func BenchmarkMembershipMutation(benchmark *testing.B) {
	for _, weight := range []int{20, 100, 500} {
		for _, mode := range []string{"full-basic", "incremental-basic"} {
			benchmark.Run(fmt.Sprintf("nodes=2440/weight=%d/%s", weight, mode), func(benchmark *testing.B) {
				ring := benchmarkRing(2440, weight)
				var durations []int64
				benchmark.ReportAllocs()
				benchmark.ResetTimer()
				for iteration := 0; iteration < benchmark.N; iteration++ {
					started := time.Now()
					membershipUpdate(ring, mode, "node-0", 0)
					durations = append(durations, time.Since(started).Nanoseconds())
					started = time.Now()
					membershipUpdate(ring, mode, "node-0", weight)
					durations = append(durations, time.Since(started).Nanoseconds())
				}
				benchmark.StopTimer()
				reportDurations(benchmark, "update", durations)
			})
		}
	}
}

func BenchmarkMembershipLookup(benchmark *testing.B) {
	for _, mode := range []string{"full", "incremental"} {
		for _, operation := range []string{"Get", "Get2"} {
			benchmark.Run(mode+"/"+operation, func(benchmark *testing.B) {
				ring := benchmarkRing(2440, 100)
				queries := make([]string, 10000)
				for index := range queries {
					queries[index] = strconv.Itoa(index)
				}
				benchmark.ReportAllocs()
				benchmark.ResetTimer()
				for iteration := 0; iteration < benchmark.N; iteration++ {
					key := queries[iteration%len(queries)]
					if operation == "Get" {
						ring.Get(key)
					} else if mode == "incremental" {
						ring.Get2(key)
					} else {
						fullRebuildGet2(ring, key)
					}
				}
			})
		}
	}
}

func fullRebuildGet2(ring *Map, key string) (string, string) {
	if ring.IsEmpty() {
		return "", ""
	}
	index := ring.lookup(key)
	first := ring.hashMap[ring.keys[index]]
	second := ""
	if len(ring.replicas) > 1 {
		for ring.hashMap[ring.keys[index]] == first {
			index++
			if index == len(ring.keys) {
				index = 0
			}
		}
		second = ring.hashMap[ring.keys[index]]
	}
	return first, second
}

func TestDeferredRoutingMovement(test *testing.T) {
	for _, weighted := range []bool{false, true} {
		ring := benchmarkRing(600, 100)
		if weighted {
			for index := 0; index < 600; index++ {
				ring.replicas[fmt.Sprintf("node-%d", index)] = 50 + index%5*50
			}
			ring.calc(ring.replicas)
			ring.dirty = true
			ring.Adjust()
		}
		const samples = 100000
		before := make([]string, samples)
		basic := make([]string, samples)
		for index := range before {
			before[index] = ring.Get(strconv.Itoa(index))
		}
		ring.Add("new-node")
		var membershipMoved, balanceMoved int
		for index := range basic {
			basic[index] = ring.Get(strconv.Itoa(index))
			if basic[index] != before[index] {
				membershipMoved++
				if basic[index] != "new-node" {
					test.Fatal("adding a node moved a key between existing nodes")
				}
			}
		}
		ring.Adjust()
		for index := range basic {
			if ring.Get(strconv.Itoa(index)) != basic[index] {
				balanceMoved++
			}
		}
		test.Logf("weighted=%t membership-moved=%.2f%% balance-moved=%.2f%%", weighted,
			float64(membershipMoved)*100/samples, float64(balanceMoved)*100/samples)
	}
}
