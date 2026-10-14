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

// Package consistenthash provides an implementation of a ring hash.
package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

type Hash func(data []byte) uint32

type Map struct {
	hash     Hash
	replica  int
	keys     []int // Sorted
	hashMap  map[int]string
	hashs    map[string][]int
	replicas map[string]int
	owners   map[int]map[string]int
	dirty    bool
}

func New(replicas int, fn Hash) *Map {
	m := &Map{
		replica:  replicas,
		hash:     fn,
		replicas: make(map[string]int),
		hashs:    make(map[string][]int),
		hashMap:  make(map[int]string),
		owners:   make(map[int]map[string]int),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// Returns true if there are no items available.
func (m *Map) IsEmpty() bool {
	return len(m.replicas) == 0
}

// Add adds keys and immediately updates routing without balancing the ring.
// Call Adjust after a batch of changes to balance the distribution.
func (m *Map) Add(keys ...string) {
	for _, key := range keys {
		m.AddWithWeight(key, m.replica)
	}
}

// AddWithWeight immediately updates a key's weight without balancing the ring.
func (m *Map) AddWithWeight(key string, replica int) {
	if replica < 1 {
		panic("replica should be positive")
	}
	old := m.replicas[key]
	if old != replica {
		m.replicas[key] = replica
		m.updatePoints(key, replica)
		m.dirty = true
	}
}

// Remove immediately removes a key from routing without balancing the ring.
func (m *Map) Remove(key string) {
	if _, ok := m.replicas[key]; ok {
		m.updatePoints(key, 0)
		delete(m.replicas, key)
		delete(m.hashs, key)
		m.dirty = true
	}
}

func (m *Map) addPoint(hash int, key string) {
	owner, exists := m.hashMap[hash]
	if !exists {
		m.hashMap[hash] = key
		return
	}
	owners := m.owners[hash]
	if owners == nil {
		owners = map[string]int{owner: 1}
		m.owners[hash] = owners
	}
	owners[key]++
	if key < owner {
		m.hashMap[hash] = key
	}
}

func (m *Map) removePoint(hash int, key string) {
	owners := m.owners[hash]
	if owners == nil {
		delete(m.hashMap, hash)
		return
	}
	owners[key]--
	if owners[key] == 0 {
		delete(owners, key)
	}
	var owner string
	var count int
	for candidate, replicas := range owners {
		if count == 0 || candidate < owner {
			owner = candidate
		}
		count += replicas
	}
	if count <= 1 {
		delete(m.owners, hash)
	}
	if count == 0 {
		delete(m.hashMap, hash)
	} else {
		m.hashMap[hash] = owner
	}
}

func (m *Map) updatePoints(key string, count int) {
	hashes := m.hashs[key]
	previous := len(hashes)
	if count == previous {
		return
	}
	for index := len(hashes); index < count; index++ {
		hashes = append(hashes, int(m.hash([]byte(strconv.Itoa(index)+key))))
	}
	var added, removed []int
	if count > previous {
		added = append([]int(nil), hashes[previous:count]...)
		for _, hash := range added {
			m.addPoint(hash, key)
		}
	} else {
		removed = append([]int(nil), hashes[count:previous]...)
		for _, hash := range removed {
			m.removePoint(hash, key)
		}
	}
	m.hashs[key] = hashes[:count]
	m.updateKeys(added, removed)
}

func (m *Map) updateKeys(added, removed []int) {
	if len(removed) > 0 {
		sort.Ints(removed)
		kept, consumed := 0, 0
		for _, hash := range m.keys {
			if consumed < len(removed) && hash == removed[consumed] {
				consumed++
			} else {
				m.keys[kept] = hash
				kept++
			}
		}
		m.keys = m.keys[:kept]
	}
	if len(added) > 0 {
		sort.Ints(added)
		remaining := len(m.keys) - 1
		m.keys = append(m.keys, added...)
		position := len(m.keys) - 1
		for incoming := len(added) - 1; incoming >= 0; position-- {
			if remaining >= 0 && m.keys[remaining] > added[incoming] {
				m.keys[position] = m.keys[remaining]
				remaining--
			} else {
				m.keys[position] = added[incoming]
				incoming--
			}
		}
	}
}

func (m *Map) updateRing(replicas map[string]int) {
	var additions, removals int
	for key, count := range replicas {
		previous := len(m.hashs[key])
		if count > previous {
			additions += count - previous
		} else {
			removals += previous - count
		}
	}
	added := make([]int, 0, additions)
	removed := make([]int, 0, removals)
	for key, count := range replicas {
		hashes := m.hashs[key]
		previous := len(hashes)
		if count == previous {
			continue
		}
		if count > previous {
			for index := previous; index < count; index++ {
				hash := int(m.hash([]byte(strconv.Itoa(index) + key)))
				hashes = append(hashes, hash)
				added = append(added, hash)
				m.addPoint(hash, key)
			}
		} else {
			removed = append(removed, hashes[count:previous]...)
			for _, hash := range hashes[count:previous] {
				m.removePoint(hash, key)
			}
		}
		m.hashs[key] = hashes[:count]
	}
	m.updateKeys(added, removed)
}

// Adjust balances the ring after membership changes and reports whether it rebuilt
// the ring. Callers must exclude concurrent updates and lookups, as with Add and Remove.
func (m *Map) Adjust() bool {
	if m.dirty {
		m.adjust(5, 0.75)
		m.dirty = false
		return true
	}
	return false
}

func (m *Map) calc(replicas map[string]int) {
	clear(m.hashMap)
	clear(m.owners)
	for key, hashes := range m.hashs {
		if _, exists := replicas[key]; !exists {
			m.hashs[key] = hashes[:0]
		}
	}
	m.keys = m.keys[:0]
	for key, count := range replicas {
		if count < 0 {
			count = 0
		}
		hashes := m.hashs[key]
		for index := len(hashes); index < count; index++ {
			hashes = append(hashes, int(m.hash([]byte(strconv.Itoa(index)+key))))
		}
		hashes = hashes[:count]
		m.hashs[key] = hashes
		m.keys = append(m.keys, hashes...)
		for _, hash := range hashes {
			m.addPoint(hash, key)
		}
	}
	sort.Ints(m.keys)
}

// adjust the replica for keys
func (m *Map) adjust(tries int, scale float64) {
	m.calc(m.replicas)
	if len(m.replicas) <= 1 || m.replica < 10 {
		return
	}
	var replicas int
	reps := make(map[string]int, len(m.replicas))
	for k, r := range m.replicas {
		reps[k] = r
		replicas += r
	}

	for t := 0; t < tries; t++ {
		stat := make(map[string]int, len(m.replicas))
		stat[m.hashMap[m.keys[0]]] = m.keys[0] + int(1<<32) - m.keys[len(m.keys)-1]
		for i, h := range m.keys[1:] {
			stat[m.hashMap[h]] += h - m.keys[i]
		}
		var changed bool
		for k, v := range stat {
			actual := float64(v) / float64(1<<32)
			rep := reps[k]
			expect := float64(m.replicas[k]) / float64(replicas)
			adjust := int(float64(rep) * (expect - actual) / float64(expect) * scale)
			if adjust > 1 || adjust <= -1 {
				reps[k] += adjust
				if reps[k] < 1 {
					reps[k] = 1
				}
				changed = true
			}
		}
		if !changed {
			return
		}
		m.updateRing(reps)
	}
}

// Gets the closest item in the hash to the provided key.
func (m *Map) Get(key string) string {
	if m.IsEmpty() {
		return ""
	}
	idx := m.lookup(key)
	return m.hashMap[m.keys[idx]]
}

func (m *Map) lookup(key string) int {
	hash := int(m.hash([]byte(key)))

	// Binary search for appropriate replica.
	idx := sort.Search(len(m.keys), func(i int) bool { return m.keys[i] >= hash })

	// Means we have cycled back to the first replica.
	if idx == len(m.keys) {
		idx = 0
	}
	return idx
}

// Gets the two items in the hash to the provided key.
func (m *Map) Get2(key string) (string, string) {
	if m.IsEmpty() {
		return "", ""
	}
	idx := m.lookup(key)
	first := m.hashMap[m.keys[idx]]
	idx2 := idx
	second := ""
	if len(m.replicas) > 1 {
		for {
			hash := m.keys[idx2]
			if owner := m.hashMap[hash]; owner != first {
				return first, owner
			}
			for owner := range m.owners[hash] {
				if owner != first && (second == "" || owner < second) {
					second = owner
				}
			}
			if second != "" {
				return first, second
			}
			idx2++
			if idx2 == len(m.keys) {
				idx2 = 0
			}
			if idx2 == idx {
				break
			}
		}
	}
	return first, second
}

func (m *Map) Clone() *Map {
	clone := &Map{
		hash:     m.hash,
		replica:  m.replica,
		keys:     make([]int, len(m.keys)),
		hashMap:  make(map[int]string, len(m.hashMap)),
		hashs:    make(map[string][]int, len(m.hashs)),
		replicas: make(map[string]int, len(m.replicas)),
		owners:   make(map[int]map[string]int, len(m.owners)),
		dirty:    m.dirty,
	}
	copy(clone.keys, m.keys)
	for k, v := range m.hashMap {
		clone.hashMap[k] = v
	}
	for k, v := range m.hashs {
		clone.hashs[k] = make([]int, len(v))
		copy(clone.hashs[k], v)
	}
	for k, v := range m.replicas {
		clone.replicas[k] = v
	}
	for k, v := range m.owners {
		v2 := make(map[string]int, len(v))
		for owner, count := range v {
			v2[owner] = count
		}
		clone.owners[k] = v2
	}
	return clone
}
