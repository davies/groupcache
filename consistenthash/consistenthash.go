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
	replicas map[string]int
}

func New(replicas int, fn Hash) *Map {
	m := &Map{
		replica:  replicas,
		hash:     fn,
		replicas: make(map[string]int),
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

// Adds some keys to the hash.
func (m *Map) Add(keys ...string) {
	for _, key := range keys {
		m.AddWithWeight(key, m.replica)
	}
}

// Adds a key with different replica to the hash.
func (m *Map) AddWithWeight(key string, replica int) {
	if replica < 1 {
		panic("replica should be positive")
	}
	old := m.replicas[key]
	if old != replica {
		m.hashMap = nil
		m.replicas[key] = replica
	}
}

// Remove a key from hash
func (m *Map) Remove(key string) {
	m.hashMap = nil
	delete(m.replicas, key)
}

func (m *Map) calc(replicas map[string]int) {
	m.keys = nil
	m.hashMap = make(map[int]string)
	for key, r := range replicas {
		for i := 0; i < r; i++ {
			hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = key
		}
	}
	sort.Ints(m.keys)
}

// init the replica for keys
func (m *Map) init(scale float64) {
	if m.hashMap != nil {
		return
	}
	m.calc(m.replicas)
	if len(m.replicas) <= 1 || m.replica < 10 {
		return
	}
	stat := make(map[string]int)
	stat[m.hashMap[m.keys[0]]] = m.keys[0] + int(1<<32) - m.keys[len(m.keys)-1]
	for i, h := range m.keys[1:] {
		stat[m.hashMap[h]] += h - m.keys[i]
	}
	var replicas int
	for _, r := range m.replicas {
		replicas += r
	}
	reps := make(map[string]int)
	for k, v := range stat {
		actual := float64(v) / float64(1<<32)
		rep := m.replicas[k]
		expect := float64(rep) / float64(replicas)
		adjust := int(float64(rep) * (expect - actual) / float64(expect) * scale)
		reps[k] = rep + adjust
	}
	m.calc(reps)
}

// Gets the closest item in the hash to the provided key.
func (m *Map) Get(key string) string {
	if m.IsEmpty() {
		return ""
	}
	m.init(1)

	hash := int(m.hash([]byte(key)))

	// Binary search for appropriate replica.
	idx := sort.Search(len(m.keys), func(i int) bool { return m.keys[i] >= hash })

	// Means we have cycled back to the first replica.
	if idx == len(m.keys) {
		idx = 0
	}

	return m.hashMap[m.keys[idx]]
}
