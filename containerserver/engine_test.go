//  Copyright (c) 2016 Rackspace
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
//  implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package containerserver

import (
	"container/list"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type hashDatabase struct {
	fakeDatabase
	hash string
}

func (h hashDatabase) ID() string {
	return h.hash
}

func TestAddRemoves(t *testing.T) {
	l := &lruEngine{
		maxSize: 3,
		cache:   make(map[string]*lruEntry),
		used:    list.New(),
	}
	dbs := make([]hashDatabase, 5)
	for i, db := range dbs {
		db.hash = fmt.Sprintf("%d", i)
		l.add(db)
		l.Return(db)
	}
	require.Equal(t, 3, len(l.cache))
	require.Equal(t, 3, l.used.Len())
}

func TestClose(t *testing.T) {
	l := &lruEngine{
		maxSize: 3,
		cache:   make(map[string]*lruEntry),
		used:    list.New(),
	}
	dbs := make([]hashDatabase, 5)
	for i, db := range dbs {
		db.hash = fmt.Sprintf("%d", i)
		l.add(db)
		l.Return(db)
	}
	l.Close()
	require.Equal(t, 0, len(l.cache))
	require.Equal(t, 0, l.used.Len())
}

func TestAddOnReturn(t *testing.T) {
	l := &lruEngine{
		maxSize: 3,
		cache:   make(map[string]*lruEntry),
		used:    list.New(),
	}
	dbs := make([]hashDatabase, 5)
	for i, db := range dbs {
		db.hash = fmt.Sprintf("%d", i)
		l.Return(db)
	}
	l.Close()
	require.Equal(t, 0, len(l.cache))
	require.Equal(t, 0, l.used.Len())
}

func TestOpenCount(t *testing.T) {
	l := &lruEngine{
		maxSize: 3,
		cache:   make(map[string]*lruEntry),
		used:    list.New(),
	}
	dbs := make([]hashDatabase, 5)
	for i, db := range dbs {
		db.hash = fmt.Sprintf("%d", i)
		l.add(db)
	}
	require.Equal(t, 5, l.OpenCount())

	for i, db := range dbs {
		db.hash = fmt.Sprintf("%d", i)
		l.Return(db)
	}
	require.Equal(t, 0, l.OpenCount())
}
