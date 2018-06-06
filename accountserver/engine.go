//  Copyright (c) 2016-2017 Rackspace
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

package accountserver

import (
	"container/list"
	"crypto/md5"
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/troubling/hummingbird/common/fs"
)

var (
	// ErrorNoSuchAccount is returned when a requested account doesn't exist.
	ErrorNoSuchAccount = fmt.Errorf("No such account.")
	// ErrorInvalidMetadata is returned for errors that violate the API metadata constraints.
	ErrorInvalidMetadata = fmt.Errorf("Invalid metadata value")
)

// AccountInfo represents the container_info database record - basic information about the container.
type AccountInfo struct {
	Account         string              `json:"account"`
	CreatedAt       string              `json:"created_at"`
	PutTimestamp    string              `json:"put_timestamp"`
	DeleteTimestamp string              `json:"delete_timestamp"`
	StatusChangedAt string              `json:"status_changed_at"`
	ObjectCount     int64               `json:"count"`
	BytesUsed       int64               `json:"bytes_used"`
	ContainerCount  int64               `json:"container_count"`
	Hash            string              `json:"hash"`
	ID              string              `json:"id"`
	RawMetadata     string              `json:"metadata"`
	Metadata        map[string][]string `json:"-"`
	MaxRow          int64               `json:"max_row"`
	invalid         bool
	updated         time.Time
	// This row isn't populated by GetInfo, it only exists for the times this is
	// serialized during replication.
	Point int64 `json:"point"`
}

type PolicyStat struct {
	StoragePolicyIndex int
	ContainerCount     int64
	ObjectCount        int64
	BytesUsed          int64
}

type policyStats struct {
	list    []*PolicyStat
	invalid bool
	updated time.Time
}

// ContainerListingRecord is the struct used for serializing objects in json and xml account listings.
type ContainerListingRecord struct {
	XMLName      xml.Name `xml:"container" json:"-"`
	Name         string   `xml:"name" json:"name"`
	Bytes        int64    `xml:"bytes" json:"bytes"`
	Count        int64    `xml:"count" json:"count"`
	LastModified string   `xml:"last_modified" json:"last_modified"`
}

// SubdirListingRecord is the struct used for serializing subdirs in json and xml account listings.
type SubdirListingRecord struct {
	XMLName xml.Name `xml:"subdir" json:"-"`
	Name2   string   `xml:"name,attr" json:"-"`
	Name    string   `xml:"name" json:"subdir"`
}

// ContainerRecord represents the object's data in-databaee, it is used by replication.
type ContainerRecord struct {
	Rowid              int64  `json:"ROWID"`
	Name               string `json:"name"`
	PutTimestamp       string `json:"put_timestamp"`
	DeleteTimestamp    string `json:"delete_timestamp"`
	ObjectCount        int64  `json:"object_count"`
	BytesUsed          int64  `json:"bytes_used"`
	Deleted            int    `json:"deleted"`
	StoragePolicyIndex int    `json:"storage_policy_index"`
}

// SyncRecord represents a row in the incoming_sync table.  It is used by replication.
type SyncRecord struct {
	SyncPoint int64  `json:"sync_point"`
	RemoteID  string `json:"remote_id"`
}

// Account is the interface implemented by an account
type Account interface {
	// GetInfo returns the AccountInfo struct for the account.
	GetInfo() (*AccountInfo, error)
	// PolicyStats returns the metrics for various policies in use by the account.
	PolicyStats() ([]*PolicyStat, error)
	// IsDeleted returns true if the account has been deleted.
	IsDeleted() (bool, error)
	// Delete deletes the account.
	Delete(timestamp string) (int, error)
	// ListContainers lists the account's container entries.
	ListContainers(limit int, marker string, endMarker string, prefix string, delimiter string, reverse bool) ([]interface{}, error)
	// GetMetadata returns the account's current metadata.
	GetMetadata() (map[string]string, error)
	// UpdateMetadata applies updates to the account's metadata.
	UpdateMetadata(updates map[string][]string) error
	// PutContainer adds a new container to the account.
	PutContainer(name string, putTimestamp string, deleteTimestamp string, objectCount int64, bytesUsed int64, storagePolicyIndex int) error
	// ID returns a unique identifier for the account.
	ID() string
	// Close frees any resources associated with the account.
	Close() error
}

// ReplicableAccount is an account that also implements the replication API.
type ReplicableAccount interface {
	Account
	// MergeItems merges object records into the account, with an optional remoteId.
	MergeItems(records []*ContainerRecord, remoteID string) error
	// ItemsSince returns count object records with a ROWID greater than start.
	ItemsSince(start int64, count int) ([]*ContainerRecord, error)
	// MergeSyncTable updates the account's incoming sync tables with new data.
	MergeSyncTable(records []*SyncRecord) error
	// SyncTable returns the account's current sync table.
	SyncTable() ([]*SyncRecord, error)
	// SyncRemoteData accepts a remote account's current status information and updates local records accordingly.
	SyncRemoteData(maxRow int64, hash, id, createdAt, putTimestamp, deleteTimestamp, metadata string) (*AccountInfo, error)
	// NewID gives the database a new unique identifier, which is used for incoming_sync entries.
	NewID() error
	// OpenDatabaseFile returns a consistent reader for the underlying database file.
	OpenDatabaseFile() (*os.File, func(), error)
	// CleanupTombstones removes any metadata and object tombstones older than reclaimAge seconds.
	CleanupTombstones(reclaimAge int64) error
	// RingHash returns the account's ring hash.
	RingHash() string
}

// AccountEngine is the interface of an object that creates and returns accounts.
type AccountEngine interface {
	// Get returns an Account, given a vars mapping.
	Get(vars map[string]string) (c Account, err error)
	// Return returns a Account to the engine, where it can close or retain them as it sees fit.
	Return(c Account)
	// Create creates a new account.  Returns true if the accoun t was created and a pointer to the account.
	Create(vars map[string]string, putTimestamp string, metadata map[string][]string) (bool, Account, error)
	// Close releases all cached accounts and any other retained resources.
	Close()

	// GetByHash returns a replicable database given its hash.  This will probably move from this interface once we
	// have replicator->replicator communication.
	GetByHash(device, hash, partition string) (c ReplicableAccount, err error)
	// Invalidate removes an account from the cache entirely.  This will probably also move, since it's only used by replication.
	Invalidate(c Account)
}

// My attempts at making this lruEngine reusable have not been successful, so for now it's sqlite-specific and not exported.
type lruEngine struct {
	deviceRoot     string
	hashPathPrefix string
	hashPathSuffix string
	maxSize        int
	cache          map[string]*lruEntry
	used           *list.List
	m              sync.Mutex
}

type lruEntry struct {
	c     Account
	inUse int
	elem  *list.Element
}

func (l *lruEngine) add(c Account) {
	if len(l.cache) >= l.maxSize {
		for elem := l.used.Front(); elem != nil; elem = elem.Next() {
			if entry, ok := elem.Value.(*lruEntry); ok && entry.inUse == 0 {
				l.used.Remove(elem)
				delete(l.cache, entry.c.ID())
				entry.c.Close()
				break
			}
		}
	}
	holder := &lruEntry{c: c, inUse: 1}
	holder.elem = l.used.PushBack(holder)
	l.cache[c.ID()] = holder
}

func (l *lruEngine) accountLocation(vars map[string]string) string {
	h := md5.New()
	fmt.Fprintf(h, "%s/%s%s", l.hashPathPrefix, vars["account"], l.hashPathSuffix)
	hexHash := fmt.Sprintf("%032x", h.Sum(nil))
	suffix := hexHash[29:32]
	return filepath.Join(l.deviceRoot, vars["device"], "accounts", vars["partition"], suffix, hexHash, hexHash+".db")
}

func (l *lruEngine) getbypath(accountFile string) (c Account, err error) {
	if !fs.Exists(accountFile) {
		return nil, ErrorNoSuchAccount
	}
	ringHash := filepath.Base(filepath.Dir(accountFile))
	l.m.Lock()
	defer l.m.Unlock()
	if e := l.cache[ringHash]; e != nil {
		e.inUse++
		l.used.MoveToBack(e.elem)
		return e.c, nil
	}
	if c, err = sqliteOpenAccount(accountFile); err != nil {
		return nil, err
	}
	l.add(c)
	return c, nil
}

// Get returns a database given the incoming vars.
func (l *lruEngine) Get(vars map[string]string) (c Account, err error) {
	return l.getbypath(l.accountLocation(vars))
}

// Create creates a new account.
func (l *lruEngine) Create(vars map[string]string, putTimestamp string, metadata map[string][]string) (bool, Account, error) {
	accountFile := l.accountLocation(vars)
	created := false
	c, err := l.Get(vars)
	if err != nil {
		created = true
		err = sqliteCreateAccount(accountFile, vars["account"], putTimestamp, metadata)
		if err == nil {
			c, err = l.Get(vars)
		}
	} else {
		created, err = sqliteCreateExistingAccount(c, putTimestamp, metadata)
	}
	return created, c, err
}

// Return returns a database object to the engine.
func (l *lruEngine) Return(c Account) {
	l.m.Lock()
	if e := l.cache[c.ID()]; e != nil && e.c == c {
		e.inUse--
	} else {
		l.add(c)
	}
	l.m.Unlock()
}

// GetByHash returns a database given its device and ring hash.
func (l *lruEngine) GetByHash(device, hash, partition string) (ReplicableAccount, error) {
	accountFile := filepath.Join(l.deviceRoot, device, "accounts", partition, hash[29:32], hash, hash+".db")
	c, err := l.getbypath(accountFile)
	if err != nil {
		return nil, err
	}
	rc, ok := c.(ReplicableAccount)
	if !ok {
		return nil, fmt.Errorf("Account does not support replication.")
	}
	return rc, nil
}

// Invalidate removes any cached backend connections to the database.
func (l *lruEngine) Invalidate(c Account) {
	defer c.Close()
	l.m.Lock()
	if e := l.cache[c.ID()]; e != nil && e.c == c {
		l.used.Remove(e.elem)
		delete(l.cache, c.ID())
	}
	l.m.Unlock()
}

// Close shuts down any backend account database connections and clears the caches.
func (l *lruEngine) Close() {
	for k, v := range l.cache {
		v.c.Close()
		delete(l.cache, k)
	}
	l.used = l.used.Init()
}

func newLRUEngine(deviceRoot, hashPathPrefix, hashPathSuffix string, accountCount int) *lruEngine {
	return &lruEngine{
		deviceRoot:     deviceRoot,
		hashPathPrefix: hashPathPrefix,
		hashPathSuffix: hashPathSuffix,
		maxSize:        accountCount,
		cache:          make(map[string]*lruEntry),
		used:           list.New(),
	}
}
