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
	// ErrorNoSuchContainer is returned when a requested container doesn't exist.
	ErrorNoSuchContainer = fmt.Errorf("No such container.")
	// ErrorInvalidMetadata is returned for errors that violate the API metadata constraints.
	ErrorInvalidMetadata = fmt.Errorf("Invalid metadata value")
	// ErrorPolicyConflict is returned when an operation conflicts with the container's existing policy.
	ErrorPolicyConflict = fmt.Errorf("Policy conflicts with existing value")
)

// ContainerInfo represents the container_info database record - basic information about the container.
type ContainerInfo struct {
	Account                 string              `json:"account"`
	Container               string              `json:"container"`
	CreatedAt               string              `json:"created_at"`
	PutTimestamp            string              `json:"put_timestamp"`
	DeleteTimestamp         string              `json:"delete_timestamp"`
	StatusChangedAt         string              `json:"status_changed_at"`
	ObjectCount             int64               `json:"count"`
	BytesUsed               int64               `json:"bytes_used"`
	ReportedPutTimestamp    string              `json:"-"`
	ReportedDeleteTimestamp string              `json:"-"`
	ReportedObjectCount     int64               `json:"-"`
	ReportedBytesUsed       int64               `json:"-"`
	Hash                    string              `json:"hash"`
	ID                      string              `json:"id"`
	XContainerSyncPoint1    string              `json:"-"`
	XContainerSyncPoint2    string              `json:"-"`
	StoragePolicyIndex      int                 `json:"storage_policy_index"`
	RawMetadata             string              `json:"metadata"`
	Metadata                map[string][]string `json:"-"`
	MaxRow                  int64               `json:"max_row"`
	invalid                 bool
	updated                 time.Time
	// This row isn't populated by GetInfo, it only exists for the times this is
	// serialized during replication.
	Point int64 `json:"point"`
}

// ObjectListingRecord is the struct used for serializing objects in json and xml container listings.
type ObjectListingRecord struct {
	XMLName      xml.Name `xml:"object" json:"-"`
	Name         string   `xml:"name" json:"name"`
	LastModified string   `xml:"last_modified" json:"last_modified"`
	Size         int64    `xml:"bytes" json:"bytes"`
	ContentType  string   `xml:"content_type" json:"content_type"`
	ETag         string   `xml:"hash" json:"hash"`
}

// SubdirListingRecord is the struct used for serializing subdirs in json and xml container listings.
type SubdirListingRecord struct {
	XMLName xml.Name `xml:"subdir" json:"-"`
	Name2   string   `xml:"name,attr" json:"-"`
	Name    string   `xml:"name" json:"subdir"`
}

// ObjectRecord represents the object's data in-databaee, it is used by replication.
type ObjectRecord struct {
	Rowid              int64  `json:"ROWID"`
	Name               string `json:"name"`
	CreatedAt          string `json:"created_at"`
	Size               int64  `json:"size"`
	ContentType        string `json:"content_type"`
	ETag               string `json:"etag"`
	Deleted            int    `json:"deleted"`
	StoragePolicyIndex int    `json:"storage_policy_index"`
}

// SyncRecord represents a row in the incoming_sync table.  It is used by replication.
type SyncRecord struct {
	SyncPoint int64  `json:"sync_point"`
	RemoteID  string `json:"remote_id"`
}

// Container is the interface implemented by a container.
type Container interface {
	// GetInfo returns the ContainerInfo struct for the container.
	GetInfo() (*ContainerInfo, error)
	// IsDeleted returns true if the container has been deleted.
	IsDeleted() (bool, error)
	// Delete deletes the container.
	Delete(timestamp string) error
	// ListObjects lists the container's object entries.
	ListObjects(limit int, marker string, endMarker string, prefix string, delimiter string, path *string, reverse bool, storagePolicyIndex int) ([]interface{}, error)
	// GetMetadata returns the container's current metadata.
	GetMetadata() (map[string]string, error)
	// UpdateMetadata applies updates to the container's metadata.
	UpdateMetadata(updates map[string][]string, timestamp string) error
	// PutObject adds a new object to the container.
	PutObject(name string, timestamp string, size int64, contentType string, etag string, storagePolicyIndex int) error
	// DeleteObject deletes an object from the container.
	DeleteObject(name string, timestamp string, storagePolicyIndex int) error
	// ID returns a unique identifier for the container.
	ID() string
	// Close frees any resources associated with the container.
	Close() error
}

// ReplicableContainer is a container that also implements the replication API.
type ReplicableContainer interface {
	Container
	// MergeItems merges object records into the container, with an optional remoteId.
	MergeItems(records []*ObjectRecord, remoteID string) error
	// ItemsSince returns count object records with a ROWID greater than start.
	ItemsSince(start int64, count int) ([]*ObjectRecord, error)
	// MergeSyncTable updates the container's incoming sync tables with new data.
	MergeSyncTable(records []*SyncRecord) error
	// SyncTable returns the container's current sync table.
	SyncTable() ([]*SyncRecord, error)
	// SyncRemoteData accepts a remote container's current status information and updates local records accordingly.
	SyncRemoteData(maxRow int64, hash, id, createdAt, putTimestamp, deleteTimestamp, metadata string) (*ContainerInfo, error)
	// NewID gives the database a new unique identifier, which is used for incoming_sync entries.
	NewID() error
	// OpenDatabaseFile returns a consistent reader for the underlying database file.
	OpenDatabaseFile() (*os.File, func(), error)
	// CleanupTombstones removes any metadata and object tombstones older than reclaimAge seconds.
	CleanupTombstones(reclaimAge int64) error
	// CheckSyncLinks makes sure container sync symlinks are correct for the database.
	CheckSyncLink() error
	// RingHash returns the container's ring hash.
	RingHash() string
}

// ContainerEngine is the interface of an object that creates and returns containers.
type ContainerEngine interface {
	// GET returns a container, given a vars mapping.
	Get(vars map[string]string) (c Container, err error)
	// Return returns a Container to the engine, where it can close or retain them as it sees fit.
	Return(c Container)
	// Create creates a new container.  Returns true if the container was created and a pointer to the container.
	Create(vars map[string]string, putTimestamp string, metadata map[string][]string, policyIndex, defaultPolicyIndex int) (bool, Container, error)
	// Close releases all cached containers and any other retained resources.
	Close()

	// GetByHash returns a replicable database given its hash.  This will probably move from this interface once we
	// have replicator->replicator communication.
	GetByHash(device, hash, partition string) (c ReplicableContainer, err error)
	// Invalidate removes a container from the cache entirely.  This will probably also move, since it's only used by replication.
	Invalidate(c Container)

	// Number of containers that have been Get()ted but not Return()ed
	OpenCount() (count int)
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
	c     Container
	inUse int
	elem  *list.Element
}

func (l *lruEngine) add(c Container) {
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

func (l *lruEngine) OpenCount() int {
	count := 0
	l.m.Lock()
	for elem := l.used.Front(); elem != nil; elem = elem.Next() {
		if entry, ok := elem.Value.(*lruEntry); ok && entry.inUse == 1 {
			count++
		}
	}
	defer l.m.Unlock()
	return count
}

func (l *lruEngine) containerLocation(vars map[string]string) string {
	h := md5.New()
	fmt.Fprintf(h, "%s/%s/%s%s", l.hashPathPrefix, vars["account"], vars["container"], l.hashPathSuffix)
	hexHash := fmt.Sprintf("%032x", h.Sum(nil))
	suffix := hexHash[29:32]
	return filepath.Join(l.deviceRoot, vars["device"], "containers", vars["partition"], suffix, hexHash, hexHash+".db")
}

func (l *lruEngine) getbypath(containerFile string) (c Container, err error) {
	if !fs.Exists(containerFile) {
		return nil, ErrorNoSuchContainer
	}
	ringHash := filepath.Base(filepath.Dir(containerFile))
	l.m.Lock()
	defer l.m.Unlock()
	if e := l.cache[ringHash]; e != nil {
		e.inUse++
		l.used.MoveToBack(e.elem)
		return e.c, nil
	}
	if c, err = sqliteOpenContainer(containerFile); err != nil {
		return nil, err
	}
	l.add(c)
	return c, nil
}

// Get returns a database given the incoming vars.
func (l *lruEngine) Get(vars map[string]string) (c Container, err error) {
	return l.getbypath(l.containerLocation(vars))
}

// Create creates a new container.
func (l *lruEngine) Create(vars map[string]string, putTimestamp string, metadata map[string][]string, policyIndex, defaultPolicyIndex int) (bool, Container, error) {
	containerFile := l.containerLocation(vars)
	created := false
	c, err := l.Get(vars)
	if err != nil {
		created = true
		if policyIndex < 0 {
			policyIndex = defaultPolicyIndex
		}
		err = sqliteCreateContainer(containerFile, vars["account"], vars["container"], putTimestamp, metadata, policyIndex)
		if err == nil {
			c, err = l.Get(vars)
		}
	} else {
		created, err = sqliteCreateExistingContainer(c, putTimestamp, metadata, policyIndex, defaultPolicyIndex)
	}
	return created, c, err
}

// Return returns a database object to the engine.
func (l *lruEngine) Return(c Container) {
	l.m.Lock()
	if e := l.cache[c.ID()]; e != nil && e.c == c {
		e.inUse--
	} else {
		l.add(c)
	}
	l.m.Unlock()
}

// GetByHash returns a database given its device and ring hash.
func (l *lruEngine) GetByHash(device, hash, partition string) (ReplicableContainer, error) {
	containerFile := filepath.Join(l.deviceRoot, device, "containers", partition, hash[29:32], hash, hash+".db")
	c, err := l.getbypath(containerFile)
	if err != nil {
		return nil, err
	}
	rc, ok := c.(ReplicableContainer)
	if !ok {
		return nil, fmt.Errorf("Container does not support replication.")
	}
	return rc, nil
}

// Invalidate removes any cached backend connections to the database.
func (l *lruEngine) Invalidate(c Container) {
	defer c.Close()
	l.m.Lock()
	if e := l.cache[c.ID()]; e != nil && e.c == c {
		l.used.Remove(e.elem)
		delete(l.cache, c.ID())
	}
	l.m.Unlock()
}

// Close shuts down any backend container database connections and clears the caches.
func (l *lruEngine) Close() {
	for k, v := range l.cache {
		v.c.Close()
		delete(l.cache, k)
	}
	l.used = l.used.Init()
}

func newLRUEngine(deviceRoot, hashPathPrefix, hashPathSuffix string, containerCount int) *lruEngine {
	return &lruEngine{
		deviceRoot:     deviceRoot,
		hashPathPrefix: hashPathPrefix,
		hashPathSuffix: hashPathSuffix,
		maxSize:        containerCount,
		cache:          make(map[string]*lruEntry),
		used:           list.New(),
	}
}
