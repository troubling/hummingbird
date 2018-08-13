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
	"crypto/md5"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/fs"
	"github.com/troubling/hummingbird/common/pickle"
)

const (
	maxQueryArgs       = 990
	maxOpenConns       = 2
	maxIdleConns       = 2
	pendingCap         = 131072
	maxMetaCount       = 90
	maxMetaOverallSize = 4096
)

var infoCacheTimeout = time.Second * 10

func chexor(old, name, timestamp string) string {
	oldDigest, err := hex.DecodeString(old)
	if err != nil {
		panic(fmt.Sprintf("Error decoding hex: %v", err))
	}
	h := md5.New()
	if _, err := io.WriteString(h, name+"-"+timestamp); err != nil {
		panic("THIS SHOULD NEVER HAPPEN")
	}
	digest := h.Sum(nil)
	for i := range digest {
		digest[i] ^= oldDigest[i]
	}
	return hex.EncodeToString(digest)
}

func init() {
	// register our sql driver with user-defined chexor function
	sql.Register("sqlite3_hummingbird",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				if err := conn.RegisterFunc("chexor", chexor, true); err != nil {
					return err
				}
				if _, err := conn.Exec(pragmaScript, nil); err != nil {
					return err
				}
				if _, err := conn.Exec(`CREATE TEMPORARY VIEW IF NOT EXISTS maxrowid (max) AS
				  						SELECT IFNULL(MAX(seq), -1) FROM sqlite_sequence WHERE name='object'`, nil); err != nil {
					return err
				}
				return nil
			},
		},
	)
}

// SqliteContainer wraps a connection to the underlying database and provides all of the operations on that database.
type sqliteContainer struct {
	connectLock sync.Mutex
	*sql.DB
	containerFile       string
	hasDeletedNameIndex bool
	infoCache           atomic.Value
	ringhash            string
}

var _ Container = &sqliteContainer{}

func (db *sqliteContainer) connect() error {
	db.connectLock.Lock()
	defer db.connectLock.Unlock()
	if db.DB != nil {
		return nil
	}
	dbConn, err := sql.Open("sqlite3_hummingbird", "file:"+db.containerFile+"?psow=1&_txlock=immediate&mode=rw")
	if err != nil {
		if common.IsCorruptDBError(err) {
			return fmt.Errorf("Failed to open: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
		}
		return fmt.Errorf("Failed to open: %v", err)
	}
	dbConn.SetMaxOpenConns(maxOpenConns)
	dbConn.SetMaxIdleConns(maxIdleConns)
	hasDeletedNameIndex, err := schemaMigrate(dbConn)
	if err != nil {
		db.closeAlreadyLocked()
		if common.IsCorruptDBError(err) {
			return fmt.Errorf("Error migrating database: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
		}
		return fmt.Errorf("Error migrating database: %v", err)
	}
	db.hasDeletedNameIndex = hasDeletedNameIndex
	db.DB = dbConn
	return nil
}

// GetInfo returns the container's information as a ContainerInfo struct.
func (db *sqliteContainer) GetInfo() (*ContainerInfo, error) {
	if err := db.connect(); err != nil {
		return nil, err
	}
	if err := db.flush(); err != nil {
		return nil, err
	}
	if _, err := db.Exec("DELETE FROM object WHERE expires < ?", time.Now().Unix()); err != nil {
		return nil, err
	}
	if info, ok := db.infoCache.Load().(*ContainerInfo); ok && !info.invalid && time.Since(info.updated) < infoCacheTimeout {
		return info, nil
	}
	info := &ContainerInfo{updated: time.Now()}
	row := db.QueryRow(`SELECT cs.account, cs.container, cs.created_at, cs.put_timestamp,
							cs.delete_timestamp, cs.status_changed_at,
							cs.object_count, cs.bytes_used,
							cs.reported_put_timestamp, cs.reported_delete_timestamp,
							cs.reported_object_count, cs.reported_bytes_used, cs.hash,
							cs.id, cs.x_container_sync_point1, cs.x_container_sync_point2,
							cs.storage_policy_index, cs.metadata, maxrowid.max
						FROM container_stat cs, maxrowid`)
	if err := row.Scan(&info.Account, &info.Container, &info.CreatedAt, &info.PutTimestamp,
		&info.DeleteTimestamp, &info.StatusChangedAt, &info.ObjectCount,
		&info.BytesUsed, &info.ReportedPutTimestamp, &info.ReportedDeleteTimestamp,
		&info.ReportedObjectCount, &info.ReportedBytesUsed, &info.Hash,
		&info.ID, &info.XContainerSyncPoint1, &info.XContainerSyncPoint2,
		&info.StoragePolicyIndex, &info.RawMetadata, &info.MaxRow); err != nil {
		if common.IsCorruptDBError(err) {
			return nil, fmt.Errorf("Failed to GetInfo: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
		}
		return nil, err
	}
	if info.RawMetadata == "" {
		info.Metadata = make(map[string][]string)
	} else if err := json.Unmarshal([]byte(info.RawMetadata), &info.Metadata); err != nil {
		return nil, err
	}
	db.infoCache.Store(info)
	return info, nil
}

func (db *sqliteContainer) invalidateCache() {
	db.infoCache.Store(&ContainerInfo{invalid: true})
}

// IsDeleted returns true if the container is deleted - if its delete timestamp is later than its put timestamp.
func (db *sqliteContainer) IsDeleted() (bool, error) {
	info, err := db.GetInfo()
	if err != nil {
		return false, err
	}
	return info.DeleteTimestamp > info.PutTimestamp, nil
}

func (db *sqliteContainer) Remove() error {
	return os.RemoveAll(path.Dir(db.containerFile))
}

// Delete sets the container's deleted timestamp and tombstones any metadata older than that timestamp.
// This may or may not make the container "deleted".
func (db *sqliteContainer) Delete(timestamp string) error {
	if err := db.connect(); err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var metastr string
	var metadata map[string][]string
	if err := tx.QueryRow("SELECT metadata FROM container_info").Scan(&metastr); err != nil {
		if common.IsCorruptDBError(err) {
			return fmt.Errorf("Failed to Delete SELECT: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
		}
		return err
	}
	if err := json.Unmarshal([]byte(metastr), &metadata); err != nil {
		return err
	}
	for key, value := range metadata {
		if value[1] < timestamp {
			metadata[key] = []string{"", timestamp}
		}
	}
	serializedMetadata, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	if _, err = tx.Exec("UPDATE container_info SET delete_timestamp = ?, metadata = ?", timestamp, string(serializedMetadata)); err != nil {
		if common.IsCorruptDBError(err) {
			return fmt.Errorf("Failed to Delete UPDATE: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
		}
		return err
	}
	defer db.invalidateCache()
	if err := tx.Commit(); err != nil {
		if common.IsCorruptDBError(err) {
			return fmt.Errorf("Failed to Delete Commit: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
		}
		return err
	}
	return nil
}

// MergeItems merges ObjectRecords into the container.  If a remote id is provided (incoming replication), the incoming_sync table is updated.
func (db *sqliteContainer) MergeItems(records []*ObjectRecord, remoteID string) error {
	if err := db.connect(); err != nil {
		return err
	}
	type recordID struct {
		policy int
		name   string
	}
	names := make([]interface{}, len(records))
	existing := make(map[recordID]*ObjectRecord)
	toAdd := make(map[recordID]*ObjectRecord)
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for i, record := range records {
		names[i] = record.Name
	}
	for i := 0; i < len(records); i += maxQueryArgs {
		j := i + maxQueryArgs
		if j > len(records) {
			j = len(records)
		}
		batch := names[i:j]
		query := ""
		if db.hasDeletedNameIndex {
			query = fmt.Sprintf("SELECT name, storage_policy_index, created_at, ROWID FROM object WHERE deleted IN (0, 1) AND name IN (%s)",
				strings.TrimRight(strings.Repeat("?,", len(batch)), ","))
		} else {
			query = fmt.Sprintf("SELECT name, storage_policy_index, created_at, ROWID FROM object WHERE name IN (%s)",
				strings.TrimRight(strings.Repeat("?,", len(batch)), ","))
		}
		rows, err := tx.Query(query, batch...)
		if err != nil {
			if common.IsCorruptDBError(err) {
				return fmt.Errorf("Failed to MergeItems SELECT: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
			}
			return err
		}
		for rows.Next() {
			var name, timestamp string
			var rowid int64
			var policy int
			if err := rows.Scan(&name, &policy, &timestamp, &rowid); err != nil {
				if common.IsCorruptDBError(err) {
					return fmt.Errorf("Failed to MergeItems SELECT Scan: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
				}
				return err
			}
			existing[recordID{policy, name}] = &ObjectRecord{CreatedAt: timestamp, Rowid: rowid}
		}
		if err := rows.Err(); err != nil {
			if common.IsCorruptDBError(err) {
				return fmt.Errorf("Failed to MergeItems SELECT Err: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
			}
			return err
		}
		if err := rows.Close(); err != nil {
			if common.IsCorruptDBError(err) {
				return fmt.Errorf("Failed to MergeItems SELECT Close: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
			}
			return err
		}
	}

	dst, err := tx.Prepare("DELETE FROM object WHERE ROWID=?")
	if err != nil {
		return err
	}
	defer dst.Close()

	ast, err := tx.Prepare("INSERT INTO object (name, created_at, size, content_type, etag, deleted, storage_policy_index, expires) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer ast.Close()

	var maxRowid int64 = -1
	for _, record := range records {
		if record.Rowid > maxRowid {
			maxRowid = record.Rowid
		}
		rid := recordID{record.StoragePolicyIndex, record.Name}
		if alreadyIn, ok := toAdd[rid]; !ok || record.CreatedAt > alreadyIn.CreatedAt {
			current, inExisting := existing[rid]
			if inExisting && current.CreatedAt < record.CreatedAt {
				if _, err := dst.Exec(current.Rowid); err != nil {
					if common.IsCorruptDBError(err) {
						return fmt.Errorf("Failed to MergeItems DELETE: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
					}
					return err
				}
				delete(existing, rid)
				toAdd[rid] = record
			} else if !inExisting {
				toAdd[rid] = record
			}
		}
	}

	for _, record := range toAdd {
		if _, err := ast.Exec(record.Name, record.CreatedAt, record.Size, record.ContentType, record.ETag, record.Deleted, record.StoragePolicyIndex, record.Expires); err != nil {
			if common.IsCorruptDBError(err) {
				return fmt.Errorf("Failed to MergeItems INSERT: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
			}
			return err
		}
	}

	if remoteID != "" && maxRowid > -1 {
		if _, err := tx.Exec(`UPDATE incoming_sync SET sync_point = ? WHERE remote_id = ?;
							  INSERT INTO incoming_sync (remote_id, sync_point) SELECT ?, ? WHERE changes() == 0;`,
			maxRowid, remoteID, remoteID, maxRowid); err != nil {
			if common.IsCorruptDBError(err) {
				return fmt.Errorf("Failed to MergeItems UPDATE: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
			}
			return err
		}
	}

	defer db.invalidateCache()
	if err := tx.Commit(); err != nil {
		if common.IsCorruptDBError(err) {
			return fmt.Errorf("Failed to MergeItems Commit: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
		}
		return err
	}
	return nil
}

func indexAfter(s, sep string, after int) int {
	index := strings.Index(s[after:], sep)
	if index == -1 {
		return -1
	}
	return index + after
}

func updateRecord(rec *ObjectListingRecord) error {
	f, err := strconv.ParseFloat(rec.LastModified, 64)
	if err != nil {
		return err
	}
	whole, nans := math.Modf(f)
	rec.LastModified = time.Unix(int64(whole), int64(nans*1.0e9)).In(common.GMT).Format("2006-01-02T15:04:05.000000")

	rec.ContentType, rec.Size, err = common.ParseContentTypeForSlo(
		rec.ContentType, rec.Size)
	return err
}

// ListObjects implements object listings.  Path is a string pointer because behavior is different for empty and missing path query parameters.
func (db *sqliteContainer) ListObjects(limit int, marker string, endMarker string, prefix string, delimiter string,
	pth *string, reverse bool, storagePolicyIndex int) ([]interface{}, error) {
	if err := db.connect(); err != nil {
		return nil, err
	}
	var point, pointDirection, queryTail, queryStart string

	if pth != nil {
		if *pth != "" {
			p := strings.TrimRight(*pth, "/") + "/"
			pth = &p
		}
		delimiter = "/"
		prefix = *pth
	}
	if db.hasDeletedNameIndex {
		queryStart = "SELECT name, created_at, size, content_type, etag FROM object WHERE deleted = 0 AND"
	} else {
		queryStart = "SELECT name, created_at, size, content_type, etag FROM object WHERE +deleted = 0 AND"
	}
	if reverse {
		marker, endMarker = endMarker, marker
		queryTail = "ORDER BY name DESC LIMIT ?"
		pointDirection = "name < ?"
	} else {
		queryTail = "ORDER BY name LIMIT ?"
		pointDirection = "name > ?"
	}

	results := []interface{}{}
	queryArgs := make([]interface{}, 8)
	wheres := make([]string, 8)
	gotResults := true

	for len(results) < limit && gotResults {
		wheres := append(wheres[:0], "storage_policy_index == ?")
		queryArgs := append(queryArgs[:0], storagePolicyIndex)
		if prefix != "" {
			wheres = append(wheres, "name BETWEEN ? AND ?")
			queryArgs = append(queryArgs, prefix, prefix+"\xFF")
		}
		if marker != "" {
			wheres = append(wheres, "name > ?")
			queryArgs = append(queryArgs, marker)
		}
		if endMarker != "" {
			wheres = append(wheres, "name < ?")
			queryArgs = append(queryArgs, endMarker)
		}
		if point != "" {
			wheres = append(wheres, pointDirection)
			queryArgs = append(queryArgs, point)
		}
		rows, err := db.Query(queryStart+" "+strings.Join(wheres, " AND ")+" "+queryTail,
			append(queryArgs, limit-len(results))...)
		if err != nil {
			if common.IsCorruptDBError(err) {
				return nil, fmt.Errorf("Failed to ListObjects SELECT: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
			}
			return nil, err
		}
		gotResults = false
		for rows.Next() && len(results) < limit {
			gotResults = true
			record := &ObjectListingRecord{}
			if err := rows.Scan(&record.Name, &record.LastModified, &record.Size, &record.ContentType, &record.ETag); err != nil {
				if common.IsCorruptDBError(err) {
					return nil, fmt.Errorf("Failed to ListObjects Scan: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
				}
				return nil, err
			}
			point = record.Name
			if delimiter != "" {
				if pth != nil && record.Name == *pth {
					continue
				}
				end := indexAfter(record.Name, delimiter, len(prefix))
				if end >= 0 && (pth == nil || len(record.Name) > end+1) {
					dirName := record.Name[:end] + delimiter
					if reverse {
						point = record.Name[:end+len(delimiter)]
					} else {
						point = dirName + "\xFF"
					}
					if pth == nil && dirName != marker {
						results = append(results, &SubdirListingRecord{Name2: dirName, Name: dirName})
					}
					break
				}
			}
			if err := updateRecord(record); err != nil {
				return nil, err
			}
			results = append(results, record)
		}
		if err := rows.Err(); err != nil {
			if common.IsCorruptDBError(err) {
				return nil, fmt.Errorf("Failed to ListObjects Err: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
			}
			return nil, err
		}
		rows.Close()
		if delimiter == "" && pth == nil {
			break
		}
	}
	return results, nil
}

// NewID sets the container's ID to a new, random string.
func (db *sqliteContainer) NewID() error {
	if err := db.connect(); err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec(`INSERT OR REPLACE INTO incoming_sync (remote_id, sync_point)
					  SELECT container_info.id, maxrowid.max FROM container_info, maxrowid`)
	if err != nil {
		return err
	}
	if _, err = tx.Exec("UPDATE container_info SET id = ?", common.UUID()); err != nil {
		return err
	}
	defer db.invalidateCache()
	return tx.Commit()
}

// ItemsSince returns (count) object records with a rowid greater than (start).
func (db *sqliteContainer) ItemsSince(start int64, count int) ([]*ObjectRecord, error) {
	db.flush()
	records := []*ObjectRecord{}
	rows, err := db.Query(`SELECT ROWID, name, created_at, size, content_type, etag, deleted, storage_policy_index, expires
						   FROM object WHERE ROWID > ? ORDER BY ROWID ASC LIMIT ?`, start, count)
	if err != nil {
		if common.IsCorruptDBError(err) {
			return nil, fmt.Errorf("Failed to ItemsSince SELECT: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
		}
		return nil, err
	}
	for rows.Next() {
		r := &ObjectRecord{}
		if err := rows.Scan(&r.Rowid, &r.Name, &r.CreatedAt, &r.Size, &r.ContentType, &r.ETag, &r.Deleted, &r.StoragePolicyIndex, &r.Expires); err != nil {
			if common.IsCorruptDBError(err) {
				return nil, fmt.Errorf("Failed to ItemsSince Scan: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
			}
			return nil, err
		}
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		if common.IsCorruptDBError(err) {
			return nil, fmt.Errorf("Failed to ItemsSince Err: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
		}
		return nil, err
	}
	rows.Close()
	return records, nil
}

// GetMetadata returns the current container metadata as a simple map[string]string, i.e. it leaves out tombstones and timestamps.
func (db *sqliteContainer) GetMetadata() (map[string]string, error) {
	info, err := db.GetInfo()
	if err != nil {
		return nil, err
	}
	metadata := make(map[string]string)
	for key, value := range info.Metadata {
		if value[0] != "" {
			metadata[key] = value[0]
		}
	}
	return metadata, nil
}

func (db *sqliteContainer) mergeMetas(a map[string][]string, b map[string][]string, deleteTimestamp string) (string, error) {
	newMeta := map[string][]string{}
	for k, v := range a {
		newMeta[k] = v
	}
	for k, v := range b {
		if existing, ok := a[k]; ok {
			if existing[1] < v[1] {
				newMeta[k] = v
			}
		} else {
			newMeta[k] = v
		}
	}
	metaSize := 0
	metaCount := 0
	for k, v := range newMeta {
		if deleteTimestamp != "" && v[1] < deleteTimestamp {
			newMeta[k] = []string{"", deleteTimestamp}
		} else if v[0] != "" && strings.HasPrefix(strings.ToLower(k), "x-container-meta-") {
			metaSize += len(k) - 17
			metaSize += len(v[0])
			metaCount++
		}
	}
	if metaCount > maxMetaCount || metaSize > maxMetaOverallSize {
		return "", ErrorInvalidMetadata
	}
	serMeta, err := json.Marshal(newMeta)
	if err != nil {
		return "", err
	}
	return string(serMeta), nil
}

// UpdateMetadata merges the current container metadata with new incoming metadata.
func (db *sqliteContainer) UpdateMetadata(newMetadata map[string][]string, timestamp string) error {
	if err := db.connect(); err != nil {
		return err
	}
	if len(newMetadata) == 0 {
		return nil
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var metadataValue, deleteTimestamp string
	if err := tx.QueryRow("SELECT metadata, delete_timestamp FROM container_info").Scan(&metadataValue, &deleteTimestamp); err != nil {
		if common.IsCorruptDBError(err) {
			return fmt.Errorf("Failed to UpdateMetadata SELECT: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
		}
		return err
	}
	var existingMetadata map[string][]string
	if metadataValue == "" {
		existingMetadata = map[string][]string{}
	} else if err := json.Unmarshal([]byte(metadataValue), &existingMetadata); err != nil {
		return err
	}
	metastr, err := db.mergeMetas(existingMetadata, newMetadata, deleteTimestamp)
	if err != nil {
		return err
	}
	if _, err = tx.Exec("UPDATE container_info SET metadata=?, put_timestamp=MAX(put_timestamp, ?)", metastr, timestamp); err != nil {
		if common.IsCorruptDBError(err) {
			return fmt.Errorf("Failed to UpdateMetadata UPDATE: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
		}
		return err
	}
	defer db.invalidateCache()
	if err := tx.Commit(); err != nil {
		if common.IsCorruptDBError(err) {
			return fmt.Errorf("Failed to UpdateMetadata Commit: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
		}
		return err
	}
	return nil
}

// MergeSyncTable updates the container's current incoming_sync table records.
func (db *sqliteContainer) MergeSyncTable(records []*SyncRecord) error {
	if err := db.connect(); err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, record := range records {
		if _, err := tx.Exec(`UPDATE incoming_sync SET sync_point = ? WHERE remote_id = ?;
							  INSERT INTO incoming_sync (remote_id, sync_point) SELECT ?, ? WHERE changes() == 0;`,
			record.SyncPoint, record.RemoteID, record.RemoteID, record.SyncPoint); err != nil {
			if common.IsCorruptDBError(err) {
				return fmt.Errorf("Failed to MergeSyncTable UPDATE: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
			}
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		if common.IsCorruptDBError(err) {
			return fmt.Errorf("Failed to MergeSyncTable Commit: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
		}
		return err
	}
	return nil
}

// CleanupTombstones removes any expired tombstoned objects or metadata.
func (db *sqliteContainer) CleanupTombstones(reclaimAge int64) error {
	if err := db.connect(); err != nil {
		return err
	}
	now := float64(time.Now().UnixNano()) / 1000000000.0
	reclaimTimestamp := common.CanonicalTimestamp(now - float64(reclaimAge))

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.Exec("DELETE FROM object WHERE deleted=1 AND created_at < ?", reclaimTimestamp); err != nil {
		if common.IsCorruptDBError(err) {
			return fmt.Errorf("Failed to CleanupTombstones DELETE: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
		}
		return err
	}
	var metastr string
	if err := tx.QueryRow("SELECT metadata FROM container_info").Scan(&metastr); err != nil {
		if common.IsCorruptDBError(err) {
			return fmt.Errorf("Failed to CleanupTombstones SELECT: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
		}
		return err
	}
	var metadata map[string][]string
	updated := false
	if metastr == "" {
		metadata = map[string][]string{}
		updated = true
	} else {
		if err := json.Unmarshal([]byte(metastr), &metadata); err != nil {
			return err
		}
		for k, v := range metadata {
			if v[0] == "" {
				if ts, err := common.GetEpochFromTimestamp(v[1]); err != nil || ts < reclaimTimestamp {
					delete(metadata, k)
					updated = true
				}
			}
		}
	}
	if updated {
		if mb, err := json.Marshal(metadata); err != nil {
			return err
		} else if _, err = tx.Exec("UPDATE container_info SET metadata = ?", string(mb)); err != nil {
			if common.IsCorruptDBError(err) {
				return fmt.Errorf("Failed to CleanupTombstones UPDATE: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
			}
			return err
		}
	}
	defer db.invalidateCache()
	if err := tx.Commit(); err != nil {
		if common.IsCorruptDBError(err) {
			return fmt.Errorf("Failed to CleanupTombstones Commit: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
		}
		return err
	}
	return nil
}

// SyncTable returns the container's current incoming_sync table, and also includes the current container's id and max row as an entry.
func (db *sqliteContainer) SyncTable() ([]*SyncRecord, error) {
	if err := db.connect(); err != nil {
		return nil, err
	}
	records := []*SyncRecord{}
	rows, err := db.Query(`SELECT sync_point, remote_id FROM incoming_sync
						   WHERE remote_id NOT IN (SELECT id FROM container_info)
						   UNION
						   SELECT maxrowid.max AS sync_point, container_info.id AS remote_id
						   FROM container_info, maxrowid`)
	if err != nil {
		if common.IsCorruptDBError(err) {
			return nil, fmt.Errorf("Failed to SyncTable SELECT: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
		}
		return nil, err
	}
	for rows.Next() {
		rec := &SyncRecord{}
		if err := rows.Scan(&rec.SyncPoint, &rec.RemoteID); err != nil {
			if common.IsCorruptDBError(err) {
				return nil, fmt.Errorf("Failed to SyncTable Scan: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
			}
			return nil, err
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		if common.IsCorruptDBError(err) {
			return nil, fmt.Errorf("Failed to SyncTable Err: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
		}
		return nil, err
	}
	rows.Close()
	return records, nil
}

// SyncRemoteData compares a remote container's info to the local info and updates any necessary replication bookkeeping, returning the current container's info.
func (db *sqliteContainer) SyncRemoteData(maxRow int64, hash, id, createdAt, putTimestamp, deleteTimestamp, metadata string) (*ContainerInfo, error) {
	if err := db.connect(); err != nil {
		return nil, err
	}
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var localMeta, localHash, localDeleteTimestamp string
	var localPoint int64
	if err := tx.QueryRow("SELECT hash, metadata, delete_timestamp FROM container_info").Scan(&localHash, &localMeta, &localDeleteTimestamp); err != nil {
		if common.IsCorruptDBError(err) {
			return nil, fmt.Errorf("Failed to SyncRemoteData SELECT: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
		}
		return nil, err
	}
	var lm, rm map[string][]string
	if err := json.Unmarshal([]byte(metadata), &rm); err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(localMeta), &lm); err != nil {
		return nil, err
	}
	if deleteTimestamp > localDeleteTimestamp {
		localDeleteTimestamp = deleteTimestamp
	}
	metastr, err := db.mergeMetas(lm, rm, localDeleteTimestamp)
	if _, err = tx.Exec(`UPDATE container_info SET created_at=MIN(?, created_at), put_timestamp=MAX(?, put_timestamp),
	  					 delete_timestamp=MAX(?, delete_timestamp), metadata=?`,
		createdAt, putTimestamp, deleteTimestamp, metastr); err != nil {
		if common.IsCorruptDBError(err) {
			return nil, fmt.Errorf("Failed to SyncRemoteData UPDATE: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
		}
		return nil, err
	}
	if err := tx.QueryRow("SELECT IFNULL(MAX(sync_point), -1) FROM incoming_sync WHERE remote_id = ?", id).Scan(&localPoint); err != nil {
		if common.IsCorruptDBError(err) {
			return nil, fmt.Errorf("Failed to SyncRemoteData SELECT2: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
		}
		return nil, err
	}
	if localHash == hash && maxRow > localPoint {
		localPoint = maxRow
		if _, err = tx.Exec("INSERT OR REPLACE INTO incoming_sync (remote_id, sync_point) VALUES (?, ?)", id, localPoint); err != nil {
			if common.IsCorruptDBError(err) {
				return nil, fmt.Errorf("Failed to SyncRemoteData INSERT: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
			}
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		if common.IsCorruptDBError(err) {
			return nil, fmt.Errorf("Failed to SyncRemoteData Commit: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
		}
		return nil, err
	}
	db.invalidateCache()
	info, err := db.GetInfo()
	if err != nil {
		return nil, err
	}
	info.Point = localPoint
	return info, nil
}

// CheckSyncLink makes sure the database's container sync symlink exists or doesn't exist, as in accordance with the existence of the X-Container-Sync-To header.
func (db *sqliteContainer) CheckSyncLink() error {
	metadata, err := db.GetMetadata()
	if err != nil {
		return err
	}
	containersDir := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(db.containerFile))))
	deviceDir := filepath.Dir(containersDir)
	pathFromDataDir, err := filepath.Rel(containersDir, db.containerFile)
	if err != nil {
		return err
	}
	symLoc := filepath.Join(deviceDir, "sync_containers", pathFromDataDir)
	if metadata["X-Container-Sync-To"] != "" {
		if fs.Exists(symLoc) {
			return nil
		}
		if err := os.MkdirAll(filepath.Dir(symLoc), 0755); err != nil {
			return err
		}
		return os.Symlink(db.containerFile, symLoc)
	} else if fs.Exists(symLoc) {
		for err := error(nil); err == nil; symLoc = filepath.Dir(symLoc) {
			err = os.Remove(symLoc)
		}
	}
	return nil
}

// OpenDatabaseFile blocks updates and opens the underlying database file for reading, so it can be uploaded to a remote server.
func (db *sqliteContainer) OpenDatabaseFile() (*os.File, func(), error) {
	if err := db.connect(); err != nil {
		return nil, nil, err
	}
	var fp *os.File
	if _, err := db.Exec(`
		PRAGMA locking_mode = EXCLUSIVE;      -- grab and hold a shared lock
		SELECT 1 FROM container_info LIMIT 1; -- it doesn't actually lock until you hit the database
		PRAGMA wal_checkpoint(TRUNCATE);      -- truncate the wal file, if it exists`,
	); err != nil {
		if common.IsCorruptDBError(err) {
			return nil, nil, fmt.Errorf("Failed to OpenDatabaseFile: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
		}
		return nil, nil, fmt.Errorf("Error locking database%s: %v", db.containerFile, err)
	}
	cleanup := func() {
		db.Exec(`
			PRAGMA locking_mode = NORMAL;         -- release shared lock
			SELECT 1 FROM container_info LIMIT 1; -- actually release shared lock
		`)
		if fp != nil {
			fp.Close()
		}
	}
	// make sure there aren't any journals lying around
	if stat, err := os.Stat(db.containerFile + "-wal"); err == nil && stat.Size() != 0 {
		cleanup()
		return nil, nil, fmt.Errorf("Stubborn wal file still exists: %s", db.containerFile)
	}
	if stat, err := os.Stat(db.containerFile + "-journal"); err == nil && stat.Size() != 0 {
		cleanup()
		return nil, nil, fmt.Errorf("Stubborn journal file still exists: %s", db.containerFile)
	}
	fp, err := os.Open(db.containerFile)
	if err != nil {
		fp = nil
		cleanup()
		return nil, nil, fmt.Errorf("Error opening %s: %v", db.containerFile, err)
	}
	return fp, cleanup, nil
}

// ID returns the container's ring hash as a unique identifier for it.
func (db *sqliteContainer) ID() string {
	return db.ringhash
}

// RingHash returns the container's ring hash as a string.
func (db *sqliteContainer) RingHash() string {
	return db.ringhash
}

func (db *sqliteContainer) flushAlreadyLocked() error {
	if err := db.connect(); err != nil {
		return err
	}
	if stat, err := os.Stat(db.containerFile + ".pending"); err != nil || stat.Size() == 0 {
		return nil
	}
	contents, err := ioutil.ReadFile(db.containerFile + ".pending")
	if err != nil || len(contents) == 0 {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var records []*ObjectRecord
	for _, base64ed := range strings.Split(string(contents), ":") {
		if len(base64ed) < 1 {
			continue
		}
		serialized, err := base64.StdEncoding.DecodeString(base64ed)
		if err != nil {
			continue
		}
		if serialized[0] == '{' {
			rec := &ObjectRecord{}
			if err = json.Unmarshal(serialized, &rec); err == nil {
				records = append(records, rec)
			} else {
				return fmt.Errorf("Invalid commit pending json record")
			}
		} else {
			r, err := pickle.PickleLoads(serialized)
			if err != nil {
				continue
			}
			record, ok := r.([]interface{})
			if !ok || len(record) < 7 {
				return fmt.Errorf("Invalid commit pending record")
			}
			casts := make([]bool, 7)
			var deleted, policy int64
			rec := &ObjectRecord{}
			rec.Name, casts[0] = record[0].(string)
			rec.CreatedAt, casts[1] = record[1].(string)
			rec.Size, casts[2] = record[2].(int64)
			rec.ContentType, casts[3] = record[3].(string)
			rec.ETag, casts[4] = record[4].(string)
			deleted, casts[5] = record[5].(int64)
			policy, casts[6] = record[6].(int64)
			rec.Deleted = int(deleted)
			rec.StoragePolicyIndex = int(policy)
			for i := 0; i < 7; i++ {
				if !casts[i] {
					return fmt.Errorf("Invalid commit pending record")
				}
			}
			records = append(records, rec)
		}
	}
	err = db.MergeItems(records, "")
	if err == nil {
		err = os.Truncate(db.containerFile+".pending", 0)
	}
	return err
}

func (db *sqliteContainer) flush() error {
	lock, err := fs.LockPath(filepath.Dir(db.containerFile), 10*time.Second)
	if err != nil {
		return err
	}
	defer lock.Close()
	return db.flushAlreadyLocked()
}

func (db *sqliteContainer) addObject(name string, timestamp string, size int64, contentType string, etag string, deleted int, storagePolicyIndex int, expires string) error {
	lock, err := fs.LockPath(filepath.Dir(db.containerFile), 10*time.Second)
	if err != nil {
		return err
	}
	defer lock.Close()
	rec := ObjectRecord{
		Name:               name,
		CreatedAt:          timestamp,
		Size:               size,
		ContentType:        contentType,
		ETag:               etag,
		Deleted:            deleted,
		StoragePolicyIndex: storagePolicyIndex,
		Expires:            &expires,
	}
	if expires == "" {
		rec.Expires = nil
	}
	file, err := os.OpenFile(db.containerFile+".pending", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	marshalled, err := json.Marshal(&rec)
	if err != nil {
		return err
	}
	if _, err := file.WriteString(":" + base64.StdEncoding.EncodeToString(marshalled)); err != nil {
		return err
	}
	if info, err := file.Stat(); err == nil && info.Size() > pendingCap {
		db.flushAlreadyLocked()
	}
	return nil
}

// PutObject adds an object to the container, by way of pending file.
func (db *sqliteContainer) PutObject(name string, timestamp string, size int64, contentType string, etag string, storagePolicyIndex int, expires string) error {
	return db.addObject(name, timestamp, size, contentType, etag, 0, storagePolicyIndex, expires)
}

// DeleteObject removes an object from the container, by way of pending file.
func (db *sqliteContainer) DeleteObject(name string, timestamp string, storagePolicyIndex int) error {
	return db.addObject(name, timestamp, 0, "", "", 1, storagePolicyIndex, "")
}

// Close closes the underlying sqlite database connection.
func (db *sqliteContainer) Close() error {
	db.connectLock.Lock()
	defer db.connectLock.Unlock()
	return db.closeAlreadyLocked()
}

func (db *sqliteContainer) closeAlreadyLocked() error {
	if db.DB != nil {
		err := db.DB.Close()
		db.DB = nil
		return err
	}
	return nil
}

func sqliteCreateExistingContainer(db Container, putTimestamp string, newMetadata map[string][]string, policyIndex, defaultPolicyIndex int) (bool, error) {
	cdb, ok := db.(*sqliteContainer)
	if !ok {
		return false, errors.New("Unable to work with non-sqliteContainer")
	}
	if err := cdb.connect(); err != nil {
		return false, err
	}
	tx, err := cdb.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var cDeleteTimestamp, cPutTimestamp, cMetadata string
	var cPolicyIndex int
	row := tx.QueryRow("SELECT put_timestamp, delete_timestamp, storage_policy_index, metadata FROM container_info")
	if err := row.Scan(&cPutTimestamp, &cDeleteTimestamp, &cPolicyIndex, &cMetadata); err != nil {
		if common.IsCorruptDBError(err) {
			return false, fmt.Errorf("Failed to sqliteCreateExistingContainer SELECT: %v; %v", err, common.QuarantineDir(path.Dir(cdb.containerFile), 4, "containers"))
		}
		return false, err
	}
	if cDeleteTimestamp <= cPutTimestamp { // not deleted
		if policyIndex < 0 {
			policyIndex = cPolicyIndex
		} else if cPolicyIndex != policyIndex {
			return false, ErrorPolicyConflict
		}
	} else { // deleted
		if policyIndex < 0 {
			policyIndex = defaultPolicyIndex
		}
	}
	var existingMetadata map[string][]string
	if cMetadata == "" {
		existingMetadata = make(map[string][]string)
	} else if err := json.Unmarshal([]byte(cMetadata), &existingMetadata); err != nil {
		return false, err
	}
	metastr, err := cdb.mergeMetas(existingMetadata, newMetadata, cDeleteTimestamp)
	if err != nil {
		return false, err
	}
	if _, err := tx.Exec("UPDATE container_info SET put_timestamp = ?, storage_policy_index = ?, metadata = ?",
		putTimestamp, policyIndex, metastr); err != nil {
		if common.IsCorruptDBError(err) {
			return false, fmt.Errorf("Failed to sqliteCreateExistingContainer UPDATE: %v; %v", err, common.QuarantineDir(path.Dir(cdb.containerFile), 4, "containers"))
		}
		return false, err
	}
	defer cdb.invalidateCache()
	if err := tx.Commit(); err != nil {
		if common.IsCorruptDBError(err) {
			return false, fmt.Errorf("Failed to sqliteCreateExistingContainer Commit: %v; %v", err, common.QuarantineDir(path.Dir(cdb.containerFile), 4, "containers"))
		}
		return false, err
	}
	return (cDeleteTimestamp > cPutTimestamp && putTimestamp > cDeleteTimestamp), nil
}

func sqliteCreateContainer(containerFile string, account string, container string, putTimestamp string,
	metadata map[string][]string, policyIndex int) error {
	var serializedMetadata []byte
	var err error

	hashDir := filepath.Dir(containerFile)
	if err := os.MkdirAll(hashDir, 0755); err != nil {
		return err
	}
	lock, err := fs.LockPath(filepath.Dir(hashDir), 10*time.Second)
	if err != nil {
		return err
	}
	defer lock.Close()

	if fs.Exists(containerFile) {
		return nil
	}

	if metadata == nil {
		serializedMetadata = []byte("{}")
	} else if serializedMetadata, err = json.Marshal(metadata); err != nil {
		return err
	}

	tfp, err := ioutil.TempFile(hashDir, ".newdb")
	if err != nil {
		return err
	}
	if err := tfp.Chmod(0644); err != nil {
		return err
	}
	defer tfp.Close()
	tempFile := tfp.Name()
	dbConn, err := sql.Open("sqlite3_hummingbird", "file:"+tempFile+"?psow=1&_txlock=immediate&mode=rwc")
	if err != nil {
		return err
	}
	defer dbConn.Close()
	tx, err := dbConn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(objectTableScript + policyStatTableScript + policyStatTriggerScript +
		containerInfoTableScript + containerStatViewScript + syncTableScript); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO container_info (account, container, created_at, id, put_timestamp,
						  status_changed_at, storage_policy_index, metadata) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		account, container, common.GetTimestamp(), common.UUID(), putTimestamp,
		putTimestamp, policyIndex, string(serializedMetadata)); err != nil {
		return err
	}
	if _, err := tx.Exec("INSERT INTO policy_stat (storage_policy_index) VALUES (?)", policyIndex); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if err := os.Rename(tempFile, containerFile); err != nil {
		return err
	}
	return nil
}

func sqliteOpenContainer(containerFile string) (ReplicableContainer, error) {
	if !fs.Exists(containerFile) {
		return nil, ErrorNoSuchContainer
	}
	db := &sqliteContainer{
		containerFile:       containerFile,
		hasDeletedNameIndex: false,
		ringhash:            filepath.Base(filepath.Dir(containerFile)),
	}
	return db, nil
}

func (db *sqliteContainer) Reported(putTimestamp, deleteTimestamp string, objectCount, bytesUsed int64) error {
	if err := db.connect(); err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.Exec("UPDATE container_info SET reported_put_timestamp = ?, reported_delete_timestamp = ?, reported_object_count = ?, reported_bytes_used = ?", putTimestamp, deleteTimestamp, objectCount, bytesUsed); err != nil {
		if common.IsCorruptDBError(err) {
			return fmt.Errorf("Failed to Reported UPDATE: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
		}
		return err
	}
	defer db.invalidateCache()
	if err := tx.Commit(); err != nil {
		if common.IsCorruptDBError(err) {
			return fmt.Errorf("Failed to Reported Commit: %v; %v", err, common.QuarantineDir(path.Dir(db.containerFile), 4, "containers"))
		}
		return err
	}
	return nil
}
