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
	"net/http"
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
var policyStatsCacheTimeout = time.Second * 10

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
	sql.Register("sqlite3_account",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				if err := conn.RegisterFunc("chexor", chexor, true); err != nil {
					return err
				}
				if _, err := conn.Exec(pragmaScript, nil); err != nil {
					return err
				}
				if _, err := conn.Exec(`CREATE TEMPORARY VIEW IF NOT EXISTS maxrowid (max) AS
				  						SELECT IFNULL(MAX(seq), -1) FROM sqlite_sequence WHERE name='container'`, nil); err != nil {
					return err
				}
				return nil
			},
		},
	)
}

type sqliteAccount struct {
	connectLock sync.Mutex
	*sql.DB
	accountFile         string
	hasDeletedNameIndex bool
	infoCache           atomic.Value
	policyStatsCache    atomic.Value
	ringhash            string
}

var _ Account = &sqliteAccount{}

func (db *sqliteAccount) connect() error {
	db.connectLock.Lock()
	defer db.connectLock.Unlock()
	if db.DB != nil {
		return nil
	}
	dbConn, err := sql.Open("sqlite3_account", "file:"+db.accountFile+"?psow=1&_txlock=immediate&mode=rw")
	if err != nil {
		if common.IsCorruptDBError(err) {
			return fmt.Errorf("Failed to open: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
		}
		return fmt.Errorf("Failed to open: %v", err)
	}
	dbConn.SetMaxOpenConns(maxOpenConns)
	dbConn.SetMaxIdleConns(maxIdleConns)
	if db.hasDeletedNameIndex, err = schemaMigrate(dbConn); err != nil {
		db.closeAlreadyLocked()
		if common.IsCorruptDBError(err) {
			return fmt.Errorf("Error migrating database: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
		}
		return fmt.Errorf("Error migrating database: %v", err)
	}
	db.DB = dbConn
	return nil
}

// GetInfo returns the account's information as a AccountInfo struct.
func (db *sqliteAccount) GetInfo() (*AccountInfo, error) {
	if err := db.connect(); err != nil {
		return nil, err
	}
	if err := db.flush(); err != nil {
		return nil, err
	}
	if info, ok := db.infoCache.Load().(*AccountInfo); ok && !info.invalid && time.Since(info.updated) < infoCacheTimeout {
		return info, nil
	}
	info := &AccountInfo{updated: time.Now()}
	row := db.QueryRow(`SELECT cs.account, cs.created_at, cs.put_timestamp,
							cs.delete_timestamp, cs.status_changed_at,
							cs.object_count, cs.bytes_used, cs.container_count,
							cs.hash, cs.id, cs.metadata, maxrowid.max
						FROM account_stat cs, maxrowid`)
	if err := row.Scan(&info.Account, &info.CreatedAt, &info.PutTimestamp,
		&info.DeleteTimestamp, &info.StatusChangedAt,
		&info.ObjectCount, &info.BytesUsed, &info.ContainerCount,
		&info.Hash, &info.ID, &info.RawMetadata, &info.MaxRow); err != nil {
		if common.IsCorruptDBError(err) {
			return nil, fmt.Errorf("Failed to GetInfo: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
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

func (db *sqliteAccount) PolicyStats() ([]*PolicyStat, error) {
	if err := db.connect(); err != nil {
		return nil, err
	}
	if err := db.flush(); err != nil {
		return nil, err
	}
	if ps, ok := db.policyStatsCache.Load().(*policyStats); ok && !ps.invalid && time.Since(ps.updated) < policyStatsCacheTimeout {
		return ps.list, nil
	}
	ps := &policyStats{updated: time.Now()}
	rows, err := db.Query(`SELECT storage_policy_index, container_count, object_count, bytes_used FROM policy_stat`)
	if err != nil {
		if common.IsCorruptDBError(err) {
			return nil, fmt.Errorf("Failed to PolicyStats SELECT: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
		}
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		e := &PolicyStat{}
		if err := rows.Scan(&e.StoragePolicyIndex, &e.ContainerCount, &e.ObjectCount, &e.BytesUsed); err != nil {
			if common.IsCorruptDBError(err) {
				return nil, fmt.Errorf("Failed to PolicyStats Scan: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
			}
			return nil, err
		}
		ps.list = append(ps.list, e)
	}
	db.policyStatsCache.Store(ps)
	return ps.list, nil
}

func (db *sqliteAccount) invalidateCache() {
	db.infoCache.Store(&AccountInfo{invalid: true})
	db.policyStatsCache.Store(&policyStats{invalid: true})
}

// IsDeleted returns true if the account is deleted - if its delete timestamp is later than its put timestamp.
func (db *sqliteAccount) IsDeleted() (bool, error) {
	info, err := db.GetInfo()
	if err != nil {
		return false, err
	}
	return info.DeleteTimestamp > info.PutTimestamp, nil
}

// Delete sets the account's deleted timestamp and tombstones any metadata older than that timestamp.
// This may or may not make the account "deleted".
func (db *sqliteAccount) Delete(timestamp string) (int, error) {
	if err := db.connect(); err != nil {
		return http.StatusInternalServerError, err
	}
	tx, err := db.Begin()
	if err != nil {
		return http.StatusInternalServerError, err
	}
	defer tx.Rollback()
	var deleteTimestamp, metastr string
	var metadata map[string][]string
	if err := tx.QueryRow("SELECT delete_timestamp, metadata FROM account_stat").Scan(&deleteTimestamp, &metastr); err != nil {
		if common.IsCorruptDBError(err) {
			return http.StatusInternalServerError, fmt.Errorf("Failed to Delete SELECT: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
		}
		return http.StatusInternalServerError, err
	}
	if deleteTimestamp >= timestamp {
		return http.StatusConflict, nil
	}
	if err := json.Unmarshal([]byte(metastr), &metadata); err != nil {
		return http.StatusInternalServerError, err
	}
	for key, value := range metadata {
		if value[1] < timestamp {
			metadata[key] = []string{"", timestamp}
		}
	}
	serializedMetadata, err := json.Marshal(metadata)
	if err != nil {
		return http.StatusInternalServerError, err
	}
	if _, err = tx.Exec("UPDATE account_stat SET delete_timestamp = ?, metadata = ?", timestamp, string(serializedMetadata)); err != nil {
		if common.IsCorruptDBError(err) {
			return http.StatusInternalServerError, fmt.Errorf("Failed to Delete UPDATE: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
		}
		return http.StatusInternalServerError, err
	}
	defer db.invalidateCache()
	if err := tx.Commit(); err != nil {
		if common.IsCorruptDBError(err) {
			return http.StatusInternalServerError, fmt.Errorf("Failed to Delete Commit: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
		}
		return http.StatusInternalServerError, err
	}
	return http.StatusNoContent, nil
}

// MergeItems merges ContainerRecords into the account.  If a remote id is provided (incoming replication), the incoming_sync table is updated.
func (db *sqliteAccount) MergeItems(records []*ContainerRecord, remoteID string) error {
	if err := db.connect(); err != nil {
		return err
	}
	names := make([]interface{}, len(records))
	existing := make(map[string]*ContainerRecord)
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
			query = fmt.Sprintf("SELECT name, put_timestamp, delete_timestamp, ROWID FROM container WHERE deleted IN (0, 1) AND name IN (%s)",
				strings.TrimRight(strings.Repeat("?,", len(batch)), ","))
		} else {
			query = fmt.Sprintf("SELECT name, put_timestamp, delete_timestamp, ROWID FROM container WHERE name IN (%s)",
				strings.TrimRight(strings.Repeat("?,", len(batch)), ","))
		}
		rows, err := tx.Query(query, batch...)
		if err != nil {
			if common.IsCorruptDBError(err) {
				return fmt.Errorf("Failed to MergeItems SELECT: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
			}
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var name, putTimestamp, deleteTimestamp string
			var rowid int64
			if err := rows.Scan(&name, &putTimestamp, &deleteTimestamp, &rowid); err != nil {
				if common.IsCorruptDBError(err) {
					return fmt.Errorf("Failed to MergeItems Scan: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
				}
				return err
			}
			existing[name] = &ContainerRecord{PutTimestamp: putTimestamp, DeleteTimestamp: deleteTimestamp, Rowid: rowid}
		}
		if err := rows.Err(); err != nil {
			if common.IsCorruptDBError(err) {
				return fmt.Errorf("Failed to MergeItems Err: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
			}
			return err
		}
		if err := rows.Close(); err != nil {
			if common.IsCorruptDBError(err) {
				return fmt.Errorf("Failed to MergeItems Close: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
			}
			return err
		}
	}

	dst, err := tx.Prepare("DELETE FROM container WHERE ROWID=?")
	if err != nil {
		return err
	}
	defer dst.Close()

	ast, err := tx.Prepare(`INSERT INTO container (name, put_timestamp, delete_timestamp, object_count, bytes_used, deleted, storage_policy_index)
							VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer ast.Close()

	var maxRowid int64 = -1
	for _, record := range records {
		if record.Rowid > maxRowid {
			maxRowid = record.Rowid
		}
		if er, exists := existing[record.Name]; exists {
			if _, err := dst.Exec(er.Rowid); err != nil {
				if common.IsCorruptDBError(err) {
					return fmt.Errorf("Failed to MergeItems DELETE: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
				}
				return err
			}
			if er.PutTimestamp > record.PutTimestamp {
				record.PutTimestamp = er.PutTimestamp
			}
			if er.DeleteTimestamp > record.DeleteTimestamp {
				record.DeleteTimestamp = er.DeleteTimestamp
			}
			if record.DeleteTimestamp > record.PutTimestamp {
				record.Deleted = 1
			} else {
				record.Deleted = 0
			}
		}
		if res, err := ast.Exec(record.Name, record.PutTimestamp, record.DeleteTimestamp, record.ObjectCount,
			record.BytesUsed, record.Deleted, record.StoragePolicyIndex); err != nil {
			if common.IsCorruptDBError(err) {
				return fmt.Errorf("Failed to MergeItems INSERT: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
			}
			return err
		} else if record.Rowid, err = res.LastInsertId(); err != nil {
			return err
		}
		existing[record.Name] = record
	}

	if remoteID != "" && maxRowid > -1 {
		if _, err := tx.Exec(`UPDATE incoming_sync SET sync_point = ? WHERE remote_id = ?;
							  INSERT INTO incoming_sync (remote_id, sync_point) SELECT ?, ? WHERE changes() == 0;`,
			maxRowid, remoteID, remoteID, maxRowid); err != nil {
			if common.IsCorruptDBError(err) {
				return fmt.Errorf("Failed to MergeItems UPDATE: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
			}
			return err
		}
	}

	defer db.invalidateCache()
	if err := tx.Commit(); err != nil {
		if common.IsCorruptDBError(err) {
			return fmt.Errorf("Failed to MergeItems Commit: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
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

// ListContainers implements container listings.
func (db *sqliteAccount) ListContainers(limit int, marker string, endMarker string, prefix string, delimiter string,
	reverse bool) ([]interface{}, error) {
	if err := db.connect(); err != nil {
		return nil, err
	}
	var point, pointDirection, queryTail, queryStart string

	queryStart = "SELECT name, object_count, bytes_used, put_timestamp FROM container WHERE "
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
		if db.hasDeletedNameIndex {
			wheres = append(wheres[:0], "deleted = 0")
		} else {
			wheres = append(wheres[:0], "+deleted = 0")
		}
		queryArgs = queryArgs[:0]
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
				return nil, fmt.Errorf("Failed to ListContainers SELECT: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
			}
			return nil, err
		}
		defer rows.Close()
		gotResults = false
		for rows.Next() && len(results) < limit {
			gotResults = true
			record := &ContainerListingRecord{}
			if err := rows.Scan(&record.Name, &record.Count, &record.Bytes, &record.LastModified); err != nil {
				if common.IsCorruptDBError(err) {
					return nil, fmt.Errorf("Failed to ListContainers Scan: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
				}
				return nil, err
			}
			if f, err := strconv.ParseFloat(record.LastModified, 64); err != nil {
				return nil, err
			} else {
				whole, nans := math.Modf(f)
				record.LastModified = time.Unix(int64(whole), int64(nans*1.0e9)).In(common.GMT).Format("2006-01-02T15:04:05.000000")
			}
			point = record.Name
			if delimiter != "" {
				end := indexAfter(record.Name, delimiter, len(prefix))
				if end >= 0 && len(record.Name) > end+1 {
					dirName := record.Name[:end] + delimiter
					if reverse {
						point = record.Name[:end+len(delimiter)]
					} else {
						point = dirName + "\xFF"
					}
					if dirName != marker {
						results = append(results, &SubdirListingRecord{Name2: dirName, Name: dirName})
					}
					break
				}
			}
			results = append(results, record)
		}
		if err := rows.Err(); err != nil {
			if common.IsCorruptDBError(err) {
				return nil, fmt.Errorf("Failed to ListContainers Err: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
			}
			return nil, err
		}
		rows.Close()
		if delimiter == "" {
			break
		}
	}
	return results, nil
}

// NewID sets the account's ID to a new, random string.
func (db *sqliteAccount) NewID() error {
	if err := db.connect(); err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec(`INSERT OR REPLACE INTO incoming_sync (remote_id, sync_point)
					  SELECT account_stat.id, maxrowid.max FROM account_stat, maxrowid`)
	if err != nil {
		if common.IsCorruptDBError(err) {
			return fmt.Errorf("Failed to NewID INSERT: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
		}
		return err
	}
	if _, err = tx.Exec("UPDATE account_stat SET id = ?", common.UUID()); err != nil {
		if common.IsCorruptDBError(err) {
			return fmt.Errorf("Failed to NewID UPDATE: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
		}
		return err
	}
	defer db.invalidateCache()
	if err := tx.Commit(); err != nil {
		if common.IsCorruptDBError(err) {
			return fmt.Errorf("Failed to NewID Commit: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
		}
		return err
	}
	return nil
}

// ItemsSince returns (count) object records with a rowid greater than (start).
func (db *sqliteAccount) ItemsSince(start int64, count int) ([]*ContainerRecord, error) {
	db.flush()
	records := []*ContainerRecord{}
	rows, err := db.Query(`SELECT ROWID, name, put_timestamp, delete_timestamp, object_count,
						   bytes_used, deleted, storage_policy_index
						   FROM container WHERE ROWID > ? ORDER BY ROWID ASC LIMIT ?`, start, count)
	if err != nil {
		if common.IsCorruptDBError(err) {
			return nil, fmt.Errorf("Failed to ItemsSince SELECT: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
		}
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		r := &ContainerRecord{}
		if err := rows.Scan(&r.Rowid, &r.Name, &r.PutTimestamp, &r.DeleteTimestamp, &r.ObjectCount, &r.BytesUsed, &r.Deleted, &r.StoragePolicyIndex); err != nil {
			if common.IsCorruptDBError(err) {
				return nil, fmt.Errorf("Failed to ItemsSince Scan: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
			}
			return nil, err
		}
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		if common.IsCorruptDBError(err) {
			return nil, fmt.Errorf("Failed to ItemsSince Err: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
		}
		return nil, err
	}
	return records, nil
}

// GetMetadata returns the current account metadata as a simple map[string]string, i.e. it leaves out tombstones and timestamps.
func (db *sqliteAccount) GetMetadata() (map[string]string, error) {
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

func (db *sqliteAccount) mergeMetas(a map[string][]string, b map[string][]string, deleteTimestamp string) (string, error) {
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
		} else if v[0] != "" && strings.HasPrefix(strings.ToLower(k), "x-account-meta-") {
			metaSize += len(k) - 15
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

// UpdateMetadata merges the current account metadata with new incoming metadata.
func (db *sqliteAccount) UpdateMetadata(newMetadata map[string][]string) error {
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
	if err := tx.QueryRow("SELECT metadata, delete_timestamp FROM account_stat").Scan(&metadataValue, &deleteTimestamp); err != nil {
		if common.IsCorruptDBError(err) {
			return fmt.Errorf("Failed to UpdateMetadata SELECT: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
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
	if _, err = tx.Exec("UPDATE account_stat SET metadata=?", metastr); err != nil {
		if common.IsCorruptDBError(err) {
			return fmt.Errorf("Failed to UpdateMetadata UPDATE: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
		}
		return err
	}
	defer db.invalidateCache()
	if err := tx.Commit(); err != nil {
		if common.IsCorruptDBError(err) {
			return fmt.Errorf("Failed to UpdateMetadata Commit: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
		}
		return err
	}
	return nil
}

// MergeSyncTable updates the account's current incoming_sync table records.
func (db *sqliteAccount) MergeSyncTable(records []*SyncRecord) error {
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
				return fmt.Errorf("Failed to MergeSyncTable UPDATE & INSERT: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
			}
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		if common.IsCorruptDBError(err) {
			return fmt.Errorf("Failed to MergeSyncTable Commit: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
		}
		return err
	}
	return nil
}

// CleanupTombstones removes any expired tombstoned objects or metadata.
func (db *sqliteAccount) CleanupTombstones(reclaimAge int64) error {
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
	if _, err = tx.Exec("DELETE FROM container WHERE deleted=1 AND delete_timestamp < ?", reclaimTimestamp); err != nil {
		if common.IsCorruptDBError(err) {
			return fmt.Errorf("Failed to CleanupTombstones DELETE: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
		}
		return err
	}
	var metastr string
	if err := tx.QueryRow("SELECT metadata FROM account_stat").Scan(&metastr); err != nil {
		if common.IsCorruptDBError(err) {
			return fmt.Errorf("Failed to CleanupTombstones SELECT: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
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
		} else if _, err = tx.Exec("UPDATE account_stat SET metadata = ?", string(mb)); err != nil {
			if common.IsCorruptDBError(err) {
				return fmt.Errorf("Failed to CleanupTombstones UPDATE: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
			}
			return err
		}
	}
	defer db.invalidateCache()
	if err := tx.Commit(); err != nil {
		if common.IsCorruptDBError(err) {
			return fmt.Errorf("Failed to CleanupTombstones Commit: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
		}
		return err
	}
	return nil
}

// SyncTable returns the account's current incoming_sync table, and also includes the current account's id and max row as an entry.
func (db *sqliteAccount) SyncTable() ([]*SyncRecord, error) {
	if err := db.connect(); err != nil {
		return nil, err
	}
	records := []*SyncRecord{}
	rows, err := db.Query(`SELECT sync_point, remote_id FROM incoming_sync
						   WHERE remote_id NOT IN (SELECT id FROM account_stat)
						   UNION
						   SELECT maxrowid.max AS sync_point, account_stat.id AS remote_id
						   FROM account_stat, maxrowid`)
	if err != nil {
		if common.IsCorruptDBError(err) {
			return nil, fmt.Errorf("Failed to SyncTable SELECT: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
		}
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		rec := &SyncRecord{}
		if err := rows.Scan(&rec.SyncPoint, &rec.RemoteID); err != nil {
			if common.IsCorruptDBError(err) {
				return nil, fmt.Errorf("Failed to SyncTable Scan: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
			}
			return nil, err
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		if common.IsCorruptDBError(err) {
			return nil, fmt.Errorf("Failed to SyncTable Err: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
		}
		return nil, err
	}
	return records, nil
}

// SyncRemoteData compares a remote account's info to the local info and updates any necessary replication bookkeeping, returning the current account's info.
func (db *sqliteAccount) SyncRemoteData(maxRow int64, hash, id, createdAt, putTimestamp, deleteTimestamp, metadata string) (*AccountInfo, error) {
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
	if err := tx.QueryRow("SELECT hash, metadata, delete_timestamp FROM account_stat").Scan(&localHash, &localMeta, &localDeleteTimestamp); err != nil {
		if common.IsCorruptDBError(err) {
			return nil, fmt.Errorf("Failed to SyncRemoteData SELECT: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
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
	if _, err = tx.Exec(`UPDATE account_stat SET created_at=MIN(?, created_at), put_timestamp=MAX(?, put_timestamp),
	  					 delete_timestamp=MAX(?, delete_timestamp), metadata=?`,
		createdAt, putTimestamp, deleteTimestamp, metastr); err != nil {
		if common.IsCorruptDBError(err) {
			return nil, fmt.Errorf("Failed to SyncRemoteData UPDATE: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
		}
		return nil, err
	}
	if err := tx.QueryRow("SELECT IFNULL(MAX(sync_point), -1) FROM incoming_sync WHERE remote_id = ?", id).Scan(&localPoint); err != nil {
		if common.IsCorruptDBError(err) {
			return nil, fmt.Errorf("Failed to SyncRemoteData SELECT2: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
		}
		return nil, err
	}
	if localHash == hash && maxRow > localPoint {
		localPoint = maxRow
		if _, err = tx.Exec("INSERT OR REPLACE INTO incoming_sync (remote_id, sync_point) VALUES (?, ?)", id, localPoint); err != nil {
			if common.IsCorruptDBError(err) {
				return nil, fmt.Errorf("Failed to SyncRemoteData INSERT: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
			}
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		if common.IsCorruptDBError(err) {
			return nil, fmt.Errorf("Failed to SyncRemoteData Commit: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
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

// OpenDatabaseFile blocks updates and opens the underlying database file for reading, so it can be uploaded to a remote server.
func (db *sqliteAccount) OpenDatabaseFile() (*os.File, func(), error) {
	if err := db.connect(); err != nil {
		return nil, nil, err
	}
	var fp *os.File
	if _, err := db.Exec(`
		PRAGMA locking_mode = EXCLUSIVE;      -- grab and hold a shared lock
		SELECT 1 FROM account_stat LIMIT 1;   -- it doesn't actually lock until you hit the database
		PRAGMA wal_checkpoint(TRUNCATE);      -- truncate the wal file, if it exists`,
	); err != nil {
		if common.IsCorruptDBError(err) {
			return nil, nil, fmt.Errorf("Failed to OpenDatabaseFile: %v; %v", err, common.QuarantineDir(path.Dir(db.accountFile), 4, "accounts"))
		}
		return nil, nil, fmt.Errorf("Error locking database%s: %v", db.accountFile, err)
	}
	cleanup := func() {
		db.Exec(`
			PRAGMA locking_mode = NORMAL;         -- release shared lock
			SELECT 1 FROM account_stat LIMIT 1; -- actually release shared lock
		`)
		if fp != nil {
			fp.Close()
		}
	}
	// make sure there aren't any journals lying around
	if stat, err := os.Stat(db.accountFile + "-wal"); err == nil && stat.Size() != 0 {
		cleanup()
		return nil, nil, fmt.Errorf("Stubborn wal file still exists: %s", db.accountFile)
	}
	if stat, err := os.Stat(db.accountFile + "-journal"); err == nil && stat.Size() != 0 {
		cleanup()
		return nil, nil, fmt.Errorf("Stubborn journal file still exists: %s", db.accountFile)
	}
	fp, err := os.Open(db.accountFile)
	if err != nil {
		fp = nil
		cleanup()
		return nil, nil, fmt.Errorf("Error opening %s: %v", db.accountFile, err)
	}
	return fp, cleanup, nil
}

// ID returns the account's ring hash as a unique identifier for it.
func (db *sqliteAccount) ID() string {
	return db.ringhash
}

// RingHash returns the account's ring hash as a string.
func (db *sqliteAccount) RingHash() string {
	return db.ringhash
}

func (db *sqliteAccount) flushAlreadyLocked() error {
	if err := db.connect(); err != nil {
		return err
	}
	if stat, err := os.Stat(db.accountFile + ".pending"); err != nil || stat.Size() == 0 {
		return nil
	}
	contents, err := ioutil.ReadFile(db.accountFile + ".pending")
	if err != nil || len(contents) == 0 {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var records []*ContainerRecord
	for _, base64ed := range strings.Split(string(contents), ":") {
		if len(base64ed) < 1 {
			continue
		}
		pickled, err := base64.StdEncoding.DecodeString(base64ed)
		if err != nil {
			continue
		}
		r, err := pickle.PickleLoads(pickled)
		if err != nil {
			continue
		}
		record, ok := r.([]interface{})
		if !ok || len(record) < 7 {
			return fmt.Errorf("Invalid commit pending record")
		}
		casts := make([]bool, 7)
		var deleted, spi int64
		rec := &ContainerRecord{}
		rec.Name, casts[0] = record[0].(string)
		rec.PutTimestamp, casts[1] = record[1].(string)
		rec.DeleteTimestamp, casts[2] = record[2].(string)
		rec.ObjectCount, casts[3] = int64MaybeStringified(record[3])
		rec.BytesUsed, casts[4] = int64MaybeStringified(record[4])
		deleted, casts[5] = record[5].(int64)
		rec.Deleted = int(deleted)
		spi, casts[6] = int64MaybeStringified(record[6])
		rec.StoragePolicyIndex = int(spi)
		for i := 0; i < 7; i++ {
			if !casts[i] {
				return fmt.Errorf("Invalid commit pending record")
			}
		}
		records = append(records, rec)
	}
	err = db.MergeItems(records, "")
	if err == nil {
		err = os.Truncate(db.accountFile+".pending", 0)
	}
	return err
}

func int64MaybeStringified(i interface{}) (int64, bool) {
	if a, ok := i.(int64); ok {
		return a, true
	} else if b, ok := i.(string); !ok {
		return 0, false
	} else {
		c, err := strconv.ParseInt(b, 10, 64)
		return c, err == nil
	}
}

func (db *sqliteAccount) flush() error {
	lock, err := fs.LockPath(filepath.Dir(db.accountFile), 10*time.Second)
	if err != nil {
		return err
	}
	defer lock.Close()
	return db.flushAlreadyLocked()
}

// PutContainer adds a container to the account, by way of pending file.
func (db *sqliteAccount) PutContainer(name string, putTimestamp string, deleteTimestamp string, objectCount int64, bytesUsed int64, storagePolicyIndex int) error {
	lock, err := fs.LockPath(filepath.Dir(db.accountFile), 10*time.Second)
	if err != nil {
		return err
	}
	defer lock.Close()
	deleted := 0
	if deleteTimestamp > putTimestamp {
		deleted = 1
	}
	tuple := []interface{}{name, putTimestamp, deleteTimestamp, objectCount, bytesUsed, deleted, storagePolicyIndex}
	file, err := os.OpenFile(db.accountFile+".pending", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.WriteString(":" + base64.StdEncoding.EncodeToString(pickle.PickleDumps(tuple))); err != nil {
		return err
	}
	if info, err := file.Stat(); err == nil && info.Size() > pendingCap {
		return db.flushAlreadyLocked()
	}
	return nil
}

// Close closes the underlying sqlite database connection.
func (db *sqliteAccount) Close() error {
	db.connectLock.Lock()
	defer db.connectLock.Unlock()
	return db.closeAlreadyLocked()
}

func (db *sqliteAccount) closeAlreadyLocked() error {
	if db.DB != nil {
		err := db.DB.Close()
		db.DB = nil
		return err
	}
	return nil
}

func sqliteCreateExistingAccount(db Account, putTimestamp string, newMetadata map[string][]string) (bool, error) {
	cdb, ok := db.(*sqliteAccount)
	if !ok {
		return false, errors.New("Unable to work with non-sqliteAccount")
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
	row := tx.QueryRow("SELECT put_timestamp, delete_timestamp, metadata FROM account_stat")
	if err := row.Scan(&cPutTimestamp, &cDeleteTimestamp, &cMetadata); err != nil {
		if common.IsCorruptDBError(err) {
			return false, fmt.Errorf("Failed to sqliteCreateExistingAccount SELECT: %v; %v", err, common.QuarantineDir(path.Dir(cdb.accountFile), 4, "accounts"))
		}
		return false, err
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
	if _, err := tx.Exec("UPDATE account_stat SET put_timestamp = ?, metadata = ?",
		putTimestamp, metastr); err != nil {
		if common.IsCorruptDBError(err) {
			return false, fmt.Errorf("Failed to sqliteCreateExistingAccount UPDATE: %v; %v", err, common.QuarantineDir(path.Dir(cdb.accountFile), 4, "accounts"))
		}
		return false, err
	}
	defer cdb.invalidateCache()
	if err := tx.Commit(); err != nil {
		if common.IsCorruptDBError(err) {
			return false, fmt.Errorf("Failed to sqliteCreateExistingAccount Commit: %v; %v", err, common.QuarantineDir(path.Dir(cdb.accountFile), 4, "accounts"))
		}
		return false, err
	}
	return (cDeleteTimestamp > cPutTimestamp && putTimestamp > cDeleteTimestamp), nil
}

func sqliteCreateAccount(accountFile string, account string, putTimestamp string, metadata map[string][]string) error {
	var serializedMetadata []byte
	var err error

	hashDir := filepath.Dir(accountFile)
	if err := os.MkdirAll(hashDir, 0755); err != nil {
		return err
	}
	lock, err := fs.LockPath(filepath.Dir(hashDir), 10*time.Second)
	if err != nil {
		return err
	}
	defer lock.Close()

	if fs.Exists(accountFile) {
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
	dbConn, err := sql.Open("sqlite3_account", "file:"+tempFile+"?psow=1&_txlock=immediate&mode=rwc")
	if err != nil {
		return err
	}
	defer dbConn.Close()
	tx, err := dbConn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(accountDBScript + policyStatTableScript + policyStatTriggerScript); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO account_stat (account, created_at, id, put_timestamp,
						  status_changed_at, metadata, container_count) VALUES (?, ?, ?, ?, ?, ?, 0)`,
		account, common.GetTimestamp(), common.UUID(), putTimestamp, putTimestamp, string(serializedMetadata)); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if err := os.Rename(tempFile, accountFile); err != nil {
		return err
	}
	return nil
}

func sqliteOpenAccount(accountFile string) (ReplicableAccount, error) {
	if !fs.Exists(accountFile) {
		return nil, ErrorNoSuchAccount
	}
	db := &sqliteAccount{
		accountFile:         accountFile,
		hasDeletedNameIndex: false,
		ringhash:            filepath.Base(filepath.Dir(accountFile)),
	}
	return db, nil
}
