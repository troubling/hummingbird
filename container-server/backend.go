package main

import (
	"database/sql"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"encoding/json"

	"github.com/mattn/go-sqlite3"
	"hummingbird/common"
)

const PENDING_CAP = 131072

type ContainerInfo struct {
	Account                 string
	Container               string
	CreatedAt               string
	PutTimestamp            string
	DeleteTimestamp         string
	StatusChangedAt         string
	ObjectCount             int64
	BytesUsed               int64
	ReportedPutTimestamp    string
	ReportedDeleteTimestamp string
	ReportedObjectCount     int64
	ReportedBytesUsed       int64
	Hash                    string
	Id                      string
	XContainerSyncPoint1    string
	XContainerSyncPoint2    string
	StoragePolicyIndex      int
	Metadata				string
}

type ObjectRecord struct {
	XMLName      xml.Name `xml:"object" json:"-"`
	Name         string   `xml:"name" json:"name"`
	LastModified string   `xml:"last_modified" json:"last_modified"`
	Size         int64    `xml:"bytes" json:"bytes"`
	ContentType  string   `xml:"content_type" json:"content_type"`
	Etag         string   `xml:"hash" json:"hash"`
}

type SubdirRecord struct {
	XMLName xml.Name `xml:"subdir" json:"-"`
	Name2   string   `xml:"name,attr" json:"-"`
	Name    string   `xml:"name" json:"subdir"`
}

type ExistingRecord struct {
	created_at string
	rowid      int64
}

type ContainerDB struct {
	*sql.DB
	containerFile string
	queryCache    map[string]*sql.Stmt
	InfoCache     *ContainerInfo
}

func InitializeDatabase() {
	sql.Register("sqlite3_chexor", &sqlite3.SQLiteDriver{ConnectHook: SQLiteSetup})
}

func (db *ContainerDB) Release() {
	for _, value := range db.queryCache {
		value.Close()
	}
	db.Close()
}

func (db *ContainerDB) CPrepare(query string, args ...interface{}) (*sql.Stmt, error) {
	stmt, ok := db.queryCache[query]
	if !ok {
		stmt, err := db.Prepare(query)
		if err != nil {
			return nil, err
		}
		db.queryCache[query] = stmt
		return stmt, nil
	}
	return stmt, nil
}

func (db *ContainerDB) CQuery(query string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := db.CPrepare(query)
	if err != nil {
		return nil, err
	}
	return stmt.Query(args...)
}

func (db *ContainerDB) CExec(query string, args ...interface{}) (sql.Result, error) {
	stmt, err := db.CPrepare(query)
	if err != nil {
		return nil, err
	}
	return stmt.Exec(args...)
}

func (db *ContainerDB) GetInfo() (*ContainerInfo, error) {
	if db.InfoCache != nil {
		return db.InfoCache, nil
	}
	info := ContainerInfo{}
	rows, err := db.CQuery(`SELECT account, container, created_at, put_timestamp,
					        delete_timestamp, status_changed_at,
					        object_count, bytes_used,
					        reported_put_timestamp, reported_delete_timestamp,
					        reported_object_count, reported_bytes_used, hash,
					        id, x_container_sync_point1, x_container_sync_point2,
					        storage_policy_index, metadata FROM container_stat`)
	defer rows.Close()
	if err == nil {
		for rows.Next() {
			rows.Scan(&info.Account, &info.Container, &info.CreatedAt, &info.PutTimestamp,
				&info.DeleteTimestamp, &info.StatusChangedAt, &info.ObjectCount,
				&info.BytesUsed, &info.ReportedPutTimestamp, &info.ReportedDeleteTimestamp,
				&info.ReportedObjectCount, &info.ReportedBytesUsed, &info.Hash,
				&info.Id, &info.XContainerSyncPoint1, &info.XContainerSyncPoint2,
				&info.StoragePolicyIndex, &info.Metadata)
			db.InfoCache = &info
			return &info, nil
		}
	}
	return nil, errors.New("whatevs")
}

func (db *ContainerDB) GetMetadata() map[string]string {
  	info, _ := db.GetInfo()
	var metadata_info interface{}
	metadata := make(map[string]string)
	err := json.Unmarshal([]byte(info.Metadata), &metadata_info)
	if err == nil {
		for key, value := range metadata_info.(map[string]interface{}) {
		  metadata[key] = value.([]interface{})[0].(string)
		}
	}
	return metadata
}

func (db *ContainerDB) IsDeleted() bool {
	info, _ := db.GetInfo()
	return info.DeleteTimestamp > info.PutTimestamp
}

func (db *ContainerDB) Delete(timestamp string) {
	db.CExec("UPDATE container_info SET delete_timestamp = ?", timestamp)
	if db.InfoCache != nil {
	  	db.InfoCache.DeleteTimestamp = timestamp
	}
}

func (db *ContainerDB) MergeItems(records []map[string]interface{}) error {
	tx, err := db.Begin()
	defer tx.Rollback()
	if err != nil {
		return err
	}
	names := make([]interface{}, len(records))
	for i, record := range records {
		names[i] = record["name"].(string)
	}
	existing := make(map[string]ExistingRecord)
	query := fmt.Sprintf("SELECT name, storage_policy_index, created_at, ROWID FROM object WHERE deleted IN (0, 1) AND name IN (%s)",
		strings.TrimRight(strings.Repeat("?,", len(records)), ","))
	rows, err := tx.Query(query, names...)
	if err != nil {
		return err
	}
	for rows.Next() {
		var name string
		var timestamp string
		var policy int
		var rowid int64
		rows.Scan(&name, &policy, &timestamp, &rowid)
		existing[fmt.Sprintf("%d/%s", policy, name)] = ExistingRecord{timestamp, rowid}
	}
	rows.Close()
	to_delete := make(map[int64]bool)
	to_add := make(map[string]map[string]interface{})
	for _, record := range records {
		if _, ok := record["storage_policy_index"]; !ok { /* legacy */
			record["storage_policy_index"] = 0
		}
		record_id := fmt.Sprintf("%d/%s", record["storage_policy_index"], record["name"])
		current, in_existing := existing[record_id]
		if !in_existing {
			to_add[record_id] = record
		} else if current.created_at < record["created_at"].(string) {
			to_delete[current.rowid] = true
			if already_in, ok := to_add[record_id]; !ok || record["created_at"].(string) > already_in["created_at"].(string) {
				to_add[record_id] = record
			}
		}
	}
	if len(to_delete) > 0 {
		txstmt, err := tx.Prepare("DELETE FROM object WHERE ROWID=?")
		defer txstmt.Close()
		if err != nil {
			return err
		}
		for key, _ := range to_delete {
			txstmt.Exec(key)
		}
	}
	if len(to_add) > 0 {
		txstmt, err := tx.Prepare("INSERT INTO object (name, created_at, size, content_type, etag, deleted, storage_policy_index) VALUES (?, ?, ?, ?, ?, ?, ?)")
		defer txstmt.Close()
		if err != nil {
			return err
		}
		for _, record := range to_add {
			txstmt.Exec(record["name"], record["created_at"], record["size"], record["content_type"], record["etag"], record["deleted"], record["storage_policy_index"])
		}
	}
	tx.Commit()
	return nil
}

func (db *ContainerDB) CommitPendingAlreadyLocked() error {
	contents, err := ioutil.ReadFile(db.containerFile + ".pending")
	if err != nil {
		return err
	}
	records := make([]map[string]interface{}, 0)
	for _, base64ed := range strings.Split(string(contents), ":") {
		if len(base64ed) < 1 {
			continue
		}
		pickled, err := base64.StdEncoding.DecodeString(base64ed)
		if err != nil {
			continue
		}
		record := hummingbird.PickleLoads(pickled).([]interface{})
		records = append(records, map[string]interface{}{
			"name":                 record[0],
			"created_at":           record[1],
			"size":                 record[2],
			"content_type":         record[3],
			"etag":                 record[4],
			"deleted":              record[5],
			"storage_policy_index": record[6],
		})
	}
	err = db.MergeItems(records)
	if err == nil {
		os.Truncate(db.containerFile+".pending", 0)
	}
	return err
}

func (db *ContainerDB) CommitPending() error {
	lock, err := hummingbird.LockParent(db.containerFile+".pending", 10)
	if err != nil {
		return err
	}
	defer lock.Close()
	return db.CommitPendingAlreadyLocked()
}

func (db *ContainerDB) ListObjectsSimple(limit int, marker *string, end_marker *string, prefix *string, storage_policy_index int) ([]interface{}, error) {
	results := make([]interface{}, 0)
	if marker == nil {
	  	if prefix != nil {
		  	marker = prefix
		} else {
		  empty_str := ""
		  marker = &empty_str
		 }
	}
	rows, err := db.CQuery("SELECT name, created_at, size, content_type, etag FROM object WHERE +deleted = 0 AND name > ? AND storage_policy_index = ? ORDER BY name ASC LIMIT ?",
		*marker, storage_policy_index, limit)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		record := ObjectRecord{}
		var createdAt string
		rows.Scan(&record.Name, &createdAt, &record.Size, &record.ContentType, &record.Etag)
		if (end_marker != nil && record.Name >= *end_marker) || (prefix != nil && !strings.HasPrefix(record.Name, *prefix)) {
			break
		}
		record.LastModified, _ = hummingbird.FormatTimestamp(createdAt)
		results = append(results, record)
	}
	rows.Close()
	return results, nil
}

func (db *ContainerDB) ListObjectsPrefixDelimiter(limit int, marker *string, end_marker *string, prefix *string, delimiter *string, storage_policy_index int) ([]interface{}, error) {
	results := make([]interface{}, 0)
	var start string
	if marker != nil {
		start = *marker
	} else {
		start = *prefix
	}
	if prefix == nil {
		empty_string := ""
		prefix = &empty_string
	}
	for len(results) < limit {
		rowcount := 0
		rows, err := db.CQuery("SELECT name, created_at, size, content_type, etag FROM object WHERE +deleted = 0 AND name > ? AND storage_policy_index = ? ORDER BY name ASC LIMIT ?",
			start, storage_policy_index, limit-len(results))
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			rowcount += 1
			record := ObjectRecord{}
			var createdAt string
			rows.Scan(&record.Name, &createdAt, &record.Size, &record.ContentType, &record.Etag)
			if (end_marker != nil && record.Name >= *end_marker) || (!strings.HasPrefix(record.Name, *prefix)) {
				rows.Close()
				return results, nil
			}
			after_prefix := record.Name[len(*prefix):]
			if strings.Contains(after_prefix, *delimiter) {
				parts := strings.SplitN(after_prefix, *delimiter, 2)
				subdir := SubdirRecord{Name: parts[0], Name2: parts[0]}
				results = append(results, subdir)
				start = *prefix + parts[0] + string(rune((*delimiter)[0]+1))
				break
			} else {
				record.LastModified, _ = hummingbird.FormatTimestamp(createdAt)
				results = append(results, record)
				start = record.Name
			}
		}
		rows.Close()
		if rowcount == 0 {
			return results, nil
		}
	}
	return results, nil
}

func (db *ContainerDB) ListObjectsPath(limit int, marker *string, end_marker *string, path *string, storage_policy_index int) ([]interface{}, error) {
	results := make([]interface{}, 0)
	var start string
	prefix := ""
	if *path != "" {
		prefix = strings.TrimRight(*path, "/") + "/"
	}
	if marker == nil {
		start = prefix
	} else {
		start = *marker
	}
	for len(results) < limit {
		rowcount := 0
		rows, err := db.CQuery("SELECT name, created_at, size, content_type, etag FROM object WHERE +deleted = 0 AND name > ? AND storage_policy_index = ? ORDER BY name ASC LIMIT ?",
			start, storage_policy_index, limit-len(results))
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			rowcount += 1
			record := ObjectRecord{}
			var createdAt string
			rows.Scan(&record.Name, &createdAt, &record.Size, &record.ContentType, &record.Etag)
			if (end_marker != nil && record.Name >= *end_marker) || (!strings.HasPrefix(record.Name, prefix)) {
				rows.Close()
				return results, nil
			}
			after_prefix := record.Name[len(prefix):]
			if record.Name == prefix || after_prefix[0] == '/' {
				continue
			}
			if strings.Contains(after_prefix, "/") {
				parts := strings.SplitN(after_prefix, "/", 2)
				record.Name = prefix + parts[0]
				if len(parts) > 1 {
					record.Name += "/"
				}
				results = append(results, record)
				start = prefix + parts[0] + "0"
				break
			} else {
				record.LastModified, _ = hummingbird.FormatTimestamp(createdAt)
				results = append(results, record)
				start = record.Name
			}
		}
		rows.Close()
		if rowcount == 0 {
			return results, nil
		}
	}
	return results, nil
}

func (db *ContainerDB) ListObjects(limit int, marker *string, end_marker *string, prefix *string, delimiter *string, path *string, storage_policy_index int) ([]interface{}, error) {
	db.CommitPending()

	if prefix == nil {
		delimiter = nil
	}

	if prefix != nil && marker != nil && !strings.HasPrefix(*marker, *prefix) {
		return nil, errors.New("WHAAAAAAT")
	}

	if delimiter != nil {
		return db.ListObjectsPrefixDelimiter(limit, marker, end_marker, prefix, delimiter, storage_policy_index)
	}

	if path != nil {
		return db.ListObjectsPath(limit, marker, end_marker, path, storage_policy_index)
	}

	return db.ListObjectsSimple(limit, marker, end_marker, prefix, storage_policy_index)
}

func (db *ContainerDB) UpdateMetadata(updates map[string][]string) error {
  	var new_metadata []byte
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.Query("SELECT metadata FROM container_stat")
	if err != nil {
		return err
	}
	defer rows.Close()
	if rows.Next() {
	  	var metadata_value string
		var metadata_if interface{}
		rows.Scan(&metadata_value)
		if metadata_value == "" {
			new_metadata, _ = json.Marshal(updates)
		} else {
			err := json.Unmarshal([]byte(metadata_value), &metadata_if)
			if err != nil {
				return err
			}
			metadata := metadata_if.(map[string]interface{})
			for key, newvalue := range updates {
			  	if newvalue[0] == "" {
				  	delete(metadata, key)
				} else if existing, ok := metadata[key]; ok {
					if existing.([]interface{})[1].(string) < newvalue[1] {
						metadata[key] = interface{}(newvalue)
					}
				} else {
					metadata[key] = interface{}(newvalue)
				}
			}
			new_metadata, _ = json.Marshal(metadata)
		}
		_, err := tx.Exec("UPDATE container_stat SET metadata=?", string(new_metadata))
		tx.Commit()
		db.InfoCache = nil
		return err
	}
	return errors.New("NO CONTAINER_STAT ENTRY")
}

func CreateDatabase(containerFile string, account string, container string, put_timestamp string, metadata map[string][]string, policy_index int) (bool, error) {
	containerDir := filepath.Dir(containerFile)
	serialized_metadata, _ := json.Marshal(metadata)
	if _, err := os.Stat(containerFile); err == nil {
		var current_put_timestamp string
		var current_delete_timestamp string
		dbConn, err := sql.Open("sqlite3_chexor", containerFile)
		if err != nil {
			return false, err
		}
		row := dbConn.QueryRow("SELECT put_timestamp, delete_timestamp FROM container_info")
		row.Scan(&current_put_timestamp, &current_delete_timestamp)
		dbConn.Exec("UPDATE container_info SET put_timestamp = ?", put_timestamp)
		dbConn.Close()
		return (current_delete_timestamp > current_put_timestamp && put_timestamp > current_delete_timestamp), nil
	}
	if err := os.MkdirAll(containerDir, 0770); err != nil {
		return false, err
	}
	dbConn, err := sql.Open("sqlite3_chexor", containerFile)
	if err != nil {
		return false, err
	}
	defer dbConn.Close()
	tx, err := dbConn.Begin()
	if err != nil {
		return false, errors.New("Failed to begin transaction.")
	}
	defer tx.Rollback()
	// I KNOW RIGHT
	dbConn.Exec(`CREATE TABLE container_info (
					  account TEXT,
					  container TEXT,
					  created_at TEXT,
					  put_timestamp TEXT DEFAULT '0',
					  delete_timestamp TEXT DEFAULT '0',
					  reported_put_timestamp TEXT DEFAULT '0',
					  reported_delete_timestamp TEXT DEFAULT '0',
					  reported_object_count INTEGER DEFAULT 0,
					  reported_bytes_used INTEGER DEFAULT 0,
					  hash TEXT default '00000000000000000000000000000000',
					  id TEXT,
					  status TEXT DEFAULT '',
					  status_changed_at TEXT DEFAULT '0',
					  metadata TEXT DEFAULT '',
					  x_container_sync_point1 INTEGER DEFAULT -1,
					  x_container_sync_point2 INTEGER DEFAULT -1,
					  storage_policy_index INTEGER DEFAULT 0,
					  reconciler_sync_point INTEGER DEFAULT -1)`)
	dbConn.Exec(`CREATE TABLE incoming_sync (
					  remote_id TEXT UNIQUE,
					  sync_point INTEGER,
					  updated_at TEXT DEFAULT 0)`)
	dbConn.Exec(`CREATE TABLE object (
					  ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
					  name TEXT,
					  created_at TEXT,
					  size INTEGER,
					  content_type TEXT,
					  etag TEXT,
					  deleted INTEGER DEFAULT 0,
					  storage_policy_index INTEGER DEFAULT 0)`)
	dbConn.Exec(`CREATE TABLE outgoing_sync (
					  remote_id TEXT UNIQUE,
					  sync_point INTEGER,
					  updated_at TEXT DEFAULT 0)`)
	dbConn.Exec(`CREATE TABLE policy_stat (
					  storage_policy_index INTEGER PRIMARY KEY,
					  object_count INTEGER DEFAULT 0,
					  bytes_used INTEGER DEFAULT 0)`)
	dbConn.Exec(`CREATE VIEW container_stat
					  AS SELECT ci.account, ci.container, ci.created_at,
						  ci.put_timestamp, ci.delete_timestamp,
						  ci.reported_put_timestamp, ci.reported_delete_timestamp,
						  ci.reported_object_count, ci.reported_bytes_used, ci.hash,
						  ci.id, ci.status, ci.status_changed_at, ci.metadata,
						  ci.x_container_sync_point1, ci.x_container_sync_point2,
						  ci.reconciler_sync_point,
						  ci.storage_policy_index,
						  coalesce(ps.object_count, 0) AS object_count,
						  coalesce(ps.bytes_used, 0) AS bytes_used
					  FROM container_info ci LEFT JOIN policy_stat ps
					  ON ci.storage_policy_index = ps.storage_policy_index`)
	dbConn.Exec(`CREATE INDEX ix_object_deleted_name ON object (deleted, name)`)
	dbConn.Exec(`CREATE TRIGGER container_stat_update
					  INSTEAD OF UPDATE ON container_stat
					  BEGIN
						  UPDATE container_info
						  SET account = NEW.account,
							  container = NEW.container,
							  created_at = NEW.created_at,
							  put_timestamp = NEW.put_timestamp,
							  delete_timestamp = NEW.delete_timestamp,
							  reported_put_timestamp = NEW.reported_put_timestamp,
							  reported_delete_timestamp = NEW.reported_delete_timestamp,
							  reported_object_count = NEW.reported_object_count,
							  reported_bytes_used = NEW.reported_bytes_used,
							  hash = NEW.hash,
							  id = NEW.id,
							  status = NEW.status,
							  status_changed_at = NEW.status_changed_at,
							  metadata = NEW.metadata,
							  x_container_sync_point1 = NEW.x_container_sync_point1,
							  x_container_sync_point2 = NEW.x_container_sync_point2,
							  storage_policy_index = NEW.storage_policy_index,
							  reconciler_sync_point = NEW.reconciler_sync_point;
					  END`)
	dbConn.Exec(`CREATE TRIGGER incoming_sync_insert AFTER INSERT ON incoming_sync
					  BEGIN
						  UPDATE incoming_sync
						  SET updated_at = STRFTIME('%s', 'NOW')
						  WHERE ROWID = new.ROWID;
					  END`)
	dbConn.Exec(`CREATE TRIGGER incoming_sync_update AFTER UPDATE ON incoming_sync
					  BEGIN
						  UPDATE incoming_sync
						  SET updated_at = STRFTIME('%s', 'NOW')
						  WHERE ROWID = new.ROWID;
					  END`)
	dbConn.Exec(`CREATE TRIGGER object_delete_policy_stat AFTER DELETE ON object
					  BEGIN
						  UPDATE policy_stat
						  SET object_count = object_count - (1 - old.deleted),
							  bytes_used = bytes_used - old.size
						  WHERE storage_policy_index = old.storage_policy_index;
						  UPDATE container_info
						  SET hash = chexor(hash, old.name, old.created_at);
					  END`)
	dbConn.Exec(`CREATE TRIGGER object_insert_policy_stat AFTER INSERT ON object
					  BEGIN
						  UPDATE policy_stat
						  SET object_count = object_count + (1 - new.deleted),
							  bytes_used = bytes_used + new.size
						  WHERE storage_policy_index = new.storage_policy_index;
						  INSERT INTO policy_stat (
							  storage_policy_index, object_count, bytes_used)
						  SELECT new.storage_policy_index,
								 (1 - new.deleted),
								 new.size
						  WHERE NOT EXISTS(
							  SELECT changes() as change
							  FROM policy_stat
							  WHERE change <> 0
						  );
						  UPDATE container_info
						  SET hash = chexor(hash, new.name, new.created_at);
					  END`)
	dbConn.Exec(`CREATE TRIGGER object_update BEFORE UPDATE ON object
					  BEGIN
						  SELECT RAISE(FAIL, 'UPDATE not allowed; DELETE and INSERT');
					  END`)
	dbConn.Exec(`CREATE TRIGGER outgoing_sync_insert AFTER INSERT ON outgoing_sync
					  BEGIN
						  UPDATE outgoing_sync
						  SET updated_at = STRFTIME('%s', 'NOW')
						  WHERE ROWID = new.ROWID;
					  END`)
	dbConn.Exec(`CREATE TRIGGER outgoing_sync_update AFTER UPDATE ON outgoing_sync
					  BEGIN
						  UPDATE outgoing_sync
						  SET updated_at = STRFTIME('%s', 'NOW')
						  WHERE ROWID = new.ROWID;
					  END`)
	dbConn.Exec(`INSERT INTO container_info (account, container, created_at, id, put_timestamp, status_changed_at, storage_policy_index, metadata) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
								account, container, hummingbird.GetTimestamp(), hummingbird.UUID(), put_timestamp, put_timestamp, policy_index, serialized_metadata)
	return true, tx.Commit()
}

func OpenDatabase(containerFile string) (*ContainerDB, error) {
	if _, err := os.Stat(containerFile); os.IsNotExist(err) {
		return nil, errors.New("Does not exist..")
	}
	dbConn, err := sql.Open("sqlite3_chexor", containerFile)
	if err != nil {
		return nil, errors.New("Failed to open.")
	}
	dbConn.Exec("PRAGMA synchronous = NORMAL")
	return &ContainerDB{dbConn, containerFile, make(map[string]*sql.Stmt), nil}, nil
}

func PutObject(containerFile string, name string, timestamp string, size int64, content_type string, etag string, deleted int, storage_policy_index int) error {
	lock, err := hummingbird.LockParent(containerFile, 10)
	if err != nil {
		return err
	}
	defer lock.Close()
	tuple := make([]interface{}, 7)
	tuple[0] = name
	tuple[1] = timestamp
	tuple[2] = size
	tuple[3] = content_type
	tuple[4] = etag
	tuple[5] = deleted
	tuple[6] = storage_policy_index
	file, err := os.OpenFile(containerFile+".pending", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		return err
	}
	defer file.Close()
	file.WriteString(":")
	file.WriteString(base64.StdEncoding.EncodeToString(hummingbird.PickleDumps(tuple)))
	if info, err := file.Stat(); err == nil && info.Size() > PENDING_CAP {
		db, err := OpenDatabase(containerFile)
		if err != nil {
			return err
		}
		db.CommitPendingAlreadyLocked()
		db.Release()
	}
	return nil
}
