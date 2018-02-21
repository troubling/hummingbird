package objectserver

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"github.com/troubling/hummingbird/common/fs"
	"go.uber.org/zap"
)

const (
	shardAny = -1
)

// IndexDBItem is a single item returned by List.
type IndexDBItem struct {
	Hash      string
	Shard     int
	Timestamp int64
	Metahash  string `json:"-"`
	Nursery   bool
	Metabytes []byte `json:"-"`
	Deletion  bool
	Path      string
	ShardHash string
}

// IndexDB will track a set of objects.
//
// This is the "index.db" per disk. Right now it just handles whole objects,
// but eventually we'd like to add either slab support or direct database
// embedding for small objects. But, those details should be transparent from
// users of a IndexDB.
//
// This is different from the standard Swift full replica object tracking in
// that the directory structure is much shallower, there are a configurable
// number of databases per drive instead of a ton of hashes.pkl files, and the
// version tracking / consolidation is much simpler.
//
// The IndexDB stores the newest object contents it knows about and
// discards any older ones, like the standard Swift's .data files. It does not
// have .meta files at all, and certainly not stacked to infinity .meta files.
// Instead the metadata is stored in the database as JSON.
//
// A given IndexDB may not even store any metadata, such as in an EC
// system, with just "key" IndexDBs storing the metadata.
type IndexDB struct {
	dbpath        string
	filepath      string
	RingPartPower uint // GLH: Temp exported for fakelist
	dbPartPower   uint
	subdirs       int
	temppath      string
	dbs           []*sql.DB
	logger        *zap.Logger
}

// NewIndexDB creates a IndexDB to manage a set of objects.
//
// The ringPartPower is defined by the ring in use, but should be greater than
// the dbPartPower. The dbPartPower will define how many
// databases are created (e.g. dbPartPower = 6 gives 64 databases). The
// subdirs value will define how many subdirectories are created where object
// content files are placed.
func NewIndexDB(dbpath, filepath, temppath string, ringPartPower, dbPartPower, subdirs int, logger *zap.Logger) (*IndexDB, error) {
	if ringPartPower <= dbPartPower {
		return nil, fmt.Errorf("ringPartPower must be greater than dbPartPower: %d is not greater than %d", ringPartPower, dbPartPower)
	}
	if subdirs < 1 || subdirs > 256 {
		return nil, fmt.Errorf("subdirs must be between 1 and 256; it was %d", subdirs)
	}
	ot := &IndexDB{
		dbpath:        dbpath,
		filepath:      filepath,
		temppath:      temppath,
		RingPartPower: uint(ringPartPower),
		dbPartPower:   uint(dbPartPower),
		subdirs:       subdirs,
		dbs:           make([]*sql.DB, 1<<uint(dbPartPower)),
		logger:        logger,
	}
	err := os.MkdirAll(ot.dbpath, 0700)
	if err != nil {
		return nil, err
	}
	err = os.MkdirAll(ot.filepath, 0700)
	if err != nil {
		return nil, err
	}
	err = os.MkdirAll(ot.temppath, 0700)
	if err != nil {
		return nil, err
	}
	for i := 0; i < 1<<ot.dbPartPower; i++ {
		ot.dbs[i], err = sql.Open("sqlite3", "file:"+path.Join(ot.dbpath, fmt.Sprintf("index.db.%02x", i))+"?psow=1&_txlock=immediate&mode=rwc")
		if err == nil {
			ot.dbs[i].SetMaxOpenConns(2)
			ot.dbs[i].SetMaxIdleConns(2)
			err = ot.init(i)
		}
		if err != nil {
			for j := 0; j < i; j++ {
				ot.dbs[j].Close()
			}
			return nil, err
		}
	}
	for i := 0; i < ot.subdirs; i++ {
		err := os.MkdirAll(path.Join(ot.filepath, fmt.Sprintf("index.db.dir.%02x", i)), 0700)
		if err != nil {
			for j := 0; j < 1<<ot.dbPartPower; j++ {
				ot.dbs[j].Close()
			}
			return nil, err
		}
	}
	return ot, nil
}

func (ot *IndexDB) init(dbi int) error {
	db := ot.dbs[dbi]
	if _, err := db.Exec(`
        PRAGMA synchronous = NORMAL;
        PRAGMA cache_size = -4096;
        PRAGMA temp_store = MEMORY;
        PRAGMA journal_mode = WAL;
        PRAGMA busy_timeout = 25000;
    `, nil); err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec(`
		CREATE TABLE IF NOT EXISTS objects (
			hash TEXT NOT NULL,
			shard INTEGER NOT NULL,
			timestamp INTEGER NOT NULL,
			nursery BOOLEAN NOT NULL,
			deletion BOOLEAN NOT NULL,
			metahash TEXT, -- NULLable because not everyone stores the metadata
			metadata TEXT, -- NULLable because not everyone stores the metadata
			shardhash TEXT, -- NULLable because not every object is a shard
			CONSTRAINT ix_objects_hash_shard_timestamp PRIMARY KEY (hash, shard, timestamp, nursery)
		) WITHOUT ROWID;
	`)
	if err != nil {
		return err
	}
	if _, err = tx.Exec("CREATE INDEX IF NOT EXISTS ix_nursery_items ON objects (nursery) WHERE nursery = 1"); err != nil {
		return err
	}
	return tx.Commit()
}

// Close closes all the underlying databases for the IndexDB; you should
// discard the IndexDB instance after this call.
func (ot *IndexDB) Close() {
	for _, db := range ot.dbs {
		db.Close()
	}
}

// TempFile returns a temporary file to write to for eventually adding the
// hash:shard to the IndexDB with Commit; may return (nil, nil) if there
// is already a newer or equal timestamp in place for the hash:shard.
func (ot *IndexDB) TempFile(hsh string, shard int, timestamp int64, sizeHint int64, nursery bool) (fs.AtomicFileWriter, error) {
	item, err := ot.Lookup(hsh, shard, false)
	if err != nil {
		return nil, err
	}
	if item != nil && item.Timestamp >= timestamp {
		if item.Timestamp > timestamp || !item.Nursery || nursery {
			return nil, nil
		}
	}
	dir, err := ot.wholeObjectDir(hsh)
	if err != nil {
		return nil, err
	}
	return fs.NewAtomicFileWriter(ot.temppath, dir)
}

// Commit moves the temporary file (from TempFile) into place and records its
// information in the database. It may actually discard it completely if there
// is already a newer object information in place for the hash:shard.
//
// Shard is mostly for EC type policies; just use 0 if you're using a full
// replica policy.
//
// Timestamp is the timestamp for the object contents, not necessarily the
// metadata.
func (ot *IndexDB) Commit(f fs.AtomicFileWriter, hsh string, shard int, timestamp int64, deletion bool, metahash string, metadata []byte, nursery bool, shardhash string) error {
	hsh, _, dbPart, _, err := ot.validateHash(hsh)
	if err != nil {
		return err
	}
	var tx *sql.Tx
	var rows *sql.Rows
	// Single defer so we can control the order of the tear down.
	defer func() {
		if rows != nil {
			rows.Close()
		}
		if tx != nil {
			// If tx.Commit() was already called, this is a No-Op.
			tx.Rollback()
		}
		if f != nil {
			// If f.Save() was already called, this is a No-Op.
			f.Abandon()
		}
	}()
	db := ot.dbs[dbPart]
	tx, err = db.Begin()
	if err != nil {
		return err
	}
	rows, err = tx.Query(`
        SELECT timestamp, metahash, metadata
        FROM objects
        WHERE hash = ? AND shard = ? AND nursery = ?
        ORDER BY timestamp DESC
    `, hsh, shard, nursery)
	if err != nil {
		return err
	}
	var dbWholeObjectPath string
	var dbTimestamp int64
	if !rows.Next() {
		rows.Close()
		if err = rows.Err(); err != nil {
			return err
		}
		if f == nil && !deletion {
			return fmt.Errorf("no existing entry for %s and so refusing to store just a metadata update", hsh)
		}
	} else {
		var dbMetahash string
		var dbMetadata []byte
		if err = rows.Scan(&dbTimestamp, &dbMetahash, &dbMetadata); err != nil {
			return err
		}
		if f == nil && !deletion {
			// We keep the original timestamp if just committing new metadata.
			timestamp = dbTimestamp
		}
		dbWholeObjectPath, err = ot.WholeObjectPath(hsh, shard, dbTimestamp, nursery)
		if err != nil {
			return err
		}
		if metahash == dbMetahash && ((f == nil && !deletion) || dbTimestamp > timestamp) {
			return nil
		}
		if metahash != dbMetahash {
			metadataMap := map[string]string{}
			if err = json.Unmarshal(metadata, &metadataMap); err != nil {
				// We return this error because the caller gave us bad metadata.
				return err
			}
			dbMetadataMap := map[string]string{}
			if err = json.Unmarshal(dbMetadata, &dbMetadataMap); err != nil {
				ot.logger.Error(
					"error decoding metadata from db; discarding",
					zap.Error(err),
					zap.String("hsh", hsh),
					zap.Int("shard", shard),
					zap.Int64("dbTimestamp", dbTimestamp),
					zap.String("dbMetahash", dbMetahash),
					zap.Binary("dbMetadata", dbMetadata),
				)
			} else {
				if f == nil {
					delete(metadataMap, "Content-Length")
					delete(metadataMap, "ETag")
				}
				metadataMap = MetadataMerge(metadataMap, dbMetadataMap)
				var newMetadata []byte
				if newMetadata, err = json.Marshal(metadataMap); err != nil {
					if _, err2 := json.Marshal(dbMetadataMap); err2 != nil {
						ot.logger.Error(
							"error reencoding metadata from db; discarding",
							zap.Error(err2),
							zap.String("hsh", hsh),
							zap.Int("shard", shard),
							zap.Int64("dbTimestamp", dbTimestamp),
							zap.String("dbMetahash", dbMetahash),
							zap.Binary("dbMetadata", dbMetadata),
							zap.String("metahash", metahash),
							zap.Binary("metadata", metadata),
						)
					} else {
						// We return this error because the caller (presumably)
						// gave us bad metadata.
						return err
					}
				} else {
					metahash = MetadataHash(metadataMap)
					metadata = newMetadata
				}
			}
		}
	}
	rows.Close()
	var pth string
	pth, err = ot.WholeObjectPath(hsh, shard, timestamp, nursery)
	if err != nil {
		return err
	}
	if f != nil {
		if err = f.Save(pth); err != nil {
			return err
		}
	}
	if dbWholeObjectPath == "" {
		_, err = tx.Exec(`
            INSERT INTO objects (hash, shard, timestamp, deletion, metahash, metadata, nursery, shardhash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        `, hsh, shard, timestamp, deletion, metahash, metadata, nursery, shardhash)
	} else {
		_, err = tx.Exec(`
            UPDATE objects
            SET timestamp = ?, deletion = ?, metahash = ?, metadata = ?, nursery = ?, shardhash = ?
            WHERE hash = ? AND shard = ? AND nursery = ?
        `, timestamp, deletion, metahash, metadata, nursery, shardhash, hsh, shard, nursery)
	}
	if err == nil {
		err = tx.Commit()
	}
	if err == nil && dbWholeObjectPath != "" && (f != nil || deletion) && timestamp > dbTimestamp {
		if err2 := os.Remove(dbWholeObjectPath); err2 != nil {
			ot.logger.Error(
				"error removing older file",
				zap.Error(err2),
				zap.String("dbWholeObjectPath", dbWholeObjectPath),
			)
		}
	}
	return err
}

func (ot *IndexDB) wholeObjectDir(hsh string) (string, error) {
	hsh, _, _, dirNm, err := ot.validateHash(hsh)
	if err != nil {
		return "", err
	}
	return path.Join(ot.filepath, fmt.Sprintf("index.db.dir.%02x", dirNm)), nil
}

func (ot *IndexDB) WholeObjectPath(hsh string, shard int, timestamp int64, nursery bool) (string, error) {
	hsh, _, _, dirNm, err := ot.validateHash(hsh)
	if err != nil {
		return "", err
	}
	if nursery {
		return path.Join(ot.filepath, fmt.Sprintf("index.db.dir.%02x/%s.n.%019d", dirNm, hsh, timestamp)), nil
	}
	return path.Join(ot.filepath, fmt.Sprintf("index.db.dir.%02x/%s.%02x.%019d", dirNm, hsh, shard, timestamp)), nil
}

// Remove removes an entry from the database and its backing disk file.
func (ot *IndexDB) Remove(hsh string, shard int, timestamp int64, nursery bool) error {
	hsh, _, dbPart, _, err := ot.validateHash(hsh)
	if err != nil {
		return err
	}
	db := ot.dbs[dbPart]
	res, err := db.Exec(`
        DELETE
		FROM objects
        WHERE hash = ? AND shard = ? AND timestamp = ? AND nursery = ?
    `, hsh, shard, timestamp, nursery)
	if err != nil {
		return err
	}
	if af, err := res.RowsAffected(); err == nil && af > 0 {
		path, err := ot.WholeObjectPath(hsh, shard, timestamp, nursery)
		if err != nil {
			return err
		}
		os.Remove(path)
	}
	return nil
}

// Lookup returns the stored information for the hsh and shard.
// Will return (nil, error) if there is an error. (nil, nil) if not found
func (ot *IndexDB) Lookup(hsh string, shard int, justStable bool) (*IndexDBItem, error) {
	var err error
	hsh, _, dbPart, _, err := ot.validateHash(hsh)
	if err != nil {
		return nil, err
	}
	db := ot.dbs[dbPart]
	var rows *sql.Rows
	if justStable {
		rows, err = db.Query(`
			SELECT timestamp, deletion, metahash, metadata, nursery, shard, shardhash
			FROM objects
			WHERE hash = ? AND shard = ? AND nursery = 0
		`, hsh, shard)
	} else if shard == shardAny {
		rows, err = db.Query(`
			SELECT timestamp, deletion, metahash, metadata, nursery, shard, shardhash
			FROM objects
			WHERE hash = ? AND metadata IS NOT NULL
			ORDER BY nursery DESC, shard ASC
		`, hsh)
	} else {
		rows, err = db.Query(`
			SELECT timestamp, deletion, metahash, metadata, nursery, shard, shardhash
			FROM objects
			WHERE hash = ? AND shard = ?
			ORDER BY nursery DESC
		`, hsh, shard)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, rows.Err()
	}
	item := &IndexDBItem{Hash: hsh}
	if err = rows.Scan(&item.Timestamp, &item.Deletion, &item.Metahash,
		&item.Metabytes, &item.Nursery, &item.Shard, &item.ShardHash); err != nil {
		return nil, err
	}
	item.Path, err = ot.WholeObjectPath(item.Hash, item.Shard, item.Timestamp, item.Nursery)
	return item, err
}

// ListNursery lists all objects that are in the nursery.
func (ot *IndexDB) ListNursery() ([]*IndexDBItem, error) {
	listing := []*IndexDBItem{}
	for _, db := range ot.dbs {
		if err := func() error {
			rows, err := db.Query(`
				SELECT hash, shard, timestamp, deletion, metahash, metadata, nursery
				FROM objects
				WHERE nursery = 1`)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				item := &IndexDBItem{}
				if err = rows.Scan(&item.Hash, &item.Shard, &item.Timestamp, &item.Deletion,
					&item.Metahash, &item.Metabytes, &item.Nursery); err != nil {
					return err
				}
				item.Path, err = ot.WholeObjectPath(item.Hash, item.Shard, item.Timestamp, item.Nursery)
				if err != nil {
					return err
				}
				listing = append(listing, item)
			}
			return rows.Err()
		}(); err != nil {
			return listing, err
		}
	}
	return listing, nil
}

// List returns the items for the ringPart given.
//
// This is for replication, auditing, that sort of thing.
func (ot *IndexDB) List(startHash, stopHash, marker string, limit int) ([]*IndexDBItem, error) {
	if startHash == "" {
		startHash = "00000000000000000000000000000000"
	}
	if stopHash == "" {
		stopHash = "ffffffffffffffffffffffffffffffff"
	}
	_, _, startDBPart, _, err := ot.validateHash(startHash)
	if err != nil {
		return nil, err
	}
	_, _, stopDBPart, _, err := ot.validateHash(stopHash)
	if err != nil {
		return nil, err
	}
	listing := []*IndexDBItem{}
	for dbPart := startDBPart; dbPart <= stopDBPart; dbPart++ {
		db := ot.dbs[dbPart]
		var rows *sql.Rows
		if limit > 0 {
			rows, err = db.Query(`
				SELECT hash, shard, timestamp, deletion, metahash, metadata, nursery, shardhash
			FROM objects
			WHERE hash BETWEEN ? AND ? AND hash > ?
			ORDER BY hash
			LIMIT ?
		    `, startHash, stopHash, marker, limit)
		} else {
			rows, err = db.Query(`
				SELECT hash, shard, timestamp, deletion, metahash, metadata, nursery, shardhash
			FROM objects
			WHERE hash BETWEEN ? AND ? AND hash > ?
			ORDER BY hash
		    `, startHash, stopHash, marker)
		}
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			item := &IndexDBItem{}
			if err = rows.Scan(&item.Hash, &item.Shard, &item.Timestamp, &item.Deletion,
				&item.Metahash, &item.Metabytes, &item.Nursery, &item.ShardHash); err != nil {
				return listing, err
			}
			listing = append(listing, item)
		}
		if err = rows.Err(); err != nil {
			return listing, err
		}
	}
	return listing, nil
}

func (ot *IndexDB) validateHash(hsh string) (hshOut string, ringPart, dbPart, dirNm int, err error) {
	hsh = strings.ToLower(hsh)
	if len(hsh) != 32 {
		return "", 0, 0, 0, fmt.Errorf("invalid hash %q; length was %d not 32", hsh, len(hsh))
	}
	hashBytes, err := hex.DecodeString(hsh)
	if err != nil {
		return "", 0, 0, 0, fmt.Errorf("invalid hash %q; decoding error: %s", hsh, err)
	}
	upper := uint64(hashBytes[0])<<24 | uint64(hashBytes[1])<<16 | uint64(hashBytes[2])<<8 | uint64(hashBytes[3])
	return hsh, int(upper >> (32 - ot.RingPartPower)), int(hashBytes[0] >> (8 - ot.dbPartPower)), int(hashBytes[15]) % ot.subdirs, nil
}

func (ot *IndexDB) RingPartRange(ringPart int) (string, string) {
	start := uint64(ringPart << (64 - ot.RingPartPower))
	stop := uint64((ringPart+1)<<(64-ot.RingPartPower)) - 1
	return fmt.Sprintf("%016x0000000000000000", start), fmt.Sprintf("%016xffffffffffffffff", stop)
}
