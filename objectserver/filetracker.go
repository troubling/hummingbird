package objectserver

import (
	"bytes"
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"fmt"
	"hash"
	"os"
	"path"

	"github.com/gholt/kvt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/troubling/hummingbird/common/fs"
	"go.uber.org/zap"
)

// defaultDiskPartPower specifies the number of high bits to use to distribute
// file across databases. Currently nothing sets or overrides this but the code
// requires it be 1-8 (uses just the high byte of the hash; uses %02x in a few
// places; ...). We can make configurable, or auto-set, or whatever in the
// future if we need to.
var defaultDiskPartPower uint = 6

// newHasher is used to get the hashing algorithm for metadata checks (and
// possibly other checks). I know everybody hates MD5, but it's already in
// widespread use throughout Openstack Swift / Hummingbird and it's not like
// it's being used here for crypto.
var newHasher func() hash.Hash = md5.New

type fileTracker struct {
	path          string
	diskPartPower uint
	tempPath      string
	dbs           []*sql.DB
	logger        *zap.Logger
}

func newFileTracker(pth string, logger *zap.Logger) (*fileTracker, error) {
	ft := &fileTracker{
		path:          pth,
		tempPath:      path.Join(pth, "temp"),
		diskPartPower: defaultDiskPartPower,
		dbs:           make([]*sql.DB, 1<<defaultDiskPartPower),
		logger:        logger,
	}
	err := os.MkdirAll(ft.tempPath, 0700)
	if err != nil {
		return nil, err
	}
	for i := 0; i < 1<<ft.diskPartPower; i++ {
		err := os.MkdirAll(path.Join(ft.path, fmt.Sprintf("%02x", i)), 0700)
		if err != nil {
			return nil, err
		}
		ft.dbs[i], err = sql.Open("sqlite3", path.Join(ft.path, fmt.Sprintf("filetracker_%02x.sqlite3", i)))
		if err == nil {
			err = ft.init(i)
		}
		if err != nil {
			for j := 0; j < i; j++ {
				ft.dbs[j].Close()
			}
			return nil, err
		}
	}
	return ft, nil
}

func (ft *fileTracker) init(dbi int) error {
	db := ft.dbs[dbi]
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.Query(`
        SELECT name
        FROM sqlite_master
        WHERE name = 'files'
    `)
	if err != nil {
		return err
	}
	tableExists := rows.Next()
	rows.Close()
	if err = rows.Err(); err != nil {
		return err
	}
	if !tableExists {
		_, err = tx.Exec(`
            CREATE TABLE files (
                hash TEXT NOT NULL,
                shard INTEGER NOT NULL,
                timestamp INTEGER NOT NULL,
                metahash TEXT, -- NULLable because not everyone stores the metadata
                metadata TEXT,
                CONSTRAINT ix_files_hash PRIMARY KEY (hash, shard)
            );
            CREATE INDEX ix_files_hash_timestamp ON files (hash, shard, timestamp);
        `)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (ft *fileTracker) close() {
	for _, db := range ft.dbs {
		db.Close()
	}
}

func (ft *fileTracker) tempFile(hsh []byte, sizeHint int) (fs.AtomicFileWriter, error) {
	return fs.NewAtomicFileWriter(ft.tempPath, ft.wholeFileDir(hsh))
}

func (ft *fileTracker) commit(f fs.AtomicFileWriter, hsh []byte, shard int, timestamp int64, metahash []byte, metadata []byte) error {
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
		// If f.Save() was already called, this is a No-Op.
		f.Abandon()
	}()
	diskPart := hsh[0] >> (8 - ft.diskPartPower)
	db := ft.dbs[diskPart]
	var err error
	tx, err = db.Begin()
	if err != nil {
		return err
	}
	hshx := fmt.Sprintf("%032x", hsh)
	rows, err = tx.Query(`
        SELECT timestamp, metahash, metadata
        FROM files
        WHERE hash = ? AND shard = ?
        ORDER BY timestamp DESC
    `, hshx, shard)
	if err != nil {
		return err
	}
	var removeOlder string
	if !rows.Next() {
		rows.Close()
		if err = rows.Err(); err != nil {
			return err
		}
	} else {
		var dbTimestamp int64
		var dbMetahash []byte
		var dbMetadata []byte
		if err = rows.Scan(&dbTimestamp, &dbMetahash, &dbMetadata); err != nil {
			return err
		}
		if dbTimestamp >= timestamp {
			return nil
		}
		removeOlder = ft.wholeFilePath(hsh, shard, dbTimestamp)
		if !bytes.Equal(metahash, dbMetahash) {
			metastore := kvt.Store{}
			if err = json.Unmarshal(metadata, &metastore); err != nil {
				// We return this error because the caller gave us bad metadata.
				return err
			}
			dbMetastore := kvt.Store{}
			if err = json.Unmarshal(dbMetadata, &dbMetastore); err != nil {
				ft.logger.Error(
					"error decoding metadata from db; discarding",
					zap.Error(err),
					zap.String("hshx", hshx),
					zap.Int("shard", shard),
					zap.Int64("dbTimestamp", dbTimestamp),
					zap.Binary("dbMetahash", dbMetahash),
					zap.Binary("dbMetadata", dbMetadata),
				)
			} else {
				metastore.Absorb(dbMetastore)
				var newMetadata []byte
				if newMetadata, err = json.Marshal(metastore); err != nil {
					if _, err2 := json.Marshal(dbMetastore); err2 != nil {
						ft.logger.Error(
							"error reencoding metadata from db; discarding",
							zap.Error(err2),
							zap.String("hshx", hshx),
							zap.Int("shard", shard),
							zap.Int64("dbTimestamp", dbTimestamp),
							zap.Binary("dbMetahash", dbMetahash),
							zap.Binary("dbMetadata", dbMetadata),
							zap.Binary("metahash", metahash),
							zap.Binary("metadata", metadata),
						)
					} else {
						// We return this error because the caller (presumably)
						// gave us bad metadata.
						return err
					}
				} else {
					metahash = []byte(metastore.Hash())
					metadata = newMetadata
				}
			}
		}
	}
	if err = f.Save(ft.wholeFilePath(hsh, shard, timestamp)); err != nil {
		return err
	}
	if removeOlder == "" {
		_, err = tx.Exec(`
            INSERT INTO files (hash, shard, timestamp, metahash, metadata)
            VALUES (?, ?, ?, ?, ?)
        `, hshx, shard, timestamp, metahash, metadata)
	} else {
		_, err = tx.Exec(`
            UPDATE files
            SET timestamp = ?, metahash = ?, metadata = ?
            WHERE hash = ? AND shard = ?
        `, timestamp, metahash, metadata, hshx, shard)
	}
	if err == nil {
		err = tx.Commit()
	}
	if err == nil && removeOlder != "" {
		if err2 := os.Remove(removeOlder); err2 != nil {
			ft.logger.Error(
				"error removing older file",
				zap.Error(err2),
				zap.String("removeOlder", removeOlder),
			)
		}
	}
	return err
}

func (ft *fileTracker) wholeFileDir(hsh []byte) string {
	return path.Join(ft.path, fmt.Sprintf("%02x", hsh[0]>>(8-ft.diskPartPower)))
}

func (ft *fileTracker) wholeFilePath(hsh []byte, shard int, timestamp int64) string {
	return path.Join(ft.path, fmt.Sprintf("%02x/%032x.%02x.%019d", hsh[0]>>(8-ft.diskPartPower), hsh, shard, timestamp))
}

func (ft *fileTracker) lookup(hsh []byte, shard int) (timestamp int64, metahash []byte, metadata []byte, path string, err error) {
	diskPart := hsh[0] >> (8 - ft.diskPartPower)
	db := ft.dbs[diskPart]
	hshx := fmt.Sprintf("%032x", hsh)
	rows, err := db.Query(`
        SELECT timestamp, metahash, metadata
        FROM files
        WHERE hash = ? AND shard = ?
        ORDER BY timestamp DESC
    `, hshx, shard)
	if err != nil {
		return 0, nil, nil, "", err
	}
	if !rows.Next() {
		rows.Close()
		return 0, nil, nil, "", rows.Err()
	}
	if err = rows.Scan(&timestamp, &metahash, &metadata); err != nil {
		return 0, nil, nil, "", err
	}
	return timestamp, metahash, metadata, ft.wholeFilePath(hsh, shard, timestamp), nil
}

type fileTrackerItem struct {
	hash      []byte
	shard     int
	timestamp int64
	metahash  []byte
}

func (ft *fileTracker) list(startHash []byte, stopHash []byte) ([]*fileTrackerItem, error) {
	listing := []*fileTrackerItem{}
	startDiskPart := startHash[0] >> (8 - ft.diskPartPower)
	stopDiskPart := stopHash[0] >> (8 - ft.diskPartPower)
	if startDiskPart > stopDiskPart {
		return nil, fmt.Errorf("startHash greater than stopHash: %x > %x", startHash, stopHash)
	}
	startHashx := fmt.Sprintf("%x", startHash)
	stopHashx := fmt.Sprintf("%x", stopHash)
	for diskPart := startDiskPart; diskPart <= stopDiskPart; diskPart++ {
		db := ft.dbs[diskPart]
		rows, err := db.Query(`
            SELECT hash, shard, timestamp, metahash
            FROM files
            WHERE hash BETWEEN ? AND ?
        `, startHashx, stopHashx)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			item := &fileTrackerItem{}
			if err = rows.Scan(&item.hash, &item.shard, &item.timestamp, &item.metahash); err != nil {
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
