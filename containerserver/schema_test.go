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
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// This is the schema from when we first released swift.
var superCruftySchema = `
	CREATE TABLE outgoing_sync (
			remote_id TEXT UNIQUE,
			sync_point INTEGER,
			updated_at TEXT DEFAULT 0
		);
	CREATE TABLE incoming_sync (
			remote_id TEXT UNIQUE,
			sync_point INTEGER,
			updated_at TEXT DEFAULT 0
		);
	CREATE TRIGGER outgoing_sync_insert AFTER INSERT ON outgoing_sync
		BEGIN
			UPDATE outgoing_sync
			SET updated_at = STRFTIME('%s', 'NOW')
			WHERE ROWID = new.ROWID;
		END;
	CREATE TRIGGER outgoing_sync_update AFTER UPDATE ON outgoing_sync
		BEGIN
			UPDATE outgoing_sync
			SET updated_at = STRFTIME('%s', 'NOW')
			WHERE ROWID = new.ROWID;
		END;
	CREATE TRIGGER incoming_sync_insert AFTER INSERT ON incoming_sync
		BEGIN
			UPDATE incoming_sync
			SET updated_at = STRFTIME('%s', 'NOW')
			WHERE ROWID = new.ROWID;
		END;
	CREATE TRIGGER incoming_sync_update AFTER UPDATE ON incoming_sync
		BEGIN
			UPDATE incoming_sync
			SET updated_at = STRFTIME('%s', 'NOW')
			WHERE ROWID = new.ROWID;
		END;
	CREATE TABLE object (
			ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT UNIQUE,
			created_at TEXT,
			size INTEGER,
			content_type TEXT,
			etag TEXT,
			deleted INTEGER DEFAULT 0
		);
	CREATE INDEX ix_object_deleted ON object (deleted);
	CREATE TRIGGER object_insert AFTER INSERT ON object
		BEGIN
			UPDATE container_stat
			SET object_count = object_count + (1 - new.deleted),
				bytes_used = bytes_used + new.size,
				hash = chexor(hash, new.name, new.created_at);
		END;
	CREATE TRIGGER object_update BEFORE UPDATE ON object
		BEGIN
			SELECT RAISE(FAIL, 'UPDATE not allowed; DELETE and INSERT');
		END;
	CREATE TRIGGER object_delete AFTER DELETE ON object
		BEGIN
			UPDATE container_stat
			SET object_count = object_count - (1 - old.deleted),
				bytes_used = bytes_used - old.size,
				hash = chexor(hash, old.name, old.created_at);
		END;
	CREATE TABLE container_stat (
			account TEXT,
			container TEXT,
			created_at TEXT,
			put_timestamp TEXT DEFAULT '0',
			delete_timestamp TEXT DEFAULT '0',
			object_count INTEGER,
			bytes_used INTEGER,
			reported_put_timestamp TEXT DEFAULT '0',
			reported_delete_timestamp TEXT DEFAULT '0',
			reported_object_count INTEGER DEFAULT 0,
			reported_bytes_used INTEGER DEFAULT 0,
			hash TEXT default '00000000000000000000000000000000',
			id TEXT,
			status TEXT DEFAULT '',
			status_changed_at TEXT DEFAULT '0'
		);
	INSERT INTO container_stat (account, container, created_at, object_count, bytes_used, id) VALUES ("a", "c", '0', 0, 0, "some database");`

func createCruftyDatabase() (string, func(), error) {
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		return "", nil, err
	}
	dbFile := filepath.Join(dir, "db.db")
	dbConn, err := sql.Open("sqlite3_hummingbird", dbFile)
	if err != nil {
		return "", nil, err
	}
	if _, err := dbConn.Exec(superCruftySchema); err != nil {
		return "", nil, err
	}
	cleanup := func() {
		os.RemoveAll(dir)
	}
	return dbFile, cleanup, nil
}

func TestMigrateCruftyDatabase(t *testing.T) {
	dbFile, cleanup, err := createCruftyDatabase()
	require.Nil(t, err)
	defer cleanup()
	c, err := sqliteOpenContainer(dbFile)
	require.Nil(t, err)
	db, ok := c.(*sqliteContainer)
	require.True(t, ok)
	require.False(t, db.hasDeletedNameIndex)
	count := 0
	require.Nil(t, db.connect())
	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE name in ('policy_stat', 'object_insert_policy_stat', 'container_info', 'container_stat_update')").Scan(&count)
	require.Nil(t, err)
	require.Equal(t, 4, count)
	require.Nil(t, db.QueryRow("SELECT COUNT(*) FROM container_info").Scan(&count))
	require.Nil(t, err)
	require.Equal(t, 1, count)
	require.Nil(t, db.QueryRow("SELECT COUNT(*) FROM policy_stat").Scan(&count))
	require.Nil(t, err)
	require.Equal(t, 1, count)
	info, err := db.GetInfo()
	require.Nil(t, err)
	require.Equal(t, "a", info.Account)
	require.Equal(t, "c", info.Container)
	require.Equal(t, "some database", info.ID)

	ensureColumnsExist := func(table string, columns []string) {
		rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
		require.Nil(t, err)
		columnNames := map[string]bool{}
		for rows.Next() {
			var columnName string
			var _x interface{}
			require.Nil(t, rows.Scan(&_x, &columnName, &_x, &_x, &_x, &_x))
			columnNames[columnName] = true
		}
		rows.Close()
		for _, column := range columns {
			require.True(t, columnNames[column])
		}
	}
	ensureColumnsExist("object", []string{"storage_policy_index"})
	ensureColumnsExist("container_stat", []string{"metadata", "x_container_sync_point1", "x_container_sync_point2"})
}
