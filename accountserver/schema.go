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
	"database/sql"
	"fmt"
	"strings"
)

const (
	accountDBScript = `
		CREATE TABLE outgoing_sync (
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

		CREATE TABLE incoming_sync (
			remote_id TEXT UNIQUE,
			sync_point INTEGER,
			updated_at TEXT DEFAULT 0
		);
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

		CREATE TABLE container (
			ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT,
			put_timestamp TEXT,
			delete_timestamp TEXT,
			object_count INTEGER,
			bytes_used INTEGER,
			deleted INTEGER DEFAULT 0,
			storage_policy_index INTEGER DEFAULT 0
		);
		CREATE INDEX ix_container_deleted_name ON container (deleted, name);

		CREATE TABLE account_stat (
			account TEXT,
			created_at TEXT,
			put_timestamp TEXT DEFAULT '0',
			delete_timestamp TEXT DEFAULT '0',
			container_count INTEGER,
			object_count INTEGER DEFAULT 0,
			bytes_used INTEGER DEFAULT 0,
			hash TEXT default '00000000000000000000000000000000',
			id TEXT,
			status TEXT DEFAULT '',
			status_changed_at TEXT DEFAULT '0',
			metadata TEXT DEFAULT ''
		);

		CREATE TRIGGER container_update BEFORE UPDATE ON container
			BEGIN
				SELECT RAISE(FAIL, 'UPDATE not allowed; DELETE and INSERT');
			END;
		CREATE TRIGGER container_insert AFTER INSERT ON container
			BEGIN
				UPDATE account_stat
				SET container_count = container_count + (1 - new.deleted),
					object_count = object_count + new.object_count,
					bytes_used = bytes_used + new.bytes_used,
					hash = chexor(hash, new.name,
								  new.put_timestamp || '-' ||
								  new.delete_timestamp || '-' ||
								  new.object_count || '-' || new.bytes_used);
			END;
		CREATE TRIGGER container_delete AFTER DELETE ON container
			BEGIN
				UPDATE account_stat
				SET container_count = container_count - (1 - old.deleted),
					object_count = object_count - old.object_count,
					bytes_used = bytes_used - old.bytes_used,
					hash = chexor(hash, old.name,
								  old.put_timestamp || '-' ||
								  old.delete_timestamp || '-' ||
								  old.object_count || '-' || old.bytes_used);
			END;
		`

	policyStatTableScript = `
		CREATE TABLE policy_stat (
			storage_policy_index INTEGER PRIMARY KEY,
			container_count INTEGER DEFAULT 0,
			object_count INTEGER DEFAULT 0,
			bytes_used INTEGER DEFAULT 0
		);
		INSERT OR IGNORE INTO policy_stat (storage_policy_index, container_count, object_count, bytes_used)
			SELECT 0, container_count, object_count, bytes_used FROM account_stat WHERE container_count > 0;
		`

	policyStatTriggerScript = `
		CREATE TRIGGER container_insert_ps AFTER INSERT ON container
			BEGIN
				INSERT OR IGNORE INTO policy_stat
					(storage_policy_index, container_count, object_count, bytes_used)
					VALUES (new.storage_policy_index, 0, 0, 0);
				UPDATE policy_stat
				SET container_count = container_count + (1 - new.deleted),
					object_count = object_count + new.object_count,
					bytes_used = bytes_used + new.bytes_used
				WHERE storage_policy_index = new.storage_policy_index;
			END;
		CREATE TRIGGER container_delete_ps AFTER DELETE ON container
			BEGIN
				UPDATE policy_stat
				SET container_count = container_count - (1 - old.deleted),
					object_count = object_count - old.object_count,
					bytes_used = bytes_used - old.bytes_used
				WHERE storage_policy_index = old.storage_policy_index;
			END;
	`

	pragmaScript = `
		PRAGMA synchronous = NORMAL;
		PRAGMA cache_size = -4096;
		PRAGMA temp_store = MEMORY;
		PRAGMA journal_mode = WAL;
		PRAGMA busy_timeout = 25000;`
)

func schemaMigrate(db *sql.DB) (bool, error) {
	hasMetadata := false
	hasPolicyStat := false
	hasContainerCount := false
	hasDeletedNameIndex := false

	tx, err := db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	// We just pull the schema out of sqlite_master and look at it to get the current state of the database.
	rows, err := tx.Query("SELECT name, sql FROM sqlite_master WHERE name in ('policy_stat', 'account_stat', 'policy_stat', 'ix_container_deleted_name')")
	if err != nil {
		return false, err
	}
	for rows.Next() {
		var name, sql string
		if err := rows.Scan(&name, &sql); err != nil {
			return false, err
		}
		if name == "policy_stat" {
			hasPolicyStat = true
			hasContainerCount = strings.Contains(sql, "container_count")
		} else if name == "account_stat" {
			hasMetadata = strings.Contains(sql, "metadata")
		} else if name == "ix_container_deleted_name" {
			hasDeletedNameIndex = true
		}
	}

	if hasMetadata && hasPolicyStat && hasContainerCount {
		return hasDeletedNameIndex, nil
	}

	if !hasMetadata {
		script := "ALTER TABLE account_stat ADD COLUMN metadata DEFAULT '{}';"
		if _, err := tx.Exec(script); err != nil {
			return hasDeletedNameIndex, fmt.Errorf("Adding metadata column: %v", err)
		}
	}
	if !hasPolicyStat {
		script := `
			ALTER TABLE container ADD COLUMN storage_policy_index INTEGER DEFAULT 0;
		` + policyStatTableScript + policyStatTriggerScript
		if _, err = tx.Exec(script); err != nil {
			return hasDeletedNameIndex, fmt.Errorf("Performing policy migration: %v", err)
		}
	} else if !hasContainerCount {
		script := `
			DROP TRIGGER container_delete_ps;
			DROP TRIGGER container_insert_ps;
			ALTER TABLE policy_stat ADD COLUMN container_count INTEGER DEFAULT 0;
		` + policyStatTriggerScript + `
			UPDATE policy_stat SET container_count = (SELECT container_count FROM account_stat)
				WHERE (SELECT COUNT(storage_policy_index) FROM policy_stat) <= 1;

			INSERT OR REPLACE INTO policy_stat (storage_policy_index, container_count, object_count, bytes_used)
				SELECT p.storage_policy_index, c.count, p.object_count, p.bytes_used
				FROM (
					SELECT storage_policy_index, COUNT(*) as count FROM container
					WHERE deleted = 0 GROUP BY storage_policy_index
				) c
				JOIN policy_stat p ON p.storage_policy_index = c.storage_policy_index
				WHERE NOT EXISTS(SELECT changes() as change FROM policy_stat WHERE change <> 0);
		`
		if _, err = tx.Exec(script); err != nil {
			return hasDeletedNameIndex, fmt.Errorf("Performing container-count migration: %v", err)
		}
	}
	return hasDeletedNameIndex, tx.Commit()
}
