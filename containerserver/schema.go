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
	"strings"
)

const (
	policyStatTableScript = `
		CREATE TABLE policy_stat (
			storage_policy_index INTEGER PRIMARY KEY,
			object_count INTEGER DEFAULT 0,
			bytes_used INTEGER DEFAULT 0
		);`

	policyStatTriggerScript = `
		CREATE TRIGGER object_insert_policy_stat AFTER INSERT ON object
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
		END;
		CREATE TRIGGER object_delete_policy_stat AFTER DELETE ON object
		BEGIN
			UPDATE policy_stat
			SET object_count = object_count - (1 - old.deleted),
				bytes_used = bytes_used - old.size
			WHERE storage_policy_index = old.storage_policy_index;
			UPDATE container_info
			SET hash = chexor(hash, old.name, old.created_at);
		END;`

	containerInfoTableScript = `
		CREATE TABLE container_info (
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
			metadata TEXT DEFAULT '{}',
			x_container_sync_point1 INTEGER DEFAULT -1,
			x_container_sync_point2 INTEGER DEFAULT -1,
			storage_policy_index INTEGER DEFAULT 0,
			reconciler_sync_point INTEGER DEFAULT -1
		);`

	containerStatViewScript = `
		CREATE VIEW container_stat
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
		ON ci.storage_policy_index = ps.storage_policy_index;
		CREATE TRIGGER container_stat_update
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
		END;`

	objectTableScript = `
		CREATE TABLE object (
				ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
				name TEXT,
				created_at TEXT,
				size INTEGER,
				content_type TEXT,
				etag TEXT,
				deleted INTEGER DEFAULT 0,
				storage_policy_index INTEGER DEFAULT 0,
				expires INTEGER DEFAULT NULL
			);
		CREATE INDEX ix_object_deleted_name ON object (deleted, name);
		CREATE INDEX ix_object_expires ON object(expires) WHERE expires IS NOT NULL;
		CREATE TRIGGER object_update BEFORE UPDATE ON object
			BEGIN
				SELECT RAISE(FAIL, 'UPDATE not allowed; DELETE and INSERT');
			END;`

	syncTableScript = `	
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
			END;`

	policyMigrateColumns = `account, container, created_at, put_timestamp, delete_timestamp, reported_put_timestamp,
		reported_object_count, reported_bytes_used, hash, id, status, status_changed_at, metadata,
		x_container_sync_point1, x_container_sync_point2`

	policyMigrateScript = policyStatTableScript +
		"INSERT INTO policy_stat (storage_policy_index, object_count, bytes_used) SELECT 0, object_count, bytes_used FROM container_stat;" +
		"ALTER TABLE object ADD COLUMN storage_policy_index INTEGER DEFAULT 0;" +
		"DROP TRIGGER object_insert;" +
		"DROP TRIGGER object_delete;" +
		policyStatTriggerScript +
		containerInfoTableScript +
		"INSERT INTO container_info (" + policyMigrateColumns + ") SELECT " + policyMigrateColumns + " FROM container_stat;" +
		"DROP TABLE IF EXISTS container_stat;" +
		containerStatViewScript

	syncPointMigrateScript = `
		ALTER TABLE container_stat ADD COLUMN x_container_sync_point1 INTEGER DEFAULT -1;
		ALTER TABLE container_stat ADD COLUMN x_container_sync_point2 INTEGER DEFAULT -1;`

	metadataMigrateScript = "ALTER TABLE container_stat ADD COLUMN metadata DEFAULT '{}';"

	pragmaScript = `
		PRAGMA synchronous = NORMAL;
		PRAGMA cache_size = -4096;
		PRAGMA temp_store = MEMORY;
		PRAGMA journal_mode = WAL;
		PRAGMA busy_timeout = 25000;`

	// There's no real reason that adding a column with a partial index on non-default values would
	// require a table scan, but I can't find any way to tell sqlite not to do it that isn't dark magic.
	xExpireMigrateScript = `
		ALTER TABLE object ADD COLUMN expires INTEGER DEFAULT NULL;
		CREATE INDEX ix_object_expires ON object(expires) WHERE expires IS NOT NULL;`
)

func schemaMigrate(db *sql.DB) (bool, error) {
	hasDeletedNameIndex := false
	hasSyncPoints := false
	hasMetadata := false
	hasPolicyStat := false
	hasExpireColumn := false

	tx, err := db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	// We just pull the schema out of sqlite_master and look at it to get the current state of the database.
	rows, err := tx.Query("SELECT name, sql FROM sqlite_master WHERE name in ('policy_stat', 'ix_object_deleted_name', 'container_stat')")
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
		} else if name == "ix_object_deleted_name" {
			hasDeletedNameIndex = true
		} else if name == "container_stat" {
			hasSyncPoints = strings.Contains(sql, "x_container_sync_point1")
			hasMetadata = strings.Contains(sql, "metadata")
		} else if name == "ix_object_expires" {
			hasExpireColumn = true
		}
	}
	if err := rows.Err(); err != nil {
		return hasDeletedNameIndex, err
	}
	if err := rows.Close(); err != nil {
		return hasDeletedNameIndex, err
	}

	if hasSyncPoints && hasMetadata && hasPolicyStat {
		return hasDeletedNameIndex, nil
	}

	if !hasSyncPoints {
		if _, err := tx.Exec(syncPointMigrateScript); err != nil {
			return hasDeletedNameIndex, fmt.Errorf("Adding sync_point columns: %v", err)
		}
	}
	if !hasMetadata {
		if _, err := tx.Exec(metadataMigrateScript); err != nil {
			return hasDeletedNameIndex, fmt.Errorf("Adding metadata column: %v", err)
		}
	}
	if !hasPolicyStat {
		if _, err = tx.Exec(policyMigrateScript); err != nil {
			return hasDeletedNameIndex, fmt.Errorf("Performing policy migration: %v", err)
		}
	}
	if !hasExpireColumn {
		if _, err = tx.Exec(xExpireMigrateScript); err != nil {
			return hasDeletedNameIndex, fmt.Errorf("Performing expires migration: %v", err)
		}
	}
	return hasDeletedNameIndex, tx.Commit()
}
