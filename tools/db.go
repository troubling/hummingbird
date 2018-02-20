package tools

import (
	"database/sql"
	"os"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/troubling/hummingbird/common/conf"
)

type dbInstance struct {
	db                     *sql.DB
	serviceErrorExpiration time.Duration
	deviceErrorExpiration  time.Duration
}

func newDB(serverconf *conf.Config, memoryDBID string) (*dbInstance, error) {
	// nil serverconf indicates test mode / in memory db ; memoryDBID will be
	// used in this case to differentiate dbs, such as for independent tests.
	db := &dbInstance{}
	var err error
	if serverconf != nil {
		db.serviceErrorExpiration = time.Duration(serverconf.GetInt("andrewd", "service_error_expiration", 3600)) * time.Second
		db.deviceErrorExpiration = time.Duration(serverconf.GetInt("andrewd", "device_error_expiration", 3600)) * time.Second
		sqlDir, ok := serverconf.Get("andrewd", "sql_dir")
		if !ok {
			sqlDir = serverconf.GetDefault("drive_watch", "sql_dir", "/var/local/hummingbird")
		}
		err = os.MkdirAll(sqlDir, 0755)
		if err != nil {
			return nil, err
		}
		db.db, err = sql.Open("sqlite3", filepath.Join(sqlDir, DB_NAME)+"??psow=1&_txlock=immediate&mode=rw")
		if err != nil {
			return nil, err
		}
	} else {
		db.serviceErrorExpiration = 3600 * time.Second
		db.deviceErrorExpiration = 3600 * time.Second
		if memoryDBID == "" {
			db.db, err = sql.Open("sqlite3", "file::memory:?cache=shared")
		} else {
			db.db, err = sql.Open("sqlite3", "file:"+memoryDBID+"?mode=memory&cache=shared")
		}
		if err != nil {
			return nil, err
		}
	}
	db.db.SetMaxOpenConns(1)
	_, err = db.db.Exec(`
        CREATE TABLE IF NOT EXISTS replication_queue (
            create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            rtype TEXT NOT NULL,        -- account, container, object
            policy INTEGER NOT NULL,    -- only used with object
            partition INTEGER NOT NULL, -- the partition number to replicate
            reason TEXT NOT NULL,       -- ring, dispersion, quarantine
            from_device INTEGER,        -- device id in ring to replicate from, NULL or < 0 = any
            to_device INTEGER           -- device id in ring to replicate to, NULL or < 0 = all
        );

        CREATE TABLE IF NOT EXISTS service_error (
            create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            rtype TEXT NOT NULL,        -- account, container, object
            policy INTEGER NOT NULL,    -- only used with object
            service TEXT NOT NULL,      -- ip:port of service erroring
            count INTEGER NOT NULL      -- count of errors since last success
        );

        CREATE TABLE IF NOT EXISTS device_error (
            create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            rtype TEXT NOT NULL,        -- account, container, object
            policy INTEGER NOT NULL,    -- only used with object
            device INTEGER NOT NULL,    -- device id in ring of device erroring
            count INTEGER NOT NULL      -- count of errors since last success
        );

        CREATE TABLE IF NOT EXISTS process_pass (
            process TEXT NOT NULL,              -- dispersion populate, dispersion scan, quarantine repair, ...
            rtype TEXT NOT NULL,                -- account, container, object
            policy INTEGER NOT NULL,            -- only used with object
            start_date TIMESTAMP DEFAULT 0,     -- when the scan last started, 0 = never ran
            progress_date TIMESTAMP DEFAULT 0,  -- when the progress was last updated, 0 = never updated
            progress TEXT,                      -- depends on the process
            complete_date TIMESTAMP DEFAULT 0   -- when the scan list completed, 0 = is running or never ran
        );
    `)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (db *dbInstance) queuePartitionReplication(typ string, policy int, partition uint64, reason string, fromDeviceID, toDeviceID int) error {
	var tx *sql.Tx
	var rows *sql.Rows
	var err error
	defer func() {
		if rows != nil {
			rows.Close()
		}
		if tx != nil {
			tx.Rollback()
		}
	}()
	tx, err = db.db.Begin()
	if err != nil {
		return err
	}
	rows, err = tx.Query(`
        SELECT 1 FROM replication_queue
        WHERE rtype = ?
          AND policy = ?
          AND partition = ?
          AND reason = ?
          AND from_device = ?
          AND to_device = ?
    `, typ, policy, partition, reason, fromDeviceID, toDeviceID)
	if err != nil {
		return err
	}
	if rows.Next() { // entry already
		return nil
	}
	rows.Close()
	rows = nil
	_, err = tx.Exec(`
        INSERT INTO replication_queue
        (rtype, policy, partition, reason, from_device, to_device)
        VALUES (?, ?, ?, ?, ?, ?)
    `, typ, policy, partition, reason, fromDeviceID, toDeviceID)
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	tx = nil
	return nil
}

type queuedReplication struct {
	time         time.Time
	typ          string
	policy       int
	partition    int
	reason       string
	fromDeviceID int
	toDeviceID   int
}

// queuedReplications returns the queued replications for the ring type
// (account, container, object) and policy index, filtered optionally by the
// reason. Use an empty string for the reason if you want all entries. Entries
// will be sorted by oldest queued to newest.
func (db *dbInstance) queuedReplications(typ string, policy int, reason string) ([]*queuedReplication, error) {
	var qrs []*queuedReplication
	var rows *sql.Rows
	var err error
	defer func() {
		if rows != nil {
			rows.Close()
		}
	}()
	if reason == "" {
		rows, err = db.db.Query(`
            SELECT create_date, partition, reason, from_device, to_device
            FROM replication_queue
            WHERE rtype = ?
              AND policy = ?
            ORDER BY create_date
        `, typ, policy)
	} else {
		rows, err = db.db.Query(`
            SELECT create_date, partition, reason, from_device, to_device
            FROM replication_queue
            WHERE rtype = ?
              AND policy = ?
              AND reason = ?
            ORDER BY create_date
        `, typ, policy, reason)
	}
	if err != nil {
		return qrs, err
	}
	for rows.Next() {
		qr := &queuedReplication{typ: typ, policy: policy}
		if err = rows.Scan(&qr.time, &qr.partition, &qr.reason, &qr.fromDeviceID, &qr.toDeviceID); err != nil {
			return qrs, err
		}
		qrs = append(qrs, qr)
	}
	return qrs, nil
}

func (db *dbInstance) incrementServiceErrorCount(typ string, policy int, service string) error {
	var tx *sql.Tx
	var rows *sql.Rows
	var err error
	defer func() {
		if rows != nil {
			rows.Close()
		}
		if tx != nil {
			tx.Rollback()
		}
	}()
	tx, err = db.db.Begin()
	if err != nil {
		return err
	}
	rows, err = tx.Query(`
        SELECT create_date, update_date, count FROM service_error
        WHERE rtype = ?
          AND policy = ?
          AND service = ?
    `, typ, policy, service)
	if err != nil {
		return err
	}
	if rows.Next() { // entry already
		var createDate time.Time
		var updateDate time.Time
		var count int
		if err = rows.Scan(&createDate, &updateDate, &count); err != nil {
			return err
		}
		rows.Close()
		rows = nil
		if time.Since(updateDate) > db.serviceErrorExpiration {
			createDate = time.Now()
			count = 1
		} else {
			count++
		}
		updateDate = time.Now()
		if _, err = tx.Exec(`
            UPDATE service_error
            SET create_date = ?,
                update_date = ?,
                count = ?
            WHERE rtype = ?
              AND policy = ?
              AND service = ?
        `, createDate, updateDate, count, typ, policy, service); err != nil {
			return err
		}
		if err = tx.Commit(); err != nil {
			return err
		}
		tx = nil
		return nil
	}
	rows.Close()
	rows = nil
	if _, err = tx.Exec(`
        INSERT INTO service_error
        (rtype, policy, service, count)
        VALUES (?, ?, ?, ?, 1)
    `, typ, policy, service); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	tx = nil
	return nil
}

func (db *dbInstance) countOfServicesWithErrors(typ string, policy int) (int, error) {
	var rows *sql.Rows
	var err error
	var count int
	defer func() {
		if rows != nil {
			rows.Close()
		}
	}()
	rows, err = db.db.Query(`
        SELECT count(*) FROM service_error
        WHERE rtype = ?
          AND policy = ?
    `, typ, policy)
	if err != nil {
		return count, err
	}
	if rows.Next() {
		if err = rows.Scan(&count); err != nil {
			return count, err
		}
	}
	return count, nil
}

func (db *dbInstance) incrementDeviceErrorCount(typ string, policy int, deviceID int) error {
	var tx *sql.Tx
	var rows *sql.Rows
	var err error
	defer func() {
		if rows != nil {
			rows.Close()
		}
		if tx != nil {
			tx.Rollback()
		}
	}()
	tx, err = db.db.Begin()
	if err != nil {
		return err
	}
	rows, err = tx.Query(`
        SELECT create_date, update_date, count FROM device_error
        WHERE rtype = ?
          AND policy = ?
          AND device = ?
    `, typ, policy, deviceID)
	if err != nil {
		return err
	}
	if rows.Next() { // entry already
		var createDate time.Time
		var updateDate time.Time
		var count int
		if err = rows.Scan(&createDate, &updateDate, &count); err != nil {
			return err
		}
		rows.Close()
		rows = nil
		if time.Since(updateDate) > db.deviceErrorExpiration {
			createDate = time.Now()
			count = 1
		} else {
			count++
		}
		updateDate = time.Now()
		if _, err = tx.Exec(`
            UPDATE device_error
            SET create_date = ?,
                update_date = ?,
                count = ?
            WHERE rtype = ?
              AND policy = ?
              AND device = ?
        `, createDate, updateDate, count, typ, policy, deviceID); err != nil {
			return err
		}
		if err = tx.Commit(); err != nil {
			return err
		}
		tx = nil
		return nil
	}
	rows.Close()
	rows = nil
	if _, err = tx.Exec(`
        INSERT INTO device_error
        (rtype, policy, device, count)
        VALUES (?, ?, ?, ?, 1)
    `, typ, policy, deviceID); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	tx = nil
	return nil
}

func (db *dbInstance) countOfDevicesWithErrors(typ string, policy int) (int, error) {
	var rows *sql.Rows
	var err error
	var count int
	defer func() {
		if rows != nil {
			rows.Close()
		}
	}()
	rows, err = db.db.Query(`
        SELECT count(*) FROM device_error
        WHERE rtype = ?
          AND policy = ?
    `, typ, policy)
	if err != nil {
		return count, err
	}
	if rows.Next() {
		if err = rows.Scan(&count); err != nil {
			return count, err
		}
	}
	return count, nil
}

func (db *dbInstance) startProcessPass(process, typ string, policy int) error {
	var tx *sql.Tx
	var rows *sql.Rows
	var err error
	defer func() {
		if rows != nil {
			rows.Close()
		}
		if tx != nil {
			tx.Rollback()
		}
	}()
	tx, err = db.db.Begin()
	if err != nil {
		return err
	}
	rows, err = tx.Query(`
        SELECT 1 FROM process_pass
        WHERE process = ?
          AND rtype = ?
          AND policy = ?
    `, process, typ, policy)
	if err != nil {
		return err
	}
	if rows.Next() { // entry already
		rows.Close()
		rows = nil
		if _, err = tx.Exec(`
            UPDATE process_pass
            SET start_date = ?,
                progress_date = 0,
                progress = "",
                complete_date = 0
            WHERE process = ?
              AND rtype = ?
              AND policy = ?
        `, time.Now(), process, typ, policy); err != nil {
			return err
		}
		if err = tx.Commit(); err != nil {
			return err
		}
		tx = nil
		return nil
	}
	rows.Close()
	rows = nil
	if _, err = tx.Exec(`
        INSERT INTO process_pass
        (process, rtype, policy, start_date, progress_date, progress, complete_date)
        VALUES (?, ?, ?, ?, 0, "", 0)
    `, process, typ, policy, time.Now()); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	tx = nil
	return nil
}

func (db *dbInstance) progressProcessPass(process, typ string, policy int, progress string) error {
	_, err := db.db.Exec(`
        UPDATE process_pass
        SET progress_date = ?,
            progress = ?
        WHERE process = ?
          AND rtype = ?
          AND policy = ?
    `, time.Now(), progress, process, typ, policy)
	return err
}

func (db *dbInstance) completeProcessPass(process, typ string, policy int) error {
	_, err := db.db.Exec(`
        UPDATE process_pass
        SET complete_date = ?
        WHERE process = ?
          AND rtype = ?
          AND policy = ?
    `, time.Now(), process, typ, policy)
	return err
}

// processPass returns start_date, progress_date, progress, and complete_date.
func (db *dbInstance) processPass(process, typ string, policy int) (time.Time, time.Time, string, time.Time, error) {
	var rows *sql.Rows
	var err error
	var start time.Time
	var progress time.Time
	var progressText string
	var complete time.Time
	defer func() {
		if rows != nil {
			rows.Close()
		}
	}()
	if rows, err = db.db.Query(`
        SELECT start_date, progress_date, progress, complete_date
        FROM process_pass
        WHERE process = ?
          AND rtype = ?
          AND policy = ?
    `, process, typ, policy); err != nil {
		return start, progress, progressText, complete, err
	}
	if rows.Next() {
		err = rows.Scan(&start, &progress, &progressText, &complete)
	}
	if start.UnixNano() == 0 {
		start = time.Time{}
	}
	if complete.UnixNano() == 0 {
		complete = time.Time{}
	}
	return start, progress, progressText, complete, err
}

type processPassData struct {
	process      string
	rtype        string
	policy       int
	startDate    time.Time
	progressDate time.Time
	progress     string
	completeDate time.Time
}

func (db *dbInstance) processPasses() ([]*processPassData, error) {
	var rows *sql.Rows
	var err error
	var data []*processPassData
	defer func() {
		if rows != nil {
			rows.Close()
		}
	}()
	if rows, err = db.db.Query(`
        SELECT process, rtype, policy, start_date, progress_date, progress, complete_date
        FROM process_pass
    `); err != nil {
		return data, err
	}
	for rows.Next() {
		ppd := &processPassData{}
		if err = rows.Scan(&ppd.process, &ppd.rtype, &ppd.policy, &ppd.startDate, &ppd.progressDate, &ppd.progress, &ppd.completeDate); err != nil {
			return data, err
		}
		if ppd.startDate.UnixNano() == 0 {
			ppd.startDate = time.Time{}
		}
		if ppd.progressDate.UnixNano() == 0 {
			ppd.progressDate = time.Time{}
		}
		if ppd.completeDate.UnixNano() == 0 {
			ppd.completeDate = time.Time{}
		}
		data = append(data, ppd)
	}
	return data, nil
}
