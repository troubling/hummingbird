package tools

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

var dbTestNameCounter int64

func dbTestName(name string) string {
	return fmt.Sprintf("%s%d", name, atomic.AddInt64(&dbTestNameCounter, 1))
}

func TestIncrementServiceErrorCount(t *testing.T) {
	start := time.Now().Truncate(time.Second) // CURRENT_TIMESTAMP doesn't seem to have subseconds
	db, err := newDB(nil, dbTestName("TestIncrementServiceErrorCount"))
	if err != nil {
		t.Fatal(err)
	}
	if err = db.incrementServiceErrorCount("object", 2, "1.2.3.4:5678"); err != nil {
		t.Fatal(err)
	}
	rows, err := db.db.Query(`
        SELECT create_date, update_date, rtype, policy, service, count
        FROM service_error
    `)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("should be one row")
	}
	var createDate, updateDate time.Time
	var rtype, service string
	var policy, count int
	if err = rows.Scan(&createDate, &updateDate, &rtype, &policy, &service, &count); err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		t.Fatal("should only be one row")
	}
	now := time.Now()
	if createDate.Before(start) || createDate.After(now) {
		t.Fatal(createDate, start, now)
	}
	if !createDate.Equal(updateDate) {
		t.Fatal(createDate, updateDate)
	}
	if rtype != "object" {
		t.Fatal(rtype)
	}
	if policy != 2 {
		t.Fatal(policy)
	}
	if service != "1.2.3.4:5678" {
		t.Fatal(service)
	}
	if count != 1 {
		t.Fatal(count)
	}
	// again
	start2 := time.Now().Truncate(time.Second)
	if err = db.incrementServiceErrorCount("object", 2, "1.2.3.4:5678"); err != nil {
		t.Fatal(err)
	}
	rows, err = db.db.Query(`
        SELECT create_date, update_date, rtype, policy, service, count
        FROM service_error
    `)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("should be one row")
	}
	var createDate2 time.Time
	if err = rows.Scan(&createDate2, &updateDate, &rtype, &policy, &service, &count); err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		t.Fatal("should only be one row")
	}
	if !createDate.Equal(createDate2) {
		t.Fatal(createDate, createDate2)
	}
	now = time.Now()
	if updateDate.Before(start2) || updateDate.After(now) {
		t.Fatal(updateDate, start, now)
	}
	if rtype != "object" {
		t.Fatal(rtype)
	}
	if policy != 2 {
		t.Fatal(policy)
	}
	if service != "1.2.3.4:5678" {
		t.Fatal(service)
	}
	if count != 2 {
		t.Fatal(count)
	}
	// fudge dates to make the row older than the db.serviceErrorExpiration
	_, err = db.db.Exec(`
        UPDATE service_error
        SET create_date = ?,
            update_date = ?
    `, createDate2.Add(-db.serviceErrorExpiration-time.Second), updateDate.Add(-db.serviceErrorExpiration-time.Second))
	// increment should reset back to just 1
	start3 := time.Now().Truncate(time.Second)
	if err = db.incrementServiceErrorCount("object", 2, "1.2.3.4:5678"); err != nil {
		t.Fatal(err)
	}
	rows, err = db.db.Query(`
        SELECT create_date, update_date, rtype, policy, service, count
        FROM service_error
    `)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("should be one row")
	}
	var createDate3, updateDate3 time.Time
	if err = rows.Scan(&createDate3, &updateDate3, &rtype, &policy, &service, &count); err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		t.Fatal("should only be one row")
	}
	now = time.Now()
	if createDate3.Before(start3) || createDate3.After(now) {
		t.Fatal(createDate3, start3, now)
	}
	if !createDate3.Equal(updateDate3) {
		t.Fatal(createDate3, updateDate3)
	}
	if rtype != "object" {
		t.Fatal(rtype)
	}
	if policy != 2 {
		t.Fatal(policy)
	}
	if service != "1.2.3.4:5678" {
		t.Fatal(service)
	}
	if count != 1 {
		t.Fatal(count)
	}
}

func TestIncrementDeviceErrorCount(t *testing.T) {
	start := time.Now().Truncate(time.Second) // CURRENT_TIMESTAMP doesn't seem to have subseconds
	db, err := newDB(nil, dbTestName("TestIncrementDeviceErrorCount"))
	if err != nil {
		t.Fatal(err)
	}
	if err = db.incrementDeviceErrorCount("object", 2, 12345); err != nil {
		t.Fatal(err)
	}
	rows, err := db.db.Query(`
        SELECT create_date, update_date, rtype, policy, device, count
        FROM device_error
    `)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("should be one row")
	}
	var createDate, updateDate time.Time
	var rtype string
	var policy, device, count int
	if err = rows.Scan(&createDate, &updateDate, &rtype, &policy, &device, &count); err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		t.Fatal("should only be one row")
	}
	now := time.Now()
	if createDate.Before(start) || createDate.After(now) {
		t.Fatal(createDate, start, now)
	}
	if !createDate.Equal(updateDate) {
		t.Fatal(createDate, updateDate)
	}
	if rtype != "object" {
		t.Fatal(rtype)
	}
	if policy != 2 {
		t.Fatal(policy)
	}
	if device != 12345 {
		t.Fatal(device)
	}
	if count != 1 {
		t.Fatal(count)
	}
	// again
	start2 := time.Now().Truncate(time.Second)
	if err = db.incrementDeviceErrorCount("object", 2, 12345); err != nil {
		t.Fatal(err)
	}
	rows, err = db.db.Query(`
        SELECT create_date, update_date, rtype, policy, device, count
        FROM device_error
    `)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("should be one row")
	}
	var createDate2 time.Time
	if err = rows.Scan(&createDate2, &updateDate, &rtype, &policy, &device, &count); err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		t.Fatal("should only be one row")
	}
	if !createDate.Equal(createDate2) {
		t.Fatal(createDate, createDate2)
	}
	now = time.Now()
	if updateDate.Before(start2) || updateDate.After(now) {
		t.Fatal(updateDate, start, now)
	}
	if rtype != "object" {
		t.Fatal(rtype)
	}
	if policy != 2 {
		t.Fatal(policy)
	}
	if device != 12345 {
		t.Fatal(device)
	}
	if count != 2 {
		t.Fatal(count)
	}
	// fudge dates to make the row older than the db.deviceErrorExpiration
	_, err = db.db.Exec(`
        UPDATE device_error
        SET create_date = ?,
            update_date = ?
    `, createDate2.Add(-db.deviceErrorExpiration-time.Second), updateDate.Add(-db.deviceErrorExpiration-time.Second))
	// increment should reset back to just 1
	start3 := time.Now().Truncate(time.Second)
	if err = db.incrementDeviceErrorCount("object", 2, 12345); err != nil {
		t.Fatal(err)
	}
	rows, err = db.db.Query(`
        SELECT create_date, update_date, rtype, policy, device, count
        FROM device_error
    `)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("should be one row")
	}
	var createDate3, updateDate3 time.Time
	if err = rows.Scan(&createDate3, &updateDate3, &rtype, &policy, &device, &count); err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		t.Fatal("should only be one row")
	}
	now = time.Now()
	if createDate3.Before(start3) || createDate3.After(now) {
		t.Fatal(createDate3, start3, now)
	}
	if !createDate3.Equal(updateDate3) {
		t.Fatal(createDate3, updateDate3)
	}
	if rtype != "object" {
		t.Fatal(rtype)
	}
	if policy != 2 {
		t.Fatal(policy)
	}
	if device != 12345 {
		t.Fatal(device)
	}
	if count != 1 {
		t.Fatal(count)
	}
}
