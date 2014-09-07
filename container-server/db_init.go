package main

/*
#cgo pkg-config: sqlite3
#include <stdlib.h>
#include <sqlite3.h>
void ChexorFunc(sqlite3_context *context, int argc, sqlite3_value **argv);
*/
import "C"
import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/mattn/go-sqlite3"
)

//export ChexorFunc
func ChexorFunc(context *C.sqlite3_context, argc C.int, argv **C.sqlite3_value) {
	p2 := (*[3]*C.sqlite3_value)(unsafe.Pointer(argv))

	old := C.GoStringN((*C.char)(unsafe.Pointer(C.sqlite3_value_text(p2[0]))),
		C.sqlite3_value_bytes(p2[0]))
	name := C.GoBytes(unsafe.Pointer(C.sqlite3_value_text(p2[1])),
		C.sqlite3_value_bytes(p2[1]))
	timestamp := C.GoBytes(unsafe.Pointer(C.sqlite3_value_text(p2[2])),
		C.sqlite3_value_bytes(p2[2]))

	hash := md5.New()
	hash.Write(name)
	hash.Write([]byte("-"))
	hash.Write(timestamp)
	digest := hash.Sum(nil)

	old_digest, _ := hex.DecodeString(old)
	for i := 0; i < 16; i++ {
		digest[i] ^= old_digest[i]
	}
	result := fmt.Sprintf("%x", digest)
	C.sqlite3_result_text(context, C.CString(result), 32, (*[0]byte)(C.free))
}

var chexor_name = C.CString("chexor")

func SQLiteSetup(conn *sqlite3.SQLiteConn) error {
	db := (*C.sqlite3)(unsafe.Pointer(reflect.ValueOf(*conn).FieldByName("db").Pointer()))
	C.sqlite3_create_function(db, chexor_name, C.int(3), C.SQLITE_ANY, nil, (*[0]byte)(C.ChexorFunc), nil, nil)
	C.sqlite3_busy_timeout(db, 10000)
	return nil
}
