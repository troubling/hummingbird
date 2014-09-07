package hummingbird

/*
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>
#include <attr/xattr.h>
#include <sys/types.h>
#include <pwd.h>
#include <stdlib.h>
#include <fcntl.h>
*/
import "C"
import "unsafe"

func FGetXattr(fd int, name string, value []byte) int {
	cname := C.CString(name)
	bytes := C.fgetxattr(C.int(fd), cname, unsafe.Pointer(&value[0]), C.size_t(len(value)))
	C.free(unsafe.Pointer(cname))
	return int(bytes)
}

func FSetXattr(fd int, name string, value []byte) int {
	cname := C.CString(name)
	ret := C.fsetxattr(C.int(fd), cname, unsafe.Pointer(&value[0]), C.size_t(len(value)), 0)
	C.free(unsafe.Pointer(cname))
	return int(ret)
}

func DropBufferCache(fd int, length int64) {
	C.posix_fadvise(C.int(fd), C.__off_t(0), C.__off_t(length), C.int(4))
}

func DropPrivileges(name string) {
	cname := C.CString(name)
	home := C.CString("HOME")
	slash := C.CString("/")
	defer C.cfree(unsafe.Pointer(home))
	defer C.cfree(unsafe.Pointer(cname))
	defer C.cfree(unsafe.Pointer(slash))
	cpw := C.getpwnam(cname)
	C.setgid(cpw.pw_gid)
	C.setuid(cpw.pw_uid)
	C.setenv(home, cpw.pw_dir, 1)
	C.setsid()
	C.chdir(slash)
	C.umask(022)
}
