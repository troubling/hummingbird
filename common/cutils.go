package common

/*
#include <unistd.h>
#include <stdlib.h>
// #include <fcntl.h>
#include <pwd.h>
#include <sys/types.h>
#include <sys/stat.h>
*/
import "C"
import "unsafe"
import "syscall"

func FSetXattr(fd int, attr string, value []byte) (int, error) {
	attrp, err := syscall.BytePtrFromString(attr)
	if err != nil {
		return 0, err
	}
	valuep := unsafe.Pointer(&value[0])
	r0, _, e1 := syscall.Syscall6(syscall.SYS_FSETXATTR, uintptr(fd), uintptr(unsafe.Pointer(attrp)), uintptr(valuep), uintptr(len(value)), 0, 0)
	if e1 != 0 {
		err = e1
	}
	return int(r0), nil
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
