package hummingbird

import (
	"sync/atomic"
	"unsafe"
)

type FreePool []*unsafe.Pointer

func (a FreePool) Get() interface{} {
	for _, p := range a {
		if v := atomic.SwapPointer(p, nil); v != nil {
			return *(*interface{})(v)
		}
	}
	return nil
}

func (a FreePool) Put(v interface{}) {
	vp := unsafe.Pointer(&v)
	for _, p := range a {
		if atomic.CompareAndSwapPointer(p, nil, vp) {
			return
		}
	}
}

func NewFreePool(size int) FreePool {
	a := make(FreePool, size)
	for i := range a {
		a[i] = new(unsafe.Pointer)
	}
	return a
}
