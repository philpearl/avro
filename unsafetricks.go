package avro

import "unsafe"

//go:linkname unsafe_New reflect.unsafe_New
func unsafe_New(rtype unsafe.Pointer) unsafe.Pointer

//go:linkname unsafe_NewArray reflect.unsafe_NewArray
func unsafe_NewArray(rtype unsafe.Pointer, length int) unsafe.Pointer

// typedslicecopy copies a slice of elemType values from src to dst,
// returning the number of elements copied.
//
//go:linkname typedslicecopy reflect.typedslicecopy
//go:noescape
func typedslicecopy(elemType unsafe.Pointer, dst, src sliceHeader) int

//go:linkname mapassign reflect.mapassign
//go:noescape
func mapassign(typ unsafe.Pointer, hmap unsafe.Pointer, key, val unsafe.Pointer)

// typedmemclr zeros the value at ptr of type t.
//
//go:linkname typedmemclr reflect.typedmemclr
//go:noescape
func typedmemclr(typ, ptr unsafe.Pointer)

// We could use the reflect version of mapiterinit, but that forces a heap
// allocation per map iteration. Instead we can use the runtime version, but
// then we have to allocate a runtime private struct for it to use instead. We
// can do this, and it uses stack memory, so that's less GC pressure and more
// speed. But it isn't excellent from a maintenance point of view. Things will
// break if the struct changes and we won't find out. But let's go for it.
//
// mapiter matches hiter in runtime/map.go. Using matching-ish types rather than
// a big enough array of unsafe.Pointer just in case the GC would run into an
// issue if something it thought was a pointer was not. Don't attempt to access
// any of the fields in this struct directly! On the plus side this hasn't
// changed significantly for 6 years
type mapiter struct {
	key         unsafe.Pointer
	elem        unsafe.Pointer
	t           unsafe.Pointer
	h           unsafe.Pointer
	buckets     unsafe.Pointer
	bptr        unsafe.Pointer
	overflow    unsafe.Pointer
	oldoverflow unsafe.Pointer
	startBucket uintptr
	offset      uint8
	wrapped     bool
	B           uint8
	i           uint8
	bucket      uintptr
	checkBucket uintptr
}

//go:linkname mapiterinit runtime.mapiterinit
//go:noescape
func mapiterinit(t unsafe.Pointer, m unsafe.Pointer, hi unsafe.Pointer)

//go:linkname mapiterkey reflect.mapiterkey
//go:noescape
func mapiterkey(it unsafe.Pointer) (key unsafe.Pointer)

//go:linkname mapiterelem reflect.mapiterelem
//go:noescape
func mapiterelem(it unsafe.Pointer) (elem unsafe.Pointer)

//go:linkname mapiternext reflect.mapiternext
//go:noescape
func mapiternext(it unsafe.Pointer)

//go:linkname maplen reflect.maplen
//go:noescape
func maplen(m unsafe.Pointer) int
