// offset returns information about the physical location of a file on a disk.  It is part of the hddreader package.

package hddreader

import (
	//"log"
	"os"
	"syscall"
	"unsafe"
)

const (
	fsctl_get_retrieval_pointers   uint32 = 0x00090073 //https://msdn.microsoft.com/en-us/library/cc246805.aspx
	extentsize                            = 16         // sizeof(Extent)
	retrieval_pointers_buffer_size        = 12         // sizeof(Retrieval_pointers_buffer)
	starting_vcn_input_buffer_size uint32 = 8          // sizeof(LARGE_INTEGER)
)

// large_integer represents a 64-bit signed integer in windows
// Surprisingly this is not in https://golang.org/src/syscall/syscall_windows.go
type large_integer struct {
	upper int32
	lower uint32
}

func (i *large_integer) get() int64 {
	return int64(i.upper)<<32 | int64(i.lower)
}

func (i *large_integer) set(v int64) {
	i.lower = uint32(v & 0xffffffff)
	i.upper = int32(v >> 32)
}

type extent struct {
	nextvcn large_integer
	lcn     large_integer
}

type retrieval_pointers_buffer struct {
	extentcount uint32 // DWORD;
	startingvcn large_integer
	//&extents[1]        // array of mapped extents (out)
}

type starting_vcn_input_buffer struct {
	startingvcn large_integer // LARGE_INTEGER
}

// offsetof returns the physical offset (relative to disk start) of
// the data at the specified absolute position in an open file
func offsetof(f *os.File, logical uint64, bytespersector uint64) (uint64, error) {

	//fd := syscall.Handle(f.Fd())
	fd, err := syscall.Open(f.Name(), os.O_RDONLY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return 0, nil
	}
	//defer syscall.Close(fd)

	extentcount := 4
	extents := make([]extent, extentcount+2) // not sure why we need one extra here but we do

	ptr := unsafe.Pointer(uintptr(unsafe.Pointer(&extents[1])) - retrieval_pointers_buffer_size)
	lpOutBuffer := (*retrieval_pointers_buffer)(ptr)
	lpOutBuffer.startingvcn.set(0)
	nOutBufferSize := uint32(retrieval_pointers_buffer_size + (extentcount+1)*extentsize)

	var bytesreturned uint32
	var startingvcn starting_vcn_input_buffer
	startingvcn.startingvcn.set(int64(logical / bytespersector))

	err = syscall.DeviceIoControl(fd,
		fsctl_get_retrieval_pointers,
		(*byte)(unsafe.Pointer(&startingvcn)), // A pointer to the input buffer, a STARTING_VCN_INPUT_BUFFER structure. (LPVOID)
		starting_vcn_input_buffer_size,        // The size of the input buffer, in bytes. (DWORD)
		(*byte)(ptr),                          // A pointer to the output buffer, a RETRIEVAL_POINTERS_BUFFER variably sized structure (LPVOID)
		nOutBufferSize,                        // The size of the output buffer, in bytes. (DWORD)
		&bytesreturned,                        // A pointer to a variable that receives the size of the data stored in the output buffer, in bytes. (LPDWORD)
		nil)                                   // lpOverlapped  //A pointer to an OVERLAPPED structure; if fd is opened without specifying FILE_FLAG_OVERLAPPED, lpOverlapped is ignored.(LPOVERLAPPED)

	if err != nil && err.Error() != "More data is available." {
		return 0, err
	}

	//log.Printf(" %d %d %d %d %d \n", bytesreturned, extents[0].lcn.get(), extents[0].nextvcn.get(), extents[1].lcn.get(), extents[1].nextvcn.get())
	return uint64(extents[1].lcn.get()) * bytespersector, nil
}
