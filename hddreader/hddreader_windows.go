package hddreader

import (
	"log"
	"path/filepath"
	"syscall"
	"unsafe"
)

const guessbps = 4096 // guess value to use if syscall fails

// bps uses syscall to get disk bytes per sector.
// Credit to https://github.com/StalkR/goircbot/blob/master/lib/disk/space_windows.go
func bps(path string) (result uint64) {
	result = guessbps

	kernel32, err := syscall.LoadLibrary("Kernel32.dll")
	if err != nil {
		log.Println("LoadLibrary:", err)
		return
	}
	defer syscall.FreeLibrary(kernel32)
	GetDiskFreeSpace, err := syscall.GetProcAddress(syscall.Handle(kernel32), "GetDiskFreeSpaceW")
	if err != nil {
		log.Println("GetProcAddress:", err)
		return
	}

	sectorsPerCluster := int64(0)
	bytesPerSector := int64(0)
	numberOfFreeClusters := int64(0)
	totalNumberOfClusters := int64(0)

	r1, _, e1 := syscall.Syscall6(uintptr(GetDiskFreeSpace), 4,
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(path))),
		uintptr(unsafe.Pointer(&sectorsPerCluster)),
		uintptr(unsafe.Pointer(&bytesPerSector)),
		uintptr(unsafe.Pointer(&numberOfFreeClusters)),
		uintptr(unsafe.Pointer(&totalNumberOfClusters)), 0)

	if r1 == 0 {
		if e1 != 0 {
			err = error(e1)
			log.Println("Syscall6, e1:", err)
		} else {
			log.Println("Syscall6:", syscall.EINVAL)
			err = syscall.EINVAL
		}
		return
	}
	log.Println("spc, bps:", sectorsPerCluster, bytesPerSector)

	result = uint64(sectorsPerCluster * bytesPerSector)
	return
}

// Get disk bytes-per-sector.
func (self *Disk) getbps(path string) uint64 {
	if self.bps == 0 {
		log.Println("Getting bps for:", path)
		self.bps = bps(filepath.VolumeName(path))
	} else {
		log.Println("Already have bps of", self.bps)
	}
	return self.bps
}
