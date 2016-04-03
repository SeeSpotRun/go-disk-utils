// offset returns information about the physical location of a file on a disk.  It is part of the hddreader package.

package hddreader

func OffsetFile(f *os.File, seek uint64, whence int) (uint64, error) {
        // TODO
        return 0, nil
}

// Offset returns the physical offset (relative to disk start) of
// the data at the specified position within a file
func Offset(path string, seek uint64, whence int) (uint64, error) {
        // TODO
        return 0, nil
}
