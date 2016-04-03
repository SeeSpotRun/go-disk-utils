// offset returns information about the physical location of a file on a disk.  It is part of the hddreader package.

package hddreader

import (
        "os"
        "github.com/SeeSpotRun/go-fibmap" // forked from https://github.com/frostschutz/go-fibmap
                                          // TODO: pull request to merge changes
)


func OffsetFile(f *os.File, seek uint64, whence int) (uint64, error) {

        switch {
        case whence == os.SEEK_CUR:
                // calculate required file offset without changing actual seek
                current, err := f.Seek(0, 0)
                if err != nil {
                        return 0, err
                }
                seek = seek + uint64(current)
        case whence == os.SEEK_END:
                info, err := f.Stat()
                if err != nil {
                        return 0, err
                }
                seek = seek + uint64(info.Size())
        }
        
        extents, errno := fibmap.NewFibmapFile(f).FiemapAt(1, seek)
        if errno == 0 {
                if len(extents) == 0 {
                        // there is no data for this range of the file - it's a hole?
                        return 0, nil
                }
                // adjust retulst for file seek position relative to start of extent
                return extents[0].Physical + seek - extents[0].Logical, nil
        } else {
                return 0, errno
        }
}

// Offset returns the physical offset (relative to disk start) of
// the data at the specified position within a file
func Offset(path string, seek uint64, whence int) (uint64, error) {
        f, err := os.Open(path)
        if err != nil {
                return 0, err
        }
        defer f.Close()
        return OffsetFile(f, seek, whence)
}
