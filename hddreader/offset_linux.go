/**
*  This file is part of go-disk-utils.
*
*  go-disk-utils is free software: you can redistribute it and/or modify
*  it under the terms of the GNU General Public License as published by
*  the Free Software Foundation, either version 3 of the License, or
*  (at your option) any later version.
*
*  go-disk-utils are distributed in the hope that it will be useful,
*  but WITHOUT ANY WARRANTY; without even the implied warranty of
*  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*  GNU General Public License for more details.
*
*  You should have received a copy of the GNU General Public License
*  along with rmlint.  If not, see <http://www.gnu.org/licenses/>.
*
** Authors:
 *
 *  - Daniel <SeeSpotRun> T.   2016-2016 (https://github.com/SeeSpotRun)
 *
** Hosted on https://github.com/SeeSpotRun/go-disk-utils
*
**/


package hddreader

import (
        "os"
        "github.com/SeeSpotRun/go-fibmap" // forked from https://github.com/frostschutz/go-fibmap
                                          // TODO: pull request to merge changes
)


// offset returns the physical offset (relative to disk start) of
// the data at the specified position within a file
func offsetf(f *os.File, seek uint64, whence int) (uint64, error) {

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

// offset returns the physical offset (relative to disk start) of
// the data at the specified position within a file associated with a path
func offset(path string, seek uint64, whence int) (uint64, error) {
        f, err := os.Open(path)
        if err != nil {
                return 0, err
        }
        defer f.Close()
        return offsetf(f, seek, whence)
}
