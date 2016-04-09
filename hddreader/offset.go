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
)


// offsetf returns the physical offset (relative to disk start) of
// the data at the specified relative position in an open file
func offsetf(f *os.File, seek int64, whence int) (physical uint64, logical uint64, size int64, err error) {

		info, err := f.Stat()
		if err != nil {
			return
		}
		size = info.Size()
		
        switch {
		case whence == os.SEEK_SET:
				logical = uint64(seek)
        case whence == os.SEEK_CUR:
                // calculate required file offset without changing actual seek
                current, e := f.Seek(0, 0)
                if e != nil {
						err = e
                        return
                }
                logical = uint64(seek + current)
        case whence == os.SEEK_END:
                logical = uint64(seek + size)
		default:
				// default to logical = 0?
        }
        physical, err = offsetof(f, logical)
		if err != nil {
				return 0, 0, 0, err
		}
		return
}

// offset returns the physical offset (relative to disk start) of
// the data at the specified position within a file associated with a path
func offset(path string, seek int64, whence int) (physical uint64, logical uint64, size int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	//defer f.Close()
	return offsetf(f, seek, whence)
}
