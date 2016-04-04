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

// walk walks one or more root paths concurrently and sends the results
// back via a channel.  It wraps path/filepath.Walk() for people who
// don't want to think too much about symlinks, recursion, hidden file
// handling, etc.
package walk
/*
 * TODO:
 * [x] write walk_test
 * [ ] add some more tests
 * [ ] benchmarking & profiling
 * [x] copyright etc
 * [ ] add separate channel for warnings vs walk errors?
 * [ ] symlink handling
 * [ ] add OneDevice option
 * [ ] add depth limiter
 * [ ] inode-based recursion test
*/

import(
        "os"
        "path/filepath"
        "errors"
        "runtime"
        "sync"
        "strings"
)

// Option flags for walking; add to options to change default behaviour
const(
        Defaults        = 0
        // TODO: SeeRootLinks    = 1     // default: if root paths are symlinks they will be ignored
        // TODO: SeeLinks        = 2     // default: all symlinks (except root links) will be ignored
        // TODO: FollowLinks     = 4     // default: returns 'seen' symlinks as symlinks instead of following links
        // TODO: SeeDotFiles     = 8     // default: ignores '.' and '..' dir entries TODO
        HiddenDirs      = 16    // default: won't descend into folders starting with '.'
        HiddenFiles     = 32    // default: won't return files starting with '.'
        ReturnDirs      = 64    // default: doesn't return dir paths (note: never returns root dirs)
        NoRecurse       = 128   // default: walks subdirs recursively
        //TODO: OneDevice       = 256   // default: walk will cross filesystem boundaries TODO
)

const winLongPathLimit = 259
const winLongPathHack = "\\\\?\\"    // workaround prefix for windows 260-character path limit

type File struct {
        Path string
        Info os.FileInfo
}

// FileCh returns a file channel and walks (concurrently) all paths under
// each root folder in []roots, sending all regular files encountered to the
// channel, which is closed once all walks have completed.
// Walking the same file or folder twice is avoided, so for example
//  ch := FileCh(d, e, {"/foo", "/foo/bar"}, true)
// will only walk the files in /foo/bar once.
// Errors are returned via the optional caller-supplied error channel.
// If hidden is false then hidden files and folders are ignored, unless
// they are members of []roots.
// If done is closed, FileCh() aborts the walk(s) and closes the file channel.
func FileCh(
        done <-chan struct{},
        errc chan <- error,
        roots []string,
        options int,
        ) <-chan *File {

        // canonicalise root dirs, removing duplicate paths
        rmap := make(map[string]bool)
        for _, root := range roots {
                r, err := FixPath(filepath.Clean(root))
                if err != nil {
                        errc <- err
                }
                if _, ok := rmap[r]; !ok {
                    // no match; add root to set
                    rmap[r] = true
                }
        }

        // create result channel
        filec := make(chan *File)

        // start goroutine for each root path
        var wg sync.WaitGroup
        for root := range rmap {
                wg.Add(1)
                go func(r string) {
                        e := filepath.Walk(r, func(path string, info os.FileInfo, err error) error {
                                if err != nil {
                                        errc <- err
                                        return nil
                                }

                                if info.IsDir() {
                                        if path == r {
                                               // top level path - descend into it but don't return it
                                                return nil
                                        }
                                        if ((options & NoRecurse) != 0)  ||
                                           ((options & HiddenDirs) == 0 && strings.HasPrefix(info.Name(), ".")) { /* ||
                                           ((options & OneDevice != 0) && TODO: xdev check) */
                                                return filepath.SkipDir
                                        }
                                        if rmap[path] == true {
                                                // walked into a path double
                                                errc <- errors.New("Skipping duplicate dir " + path)
                                                return filepath.SkipDir
                                                // TODO: add inode-based recursion test for OS's which support recursion
                                        }
                                        if (options & ReturnDirs) != 0 {
                                                filec <- &File{UnFixPath(path), info}
                                        }
                                        return nil
                                }

                                if !info.Mode().IsRegular() {
                                        // don't return anything other than regular files
                                        // TODO: symlink handling
                                        return nil
                                }

                                if path != r {
                                        if rmap[path] == true {
                                                // path double
                                                errc <- errors.New("Skipping duplicate file " + path)
                                                return nil
                                        }
                                        if (options & HiddenFiles) == 0 && strings.HasPrefix(info.Name(), ".") {
                                                // skip hidden file
                                                return nil
                                        }
                                }
                                // TODO: also collect & return dev and inode data

                                // Send result to filec unless done has been closed
                                select{
                                case filec <- &File{UnFixPath(path), info}:
                                case <-done:
                                        return errors.New("walk canceled")
                                }
                                return nil
                        })
                        if e != nil {
                                errc <- e
                        }
                        wg.Done()
                }(root)
        }

        go func() {
                // wait until all roots have been walked, then close channels
                wg.Wait()
                close(filec)
        }()

        return filec
}


func FixPath(p string) (string, error) {
        var err error
        // clean path
        p, err = filepath.Abs(filepath.Clean(p))
        // Workaround for archaic Windows path length limit
        if runtime.GOOS == "windows" && !strings.HasPrefix(p, winLongPathHack) {
                p = winLongPathHack + p
        }
        return p, err
}

func UnFixPath(p string) string {
        if runtime.GOOS == "windows" {
                return strings.TrimPrefix(p, winLongPathHack)
        }
        return p
}
