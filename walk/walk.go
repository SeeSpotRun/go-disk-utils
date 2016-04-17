/**
*  This file is part of drs.
*
*  drs is free software: you can redistribute it and/or modify
*  it under the terms of the GNU General Public License as published by
*  the Free Software Foundation, either version 3 of the License, or
*  (at your option) any later version.
*
*  drs is distributed in the hope that it will be useful,
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
** Hosted on https://github.com/SeeSpotRun/drs
*
**/

file walk.go provides filesystem walking using the drs disk scheduler

package drs

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

// Option flags for walking; add to options to change default behaviour
const (
	Defaults = 0
	// TODO: SeeRootLinks    = 1     // default: if root paths are symlinks they will be ignored
	// TODO: SeeLinks        = 2     // default: all symlinks (except root links) will be ignored
	// TODO: FollowLinks     = 4     // default: returns 'seen' symlinks as symlinks instead of following links
	// TODO: SeeDotFiles     = 8     // default: ignores '.' and '..' dir entries TODO
	HiddenDirs  = 16  // default: won't descend into folders starting with '.'
	HiddenFiles = 32  // default: won't return files starting with '.'
	ReturnDirs  = 64  // default: doesn't return dir paths (note: never returns root dirs)
	NoRecurse   = 128 // default: walks subdirs recursively
	//TODO: OneDevice       = 256   // default: walk will cross filesystem boundaries TODO
)

const winLongPathLimit = 259
const winLongPathHack = "\\\\?\\" // workaround prefix for windows 260-character path limit

var devs map[devt]*Disk

type WalkOptions struct {
	SeeRootLinks   bool
        SeeLinks       bool
        FollowLinks    bool
	SeeDotFiles    bool
	HiddenDirs     bool
        HiddenFiles    bool
        ReturnDirs     bool
        NoRecurse      bool
        OneDevice      bool
        Errs           chan <- error
        results        chan <- *File
	wg	       *sync.WaitGroup
}

// Path implements the drs.Job interface
type Path struct {
	Name     string
	Offset   uint64
	Depth    int
	Info     os.FileInfo
	Disk     *Disk
	opts     *WalkOptions
}

// Node is a unique identifier for a file
// TODO: windows flavour
type node struct {
	Dev     uint64
	Ino     uint64
}

// Go recurses into a directory, sending any files found and recursing any directories
func (p *Path) Go(read TokenReturn) {
	defer read.Done()
	defer p.wg.Done()

	if p.opts.ReturnDirs || !p.Info.IsDir() { // TODO: link checks
		p.Send()
	}

	if !p.Info.IsDir() {
		return
	}

	f, err := os.Open(p.Name)
	if err != nil {
		p.Report(err)
		return
	}
	names, err := f.Readdir(-1)
	f.Close()
	if err != nil {
		p.Report(err)
		return
	}

	// recurse into next level:
	// TODO: depth check
	for _, name := range names {
		if !opts.HiddenDirs && !opts.HiddenFiles && strings.HasPrefix(name, ".") {
			continue
		}

		// TODO: check if not in roots

		filename := Join(w.dirname, name)

		fileInfo, err := os.Lstat(filename)
		if err != nil {
			p.Report(err)
			continue
		}

		if fileInfo.IsDir() && !opts.HiddenDirs && strings.HasPrefix(name, ".") {
			// skip hidden dir
			continue
		}

		if !fileInfo.IsDir() && !opts.HiddenFiles && strings.HasPrefix(name, ".") {
			// skip hidden file
			continue
		}

		path := &Path {
			Name:   filename
			Depth:  p.Depth + 1
			Info:   fileInfo
			Disk:   p.Disk  // TODO
			opts:   p.opts
			wg:     p.wg
		}
		
		if !path.Disk.ssd {
			path.Offset, _ := StartOffset(filename, 0, os.SEEK_SET)
		}

		if fileInfo.IsDir() {
			// map inode to prevent recursion and path doubles
			stat := fileInfo.Sys().(*syscall.Stat_t)
			if !p.disk.AddDir( node { stat.Dev, stat.Ino }, filename )
				p.Report(fmt.Errorf("Duplicate dir %s; skipping", filename))
				continue
			}

		}
		p.wg.Add()
		path.Disk.Schedule(path, path.Offset, Normal)
	}
}


func(p *Path) Report(e error) {
	if p.opts.Errs != nil {
		p.opts.Errs <- e
	}
}

func(p *Path) Send() {
	if p.opts.Results != nil {
		p.opts.Results <- p
	}
}


// Walk returns a path channel and walks (concurrently) all paths under
// each root folder in []roots, sending all regular files encountered to the
// channel, which is closed once all walks have completed.
// Walking the same file or folder twice is avoided, so for example
//  ch := Walk(e, {"/foo", "/foo/bar"}, options, disk)
// will only walk the files in /foo/bar once.
// Errors are returned via errc (if provided).
func Walk(
	errc chan<- error,
	roots []string,
	options *WalkOptions,
	disk *Disk,   // TODO
) <-chan *Path {

	// canonicalise root dirs, removing duplicate paths
	rmap := make(map[string]bool)
	for _, root := range roots {
		r, err := fixpath(filepath.Clean(root))
		if err != nil {
			errc <- err
		}
		if _, ok := rmap[r]; !ok {
			// no match; add root to set
			rmap[r] = true
		}
	}

	// create result channel
	opts.Results = make(chan *File)

	// be flexible with done channel
	if done == nil {
		done = make(chan (struct{}))
	}

	for root := range rmap {

		fileInfo, err := os.Lstat(root)
		path := &Path {
			Name:   root
			Depth:  0
			Info:   fileInfo
			Disk:   disk  // TODO
			opts:   options
		}

		if !disk.ssd {
			path.Offset, _, _, _ := StartOffset(filename, 0, os.SEEK_SET)
		}
		
		wg.Add(1)
		disk.Schedule(path, path.Offset, Normal)
	}

	go func() {
		// wait until all roots have been walked, then close channels
		wg.Wait()
		close(filec)
	}()

	return opts.Results
}

func fixpath(p string) (string, error) {
	var err error
	// clean path
	p, err = filepath.Abs(filepath.Clean(p))
	// Workaround for archaic Windows path length limit
	if runtime.GOOS == "windows" && !strings.HasPrefix(p, winLongPathHack) {
		p = winLongPathHack + p
	}
	return p, err
}

func unfixpath(p string) string {
	if runtime.GOOS == "windows" {
		return strings.TrimPrefix(p, winLongPathHack)
	}
	return p
}
