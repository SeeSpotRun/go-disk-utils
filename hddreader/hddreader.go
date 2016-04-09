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

// Package hddreader wraps an os.File object with a algorithm which tries to optimise
// parallel reading of multiple files from a hdd.  It does this by limiting number
// of open files and reading files in order of disk offsets.
// Main components:
// File: file object, semi-interchangeable with os.File.  Implements: Reader, Closer, WriterTo
// Disk: disk object, schedules file reads
// bufferpool: pool of reusable same-sized []byte buffers
package hddreader

/*
 * TODO:
 * [x] write hddreader_test
 *     [ ] add some more test cases
 * [ ] add missing os.File calls
 * [ ] add ssd option
 * [ ] write some utilities to use this package:
 *     [x] sum (fast checksum calculator)
 *     [ ] grep
 *     [ ] match (duplicate detector)
 * [ ] test hddreader performance impact on existing utilities
 *     [ ] https://github.com/svent/sift
 * [ ] add bufio option?
 * [ ] benchmarking
 * [ ] profiling
 * [ ] astyler or similar
 * [x] copyright etc
*/

import (
        "log"
        "os"
        "sync"
        "io"
        "errors"
        "sort"
        "fmt"
)

const (
        defaultMaxRead = 1    // number of files simultaneously reading
        defaultMaxOpen = 50   // number of file.File's simultaneously open
        defaultMaxWindow = 5  // number of file.File's simultaneously reading under ahead/behind clause
                              // https://www.usenix.org/legacy/event/usenix09/tech/full_papers/vandebogart/vandebogart_html/index.html
                              // suggests a window of opportunity for quick seeks in the +/- 0.5 MB range.  This article is
                              // from Proc. USENIX (2009) when typical hard drive size was around 1TB; scaling to 4TB
                              // drives suggests maybe a 2MB window
        defaultAhead = 2 *1024 * 1024
        defaultBehind = 1 * 1024 * 1024

        defaultBufSize = 4096   // for buffering file.WriteTo() calls
        defaultBufCount = 1024  // max number of buffers total
        defaultBufPerFile = 10  // number of buffers per file
)

// convenience type for signalling channels
type nothing struct {}


// bufferpool manages a pool of recyclable fixed-size []byte buffers.  I learnt this trick from sahib:
// https://github.com/sahib/rmlint/commit/f8fe6ffa4684405ece1d7c3aa173d0c3d82ccdb1#diff-5181df57afdfbebfb9dd6e2fd4060cf6R18
type bufferpool struct {
    BufSize    int
    maxbufs    int
    nbufs      int
    bufs       chan []byte
    reqs       chan nothing
}

// newbufferpool creates a bufferpool and starts a goroutine which
// feeds new buffers into the pool as required
func newbufferpool(bufsize int, maxbufs int) *bufferpool {
        self := &bufferpool{
                        BufSize: bufsize,
                        maxbufs: maxbufs,
                        bufs:  make(chan([]byte), maxbufs),
                        reqs:  make(chan(nothing)),
                        }
        go func() {
                // if any requests received for new buffers then pop one or make one
                for _ = range(self.reqs) {
                        if self.nbufs < self.maxbufs {
                                self.nbufs++
                                self.bufs <- make([]byte, self.BufSize)
                        }
                }
        }()

        return self
}

// get gets a buffer from the pool
func (p *bufferpool) get() []byte {
        select {
        case b:= <- p.bufs:
                // recycled buffer
                return b
        default:
                // no buffers in pool; request new one
                p.reqs <- nothing{}
                // wait for buffer to arrive
                return <- p.bufs
        }
}

// ret returns a buffer to the pool
func (p *bufferpool) ret(b []byte) {
        select {
        // note: b is expanded to cap(b) before returning
        case p.bufs <- b[:cap(b)]:  // should not block
        default:
                panic("Blocked returning buffer")
        }
}

// close closes the bufferpool and returns the number of buffers used
func (p *bufferpool) close() (used int, err error) {
        close(p.reqs)
        close(p.bufs)
        used = 0
        for _ = range(p.bufs) {
                used++
        }
        if used != p.nbufs{
                err = errors.New(fmt.Sprintf("Expected %d buffers, got %d", p.nbufs, used))
        }
        return
}



// A Disk schedules read operations for files.  It shoulds be created using NewDisk.  A call
// to Disk.Start() is required before the disk will allow associated files to start
// reading data.
//
// Example usage:
//  d := hddreader.NewDisk(1, 0, 0, 0, 100, 64)
//  for _, p := range(paths) {
//          wg.Add(1)
//          go read_stuff_from(p, &wg)
//          // calls hddreader.OpenFile() and file.Read() but will block until d.Start() called
//  }
//  d.Start()  // enables reading and will unblock pending Read() calls in disk offset order
//  wg.Wait()
//  d.Close()
type Disk struct {
        opench        chan struct{} // tickets to limit number of simultaneous files open
        reqch         chan *File    // requests for read permission
        donech        chan *File    // signal that file has finished reading
        bufpool       *bufferpool   // reusable read buffers
        startch       chan struct{} // used by disk.Start() to signal start of reading
        wait          int           // how many pending reads to wait for before starting first read
}

// NewDisk creates a new disk object to schedule read operations in order of increasing
// physical offset on the disk.
//
// Up to maxread files will be read concurrently.  An additional up to maxwindow files
// may be opened for reading if they are within (-behind,+ahead) bytes of
// the current head position. An additional up to maxopen files may be open for the
// sole purpose of reading disk offset.
//
// If bufkB > 0 then an internal buffer pool is created to buffer WriteTo() calls.  Note that while
// read order is preserved, buffering may change the order in which WriteTo(w) calls complete, depending
// on speed at which buffers are writted to w.
//
func NewDisk(maxread int, maxwindow int, ahead int64, behind int64, maxopen int, bufkB int) *Disk {

        if maxread <= 0 {
                maxread = defaultMaxRead
        }
        if maxopen <= 0 {
                maxopen = defaultMaxOpen
        }
        if maxwindow < 0 {
                maxwindow = defaultMaxWindow
        }

        if ahead < 0 {
                ahead = defaultAhead
        }
        if behind < 0 {
                behind = defaultBehind
        }

        if maxread > maxopen {
            panic("Disk can't have MaxRead > MaxOpen")
        }

        self := &Disk{
                    opench: make(chan(struct{}), maxopen),
                    startch: make(chan(struct{})),
                    reqch:  make(chan(*File)),
                    donech: make(chan(*File)), // TODO: does a buffer done channel improve speed?
                    }

        if bufkB > 0 {
                self.bufpool = newbufferpool(defaultBufSize, 1 + (bufkB * 1024 - 1) / defaultBufSize)
        }

        // Populate opench (a buffered chanel to limit total concurrent Open() + Read() calls)
        for i:=0; i<maxopen; i++ {
                self.pushtik()
        }

        // start scheduler to prioritise Read() calls
        go self.scheduler(maxread, maxwindow, ahead, behind)

        return self
}

// Start needs to be called once in order to enable file data to start reading.
// If wait > 0 then will wait until that many read requests have been registered before starting
func (self *Disk) Start(wait int)       {
        // wait until no Open() calls in progress
        self.wait = wait
        self.startch <- nothing{}
}

// scheduler manages file.Read() calls to try to process files in disk order
func (self *Disk) scheduler(maxread int, maxwindow int, ahead int64, behind int64) {

        openFiles := 0
        var offset uint64 = 0
        var reqs []*File  // sorted reqs
        var requ []*File  // unsorted reqs
        started := false  // whether reading has been started yet by disk.Start


        release := func() {
                if !started {
                        return
                }
                if self.wait > 0 && len(reqs) + len(requ) < self.wait {
                        // start signal received but not enough pending reads yet
                        return
                }
                self.wait = 0
                for ; len(reqs) + len(requ) > 0 && openFiles < maxread + maxwindow;  {
                        if len(requ) >= len(reqs) {
                                // time to merge
                                sort.Sort(byoffset(requ))
                                reqs = merge(reqs, requ)
                                requ = nil
                        }

                        // find first file at-or-ahead of disk head position
                        i := sort.Search(len(reqs), func(i int) bool { return reqs[i].Offset >= offset })


                        // pop() pops a job from the queue and unblocks its read request
                        pop := func(i int) {
                                popped := reqs[i]
                                if openFiles <= maxread {
                                        // ahead/behind window 'extras' don't reset offset
                                        offset = popped.Offset
                                }
                                // log.Printf("%20d %10d", popped.Offset, len(reqs))

                                // safe delete from slice:
                                copy(reqs[i:], reqs[i+1:])
                                reqs[len(reqs)-1] = nil
                                reqs = reqs[:len(reqs)-1]

                                // unblock pending read on popped file
                                openFiles++
                                popped.wg.Done()
                        }

                        // gap returns the seek gap from current head position to file i
                        gap := func(i int) int64 {
                                return int64(reqs[i].Offset) - int64(offset)
                        }

                        // release best match
                        if i == len(reqs) {  // all file are behind current offset
                                i = i - 1
                        }

                        if gap(i) >= 0 && gap(i) <= ahead {
                                // found a job in ahead window; release it
                                pop(i)
                        } else if i > 0 && gap(i-1) <= 0 && gap(i-1) >= -behind {
                                // found a job in behind window; release it
                                pop(i-1)
                        } else if gap(i) >= 0 {
                                // jump ahead to next file
                                pop(i)
                        } else {
                                // we've reached the end of the disk; don't release
                                // further jobs until all active reads have finished,
                                // then release most-negative offset
                                if openFiles > 0 {
                                        break
                                }
                                pop(0)
                        }
                }
        }

        // main scheduler loop:
        for {
                select{
                case f := <- self.reqch:
                        // request to read data from f
                        if f == nil {
                                // channel closed; we are done
                                break
                        }
                        // append to unsorted reqs:
                        requ = append(requ, f)
                        release()

                case  f := <- self.donech:
                        // a read job has finished; release pending requests accordingly
                        openFiles --
                        offset = f.Offset
                        release()
                case <- self.startch:
                        started = true
                        release()
                }
        }
        // clean-up after reqch closed
        if len(reqs) != 0 {
                log.Printf("NON-ZERO OPEN FILES: %d\n", len(reqs))
        }

}

// poptik grants a ticket to open an fd
func (d *Disk) poptik() {
        <- d.opench
}

// pushtik returns an open fd ticket
func (d *Disk) pushtik() {
        d.opench <- nothing{}
}

// Closes closes the disk scheduler and frees buffer memory
func (d *Disk) Close() {
        // close bufpool
        if d.bufpool != nil {
                n, err := d.bufpool.close()
                // TODO: clean up debug logging
                log.Printf("Buffers used: %d", n)
                if err != nil {
                        log.Println(err)
                }
        }
}


// a File struct wraps an os.File stuct with the necessary field
// and methods for hdd-optimised reads.
type File struct {
    file   *os.File
    disk   *Disk
    size   int64   // file size in bytes (used for deciding how many read buffers)
    name   string  // duplicates *os.File.Name() since File may be closed
    Offset uint64  // file location relative to start of disk
    wg     sync.WaitGroup  // for signalling read permission
    isshut bool    // whether *os.File is closed
}

// Open creates a new File object associated with a Disk, and register the
// file's physical position on the disk.  It may block until the disk's
// total open file count falls below the disk's open file limit.
func Open(name string, disk *Disk) (*File, error) {

        self := &File{file: nil, disk: disk, name: name, isshut: true}

        // wait for open ticket
        disk.poptik()
        defer disk.pushtik()

		var err error
		self.Offset, _, self.size, err = offset(name, 0, 0)
        if err != nil {
                return nil, err
        }

        return self, err
}

func OpenFile(name string, flag int, perm os.FileMode, disk *Disk, keepOpen bool) (*File, error) {
        // TODO
        return Open(name, disk)
}


// Read reads up to len(b) bytes from the File.
// It returns the number of bytes read and an error, if any.
// EOF is signaled by a zero count with err set to io.EOF.
func (f *File) Read(b []byte) (n int, err error) {
        // open f.File for reading
        err = f.open()
        if err != nil {
                f.Close()
                return
        }

        n, err = f.file.Read(b)
        if err != nil && err != io.EOF {
                log.Printf("shut %s\n", f.name)
                f.Close()
        }

        // TODO: would this help or is it an over-optimisation?:
        // o, e := OffsetFile(f.file, 0, 0)
        // if e == nil {
        //     f.Offset = o
        // }
        return
}


// WriteTo implements io.WriterTo interface.
func (f *File) WriteTo(w io.Writer) (n int64, err error) {

        // open f.file for reading
        err = f.open()
        if err != nil {
                f.Close()
                return 0, err
        }

        if f.disk.bufpool != nil {
                // do asynchronous read & write, freeing up disk as soon as read is finished

                ch := make(chan([]byte), defaultBufPerFile)  // TODO: revisit buffer count

                // TODO: maybe need to flush any pending writes?

                // writes to writer will be in background goroutine
                var wg sync.WaitGroup
                wg.Add(1)
                var werr error // any error during writing
                go func() {
                        for b := range(ch) {
                                m, werr := w.Write(b)
                                f.disk.bufpool.ret(b)
                                n +=  int64(m)
                                if werr != nil {
                                        close(ch)
                                        break
                                }
                        }
                        wg.Done()
                }()

                // do the reading and send to the writer goroutine
                for ; err == nil; {
                        b := f.disk.bufpool.get()
                        var m int
                        m, err = f.file.Read(b)
                        if err == nil || err == io.EOF {
                                // send read data to the writer goroutine
                                ch <- b[:m]
                        } else {
                                f.disk.bufpool.ret(b)
                        }
                }
                close(ch)
                if err == io.EOF {
                        // EOF is expected result so cancel error
                        err = nil
                }

                // shut the underlying file
                e := f.shut()
                if e != nil && err == nil {
                        err = e
                }

                // tell the disk that reading is done
                f.disk.donech <- f

                // wait for reading to finish
                wg.Wait()

                if werr != nil {
                        err = werr
                }
                return n, err
        }

        // if w impements the ReaderFrom interface then use w.ReadFrom()
        if w, ok := w.(io.ReaderFrom); ok {
                m, err := w.ReadFrom(f.file)
                n += m
                // shut the underlying file
                e := f.shut()
                if e != nil && (err == nil || err == io.EOF) {
                        err = e
                }
                // tell the disk that reading is done
                f.disk.donech <- f

                return n, err
        }

        // do 'conventional' read/write
        // TODO: benchmark vs bufio
        b := make([]byte, defaultBufSize)
        for ; err == nil; {
                var nr int
                nr, err = f.file.Read(b)
                if err == nil || err == io.EOF {
                        nw, e := w.Write(b[:nr])
                        n +=  int64(nw)
                        if e == nil && nw != nr {
                                e = errors.New("Written bytes not equal to read bytes")
                                log.Printf("Read %d wrote %d\n", nr, nw)
                        }
                        if e != nil {
                                err = e
                        }
                }
                // reexpand b if necessary
                if len(b) < cap(b) {
                        b = b[:cap(b)]
                }
        }
        if err == io.EOF {
                err = nil
        }
        return

}


// shut closes the underlying os.File but not f itself
func (f *File) shut() (err error) {
        if f == nil {
                return os.ErrInvalid
        }

        if f.isshut {
                panic("Attempt to shut a shut file")
        }

        // close the file
        err = f.file.Close()
        f.isshut = true

        return
}


// open opens a file for reading.  If the file is already open for reading then
// it's a NOP, otherwise it blocks until permission is granted by f.disk
// to start reading.
func (f *File) open() (err error) {
        if f.isshut {

                f.wg.Add(1)
                f.disk.reqch <- f
                f.wg.Wait()

                f.file, err = os.Open(f.name)
                if err != nil {
                        // failed to open, tell disk
                        f.disk.donech <- f
                } else {
                        f.isshut = false
                }
        }
        return
}


// Close closes f, notifying the disk scheduler accordingly
func (f *File) Close() (err error) {
        if !f.isshut {
                err = f.shut()
                f.disk.donech <- f
        }
        return
}

// Name returns file name as provided to Open
func (f *File) Name() string {
        return f.name
}

// TODO: extend func (f *File) to cover all os.File functions


// byoffset implements sort.Interface for sorting a slice of File
// pointers by increasing Offset.
type byoffset []*File

func (f byoffset) Len() int           { return len(f) }
func (f byoffset) Swap(i, j int)      { f[i], f[j] = f[j], f[i] }
func (f byoffset) Less(i, j int) bool { return f[i].Offset < f[j].Offset }


type mergeptr struct {
        f []*File
        i0 int // index of first uncopied file
        i int  // current file index for comparison
}

// merge merges two sorted slices of *File
func merge(a []*File, b []*File) []*File {

        if len(a) == 0 {
                return b
        }
        if len(b) == 0 {
                return a
        }

        merged := make([]*File, 0, len(a) + len(b))

        from:= &mergeptr{a, 0, 0} // next files will come from a
        then:= &mergeptr{b, 0, 0} // then from b

        for ; from.i <= len(from.f) ; {
                if from.i == len(from.f)  {
                        // from is finished, write remaining files
                        merged = append(merged, from.f[from.i0:]...)
                        merged = append(merged, then.f[then.i0:]...)
                        break
                }
                if then.i == len(then.f)  {
                        panic("then.i == len(then.f)")
                }

                if then.f[then.i].Offset < from.f[from.i].Offset {
                        // time to switch piles
                        merged = append(merged, from.f[from.i0:from.i]...)
                        from.i0 = from.i
                        from, then = then, from
                }
                from.i++
        }

        // verify sort
        // TODO: remove me
        off := uint64(0)
        for _, f := range(merged) {
                if f.Offset < off {
                        panic("Sort failed")
                }
                off = f.Offset
        }

        return merged
}

