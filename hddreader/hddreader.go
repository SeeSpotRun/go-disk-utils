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
// bufferpool.go: greates a pool of reusable same-sized []byte buffers
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
	"io"
	"log"
	"os"
	"sort"
	"sync"
)

const (
	defaultMaxRead   = 1  // number of files simultaneously reading
	defaultMaxOpen   = 50 // number of file.File's simultaneously open
	defaultMaxWindow = 5  // number of file.File's simultaneously reading under ahead/behind clause
	// https://www.usenix.org/legacy/event/usenix09/tech/full_papers/vandebogart/vandebogart_html/index.html
	// suggests a window of opportunity for quick seeks in the +/- 0.5 MB range.  This article is
	// from Proc. USENIX (2009) when typical hard drive size was around 1TB; scaling to 4TB
	// drives suggests maybe a 2MB window
	defaultAhead  = 2 * 1024 * 1024
	defaultBehind = 1 * 1024 * 1024

	defaultBufSize    = 32 * 1024 // for buffering file.WriteTo() calls
	defaultBufCount   = 1024      // max number of buffers total
	defaultBufPerFile = 10        // number of buffers per file
)

// convenience type for signalling channels
type token struct{}

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
	opench  chan struct{} // tickets to limit number of simultaneous files open
	reqch   chan *File    // requests for read permission
	jobch   chan *Job
	donech  chan *File    // signal that file has finished reading
	startch chan struct{} // used by disk.Start() to signal start of reading
	wait    int           // how many pending reads to wait for before starting first read
	bps     uint64        // how many bytes per disk sector (needed for Windows offset calcs)
	maxopen int           // max number of simultanous Open() calls
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
// on speed at which buffers are written to w.
func NewDisk(maxread int, maxwindow int, ahead int64, behind int64, maxopen int, bufkB int) *Disk {

	log.Println("NewDisk", bufkB)
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
		opench:  make(chan (struct{}), maxopen),
		startch: make(chan (struct{})),
		reqch:   make(chan (*File)),
		donech:  make(chan (*File)), // TODO: does a buffer done channel improve speed?
		maxopen: maxopen,
	}

	if bufkB > 0 {
		log.Println("Init bufpool")
		InitPool(defaultBufSize, 1+(bufkB*1024-1)/defaultBufSize)
	}

	// Populate opench (a buffered chanel to limit total concurrent Open() + Read() calls)
	for i := 0; i < maxopen; i++ {
		self.pushtik()
	}

	// start scheduler to prioritise Read() calls
	go self.scheduler(maxread, maxwindow, ahead, behind)

	return self
}

// Start needs to be called once in order to enable file data to start reading.
// If wait > 0 then will wait until that many read requests have been registered before starting
func (d *Disk) Start(wait int) {
	d.wait = wait
	// wait until no Open() calls in progress: drain the open ticket pool
	for i := 0; i < d.maxopen; i++ {
		d.poptik()
	}
	// repopulate the open tickets
	for i := 0; i < d.maxopen; i++ {
		d.pushtik()
	}
	d.startch <- token{}
}

// scheduler manages file.Read() calls to try to process files in disk order
func (self *Disk) scheduler(maxread int, maxwindow int, ahead int64, behind int64) {

	openFiles := 0
	var offset uint64 = 0
	var reqs Queue   // sorted reqs
	var requ Queue   // unsorted reqs
	started := false // whether reading has been started yet by disk.Start

	release := func() {
		if !started {
			return
		}
		if self.wait > 0 && len(reqs)+len(requ) < self.wait {
			// start signal received but not enough pending reads yet
			return
		}
		self.wait = 0
		for len(reqs)+len(requ) > 0 && openFiles < maxread+maxwindow {
			if len(requ) >= len(reqs) {
				// time to merge
				requ.Sort()
				reqs, requ = Merge(reqs, requ)
			}
			//for _, f := range reqs {
			//	log.Printf("       %12d:%s", f.Offset, f.name)
			//}
			// find first file at-or-ahead of disk head position
			i := sort.Search(len(reqs), func(i int) bool { return reqs[i].Offset >= offset })

			// pop() pops a job from the queue and unblocks its read request
			pop := func(i int) {
				popped := reqs[i]
				//log.Printf("popped:%12d:%s", popped.Offset, popped.name)
				if openFiles <= maxread {
					// ahead/behind window 'extras' don't reset offset
					offset = popped.Offset
				}

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
			if i == len(reqs) { // all file are behind current offset
				i = i - 1
			}

			if gap(i) >= 0 && gap(i) <= ahead {
				// found a job in ahead window; release it
				pop(i)
			} else if i > 0 && gap(i-1) <= 0 && gap(i-1) >= -behind {
				// found a job in behind window; release it
				pop(i - 1)
			} else if gap(i) >= 0 && openFiles < maxread {
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
		select {
		case f := <-self.reqch:
			// request to read data from f
			if f == nil {
				// channel closed; we are done
				break
			}

			// adjust offset windows for sector size in windows
			if self.bps == 0 {
				self.bps = bps(f.name)
				ahead = ahead / int64(self.bps)
				behind = behind / int64(self.bps)
			}

			// append to unsorted reqs:
			requ = append(requ, f)
			release()

		case f := <-self.donech:
			// a read job has finished; release pending requests accordingly
			openFiles--
			offset = f.Offset
			release()
		case <-self.startch:
			log.Println("Disk start")
			started = true
			release()
		}
	}
	// clean-up after reqch closed
	if len(reqs) != 0 {
		log.Printf("NON-ZERO OPEN FILES: %d\n", len(reqs))
	}

}

type Work func(path string, data interface{}) //, done chan token)

type Job struct {
	Path   string      // file path
	Offset uint64      // file location on disk
	Work   Work        // callback
	Data   interface{} // callback data
}

func (j *Job) Do() {
	log.Println("hi-ho, hi-ho, it's off to work we go!")
	j.Work(j.Path, j.Data)
}

func (d *Disk) AddJob(path string, poffset *uint64, work Work, data interface{}) {
	j := &Job{Path: path, Work: work, Data: data}
	if poffset == nil {
		j.Offset, _, _, _ = offset(path, 0, 0)
	} else {
		j.Offset = *poffset
	}
	//d.jobch <- j
	j.Do()
}

// poptik grants a ticket to open an fd
func (d *Disk) poptik() {
	<-d.opench
}

// pushtik returns an open fd ticket
func (d *Disk) pushtik() {
	d.opench <- token{}
}

// Closes closes the disk scheduler and frees buffer memory
func (d *Disk) Close() {
	// close bufpool
	if bufc != nil {
		n, err := ClosePool()
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
	reader io.Reader
	disk   *Disk
	size   int64          // file size in bytes (used for deciding how many read buffers)
	name   string         // duplicates *os.File.Name() since File may be closed
	Offset uint64         // file location relative to start of disk
	wg     sync.WaitGroup // for signalling read permission
	isshut bool           // whether *os.File is closed
}

// Open creates a new File object associated with a Disk, and register the
// file's physical position on the disk.  It may block until the disk's
// total open file count falls below the disk's open file limit.
func Open(name string, disk *Disk) (self *File, err error) {

	// wait for open ticket
	disk.poptik()
	defer disk.pushtik()

	self = &File{disk: disk, name: name, isshut: true}
	self.Offset, _, self.size, err = offset(name, 0, 0)
	if err != nil {
		self = nil
	}
	return
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
		f.Close() // TODO: this can't be right
		return
	}

	n, err = f.reader.Read(b)
	if err != nil && err != io.EOF {
		f.Close()
	}

	return
}

// Seek implements the io.Seeker interface.
func (f *File) Seek(offset int64, whence int) (n int64, err error) {
	// open f.file for reading
	err = f.open()
	if err != nil {
		f.Close()
		return 0, err
	}
	return f.file.Seek(offset, whence)

}

// WriteTo implements the io.WriterTo interface.
// Does not support concurrent calls to WriteTo for the same file.
func (f *File) WriteTo(w io.Writer) (int64, error) {
	return f.writeTo(w, nil, -1)
}

func (f *File) LimitWriteTo(w io.Writer, l int64) (int64, error) {
	return f.writeTo(w, nil, l)
}

// SendTo reads from f and sends the data as hddreader buf type to
// the provided channel, then closes the channel.
func (f *File) SendTo(ch chan buf) (n int64, err error) {
	if bufc == nil {
		panic("hddreader: SendTo called but bufferpool not initialised")
	}
	return f.writeTo(nil, ch, f.size)
}

// writeTo does the work for WriteTo and SendTo.
func (f *File) writeTo(w io.Writer, ch chan buf, l int64) (n int64, err error) {

	// open f.file for reading
	err = f.open()
	if err != nil {
		f.Close()
		return 0, err
	}

	if l >= 0 {
		f.reader = io.LimitReader(f.reader, l)
	}

	if bufc != nil {
		// do async write so reading part gets freed up earlier
		return f.writeAsync(w, ch)
	}

	// if w impements the ReaderFrom interface then use w.ReadFrom()
	if w, ok := w.(io.ReaderFrom); ok {
		var m int64
		m, err = w.ReadFrom(f.reader)
		n += int64(m)
		// shut the underlying file
		e := f.shut()
		if e != nil && (err == nil || err == io.EOF) {
			err = e
		}
		// tell the disk that reading is done
		f.disk.donech <- f

		return
	}

	// do 'conventional' read/write
	// TODO: benchmark vs bufio
	b := make([]byte, defaultBufSize)
	for {
		nr, er := f.reader.Read(b)
		if nr > 0 {
			nw, ew := w.Write(b[:nr])
			n += int64(nw)

			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er == io.EOF {
			break
		}

		if er != nil {
			err = er
			break
		}
	}

	e := f.shut()
	if e != nil && err == nil {
		err = e
	}
	// tell the disk that reading is done
	f.disk.donech <- f

	return

}

// do asynchronous read & write, freeing up disk as soon as read is finished
// writes to writer will be in background goroutine.
func (f *File) writeAsync(w io.Writer, ch chan buf) (n int64, err error) {
	var wg sync.WaitGroup
	var werr error // last error during writing

	if w != nil {
		// send buffers to writer
		if ch != nil {
			panic("writeAsync: either w or ch must be nil")
		}
		ch = make(chan (buf), defaultBufPerFile) // TODO: revisit buffer count
		wg.Add(1)
		go func() {
			for b := range ch {
				if werr != nil {
					PutBuf(b)
					continue
				}
				var nw int
				nw, werr = w.Write(b)

				n += int64(nw)
				if nw != len(b) && werr == nil {
					werr = io.ErrShortWrite
				}
				PutBuf(b)
			}
			wg.Done()
		}()
	}

	// do the reading and send to the writer goroutine
	for {
		b := GetBuf()
		nr, er := f.reader.Read(b)
		if nr > 0 {
			ch <- b[:nr]
		} else {
			PutBuf(b)
		}
		if er == io.EOF {
			break
		}

		if er != nil {
			err = er
			break
		}
	}

	for err == nil && werr == nil {
		b := GetBuf()
		var m int
		m, err = f.reader.Read(b)
		if m > 0 {
			// send read data to the writer goroutine
			ch <- b
			n += int64(m)
		} else {
			PutBuf(b)
		}
	}
	if w != nil {
		close(ch)
	}

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

	// wait for writing to finish
	wg.Wait()

	if werr != nil && err == nil {
		err = werr
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
// to start reading.  If l >= 0 then the file is opened as a
func (f *File) open() (err error) {
	if f.isshut {

		f.wg.Add(1)
		f.disk.reqch <- f
		f.wg.Wait()

		f.file, err = os.Open(f.name)
		if err != nil {
			// failed to open, tell disk
			f.disk.donech <- f
		}
		f.reader = f.file
		f.isshut = false
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

type Queue []*File

// implements sort.Interface for sorting by increasing Offset.
func (q Queue) Len() int           { return len(q) }
func (q Queue) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }
func (q Queue) Less(i, j int) bool { return q[i].Offset < q[j].Offset }

// Sort sorts files into ascending disk offset
func (q Queue) Sort() {
	sort.Sort(Queue(q))
}

// merge merges two sorted file queues
func Merge(a Queue, b Queue) (Queue, Queue) {

	if len(a) == 0 {
		return b, nil
	}
	if len(b) == 0 {
		return a, nil
	}

	merged := make(Queue, 0, len(a)+len(b))

	type mergeptr struct {
		q   Queue
		i0  int // index of first uncopied file
		i   int // current file index for comparison
		len int // readability shortcut for len(self.q)
	}

	from := &mergeptr{a, 0, 0, len(a)} // next files will come from a
	then := &mergeptr{b, 0, 0, len(b)} // then from b

	for from.i <= from.len {
		if from.i == from.len {
			// from is finished, write remaining files
			merged = append(merged, from.q[from.i0:]...)
			merged = append(merged, then.q[then.i0:]...)
			break
		}

		if then.q[then.i].Offset < from.q[from.i].Offset {
			// time to switch piles
			merged = append(merged, from.q[from.i0:from.i]...)
			from.i0 = from.i
			from, then = then, from
		}
		from.i++
	}

	// verify sort
	// TODO: remove me
	off := uint64(0)
	for _, f := range merged {
		if f.Offset < off {
			panic("Sort failed")
		}
		off = f.Offset
	}

	return merged, nil
}
