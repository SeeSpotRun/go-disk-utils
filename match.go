// match is a rough implementation of rmlint's duplicate finding algorithm in go.

package main

// The duplicate finding algorithm is as follows:
//
// 1. Walk the user-input path[s], collecting regular files
// 2. Group files by size
// 3. Discard groups with only 1 file
// 4. Calculate hash for first part of each remaining file.
// 5. Regroup the resulting partially hashed files into groups of same size and hash
// 6. Repeat 3-5, hashing a bit more of each file each generation, until the end of the file is reached
//
// File mathing on hard disks is optimised by sorting the jobs in (4) above into ascending order
// of disk offsets, to reduces seek times and disk thrash.
//
// A high level of concurrency is employed, so for example:
// * Hash calculations are separated from reading operations, so disk reading can continue uninterrupted
// * Iteration of 3-6 above is concurrent, so some files may be on their second or third increment before
// others have started their first.
//
// Worker routines:
// * Walker (one per root path) to walk the root directories
// * Grouper (one for each generation of each group of partially-matched files) to do the hash matching
//   TODO: is this overkill?
// * Sorter (one per disk) to sort the files into disk offset order
// * Reader (one per disk) to read the file data
// * Flusher (multiple) to flush the read data to the checksum calculator
// * Reporter (one) to output matched groups as they are completed
//
// Channels:
// * Group.newc to send new files to a group (1 per Group)
// * Disk.sortc to send files from the Grouper to the Sorter (1 per Disk)
// * Disk.readc to send files from the Sorter to the Reader (1 per Disk)
// * flushc for the reader to pass the file on to the Flusher (1)
// * Disk.popc for the reader to request the next file from the Sorter (1 per Disk)
// * Group.hashc for the Flusher to send the hashed result back to the Grouper (1 per Group)

import (
	"asyncwriter"
	SUM "crypto/sha1" // imported as alias SUM so that hash algorithm can be changed easily
	"errors"
	"fmt"
	"hash"
	"io"
	"log"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"
	// local packages:
	"offset"
	"walk"
)

// Constants:
const SortByOffset = true               // process files in order of disk offset (TODO: move to flag) // TODO: fix false case
const Incr0 int64 = 4096 * 4            // size of first hash increment
const IncrMult int64 = 8                // multiplier for subsequent increments
const IncrMax int64 = 256 * 1024 * 1024 // maximum hash increment size
const MaxBufPages = 16                  // maximum buffer size for bufio.Writer

// The flusher channel conveys flushjobs from the Reader to the Flusher goroutines.
// Since the Reader can be a workflow bottleneck due to io limitations, we need
// a decent buffer size to minimise blocking of the reader.
const flushchannelBufferSize = 20
const ReporterChannelBufferSize = 4

// Sum is a convenience type for storing hash results
type Sum [SUM.Size]byte

// File represents a match candidate file
type File struct {
	path   string    // the absolute file path
	size   int64     // file size in bytes
	hashed int64     // the number of bytes hashed so far
	mtime  time.Time // file modification time (unused)
	hash   hash.Hash // checksum calculator
	sum    Sum       // copy of the current checksum result
	diskID int       // disk identifier (eg dev number in Linux)
	offset int64     // physical offset of the file from the start of the disk
	err    error     // error encountered when trying to read file
	disk   *Disk     // pointer to the Disk object for this file
	group  *Group    // pointer to the Group object to which the file currently belongs
}

// File.isFinal is a convenience function which returns true if the file has been fully hashed
func (self *File) isFinal() bool {
	return self.hashed == self.size
}

func (self *File) Hash() {
	if file.hash == nil {
		file.hash = SUM.New()
	}

	defer func(f *File) {
		if f.err != nil {
			hashedc <- file
		}
	}(file)

	fi, err := hddreader.Open(file.path)
	if err != nil {
		file.err = err
		return
	}

	defer fi.Close()

	// seek to start position
	_, file.err = fi.Seek(file.hashed, 0)
	if file.err != nil {
		return
	}

	bytes := bytesToRead(file)

	written, err := fi.WriteTo(
				writer := asyncwriter.New(file.hash, 1024*8, 64, bufpool)

				written, err := writer.ReadFrom(fi)
				file.hashed += int64(written)

				if err != nil && err != io.EOF {
					file.err = err
					return
				}

				if int64(written) != bytes && file.hashed != file.size {
					file.err = errors.New("Unexpected number of bytes read")
					return
				}

				if file.offset >= 0 {
					// remember head position for next iteration
					offset = file.offset
				}
				flushc <- &FlushJob{writer, file}
				hashedc <- j.file
	*/
}

// The Group struct is an object representing a group of files which might all be duplicates
type Group struct {
	files    []*File  // files which are identical based on partial hashes so far
	active   bool     // true if group files have been sent out for hashing
	refs     int      // reference count (1 for incoming channel and 1 for each pending hash)
	children []*Group // child groups are sub-groups of this group's files, which have
	// identical hashes after the _next_ hash increment.
}

// Group.isFinal is a convenience function which returns true if the files in the group have been fully hashed
func NewGroup() *Group {
	return &Group{refs: 1}
}

// Group.isFinal is a convenience function which returns true if the files in the group have been fully hashed
func (self *Group) isFinal() bool {
	return self.files != nil && self.files[0].isFinal()
}

// Group.qualifies returns true if the Group.files meets all criteria for a duplicate set.
// Typically this just means 2 or more [partially] matched files.
func (self *Group) qualifies() bool {
	return self.files != nil && len(self.files) > 1
	//TODO: can add additional criteria here later for options such as --must-match-tagged
}

// Group.add adds a File to the Group.  It assumes the calling routine has taken care of
// any concurrency issues.  It returns the net increase in number of potential duplicates
// (for a group with 1 file, adding a second file typically increases the number of potential
// duplicates by 2, while adding a third file increases the potential duplicates by just one)
func (self *Group) add(file *File) int {
	file.group = self
	self.files = append(self.files, file)
	if self.active {
		// send file to Disk sorter
		self.refs++
		go file.Hash()
		return 1
	} else {
		// check if it's time to go active yet
		if self.qualifies() && !self.isFinal() {
			// go active
			self.active = true
			self.refs += len(self.files)
			added := len(self.files)
			// send all pre-added files to sorter for [further] hashing
			for _, f := range self.files {
				go f.Hash()
			}
			return added
		}
		return 0
	}
}

// Group.send adds a File to the Group.  It should only be called from a single process thread to avoid
// concurrency issues.  It returns the net increase in number of potential duplicates (see Group.add())
func (self *Group) send(file *File, addc chan<- *File) int {
	file.group = self
	if self.active == true {
		// active group; send the file via newc channel
		// note: file.group must be set to target group before sending to addc
		addc <- file
		return 1
	} else {
		return self.add(file)
	}
}

// Group.unref decreases the reference count for Group; if this reaches zero then the Group sends itself,
// if appropriate, to the Reporter via reporterc
func (self *Group) unref() {
	if self == nil {
		panic("Error: unref of nil group")
	}
	self.refs--
	if self.refs < 0 {
		panic("Group.unref yields negative reference count")
	}
	if self.refs == 0 {
		if self.active {
			// process children
			for _, child := range self.children {
				child.unref()
			}
			//self.children = nil
			//self.files = nil
		} else {
			// Group which didn't launch; this will be either single files with
			// no duplicates, or groups of fully-hashed duplicates.  Reporter() will
			// decide what to do in each case, then will dispose of the files
			// TODO: assert len(self.files) > 0
			// TODO: this is a very indirect way to find reporterc
			self.files[0].disk.reporterc <- self
		}
	}
}

// Group.detach is called during initial Group build to tell the Group not to expect any new files
func (self *Group) detach(detachc chan<- *Group) {
	if self == nil {
		return
	}
	if self.active {
		// not safe to access Group directly; send to channel
		detachc <- self
	} else {
		self.unref()
	}
}

// Grouper is a goroutine which handles all concurrent group operations.  Note that Group
// operations during initial Group build, up to the poing of the Group going active, are
// not concurrent and are handled by Group.send()
func Grouper(donec <-chan struct{}, detachc <-chan *Group, addc <-chan *File, hashedc <-chan *File, filewg *sync.WaitGroup) {
	// handles all concurrent access to Groups.  Note that
	for {
		select {
		case group := <-detachc:
			// close group to new files
			group.unref()
		case file := <-addc:
			// add a new file to an active group (note: file.group must already be set to target group)
			file.group.add(file)
		case file := <-hashedc:
			// process file that has returned from hashing
			group := file.group
			if file.err != nil {
				// files with errors during hashing are discarded
				log.Printf("Error hashing %s: %s", file.path, file.err)
				filewg.Done()
			} else {
				copy(file.sum[:], file.hash.Sum(nil)[:SUM.Size])
				// linear search for matching child group
				// TODO: revert to map? (simpler code but more resources)
				var child *Group
				for _, g := range group.children {
					if g.files[0].sum == file.sum { // TODO: g.sum
						child = g
						break
					}
				}
				if child == nil {
					// no matching child group; create one
					child = NewGroup()
					group.children = append(group.children, child)
				}
				child.add(file)
			}
			group.unref()
		case <-donec:
			return
		}
	}
}

// byteToRead calculates the number of bytes for the next hash increment of a file
func bytesToRead(file *File) int64 {
	var result int64 = Incr0
	if file.offset != 0 {
		result = file.offset * IncrMult
		if result > IncrMax {
			result = IncrMax
		}
		if result > file.size {
			result = file.size
		}
	}
	return result
}

// Reporter receives Groups, checks if they are confirmed duplicates, and outputs
// an fdupes-like output of duplicate files.  All files in recieved groups are unreferenced via
// the filewg WaitGroup.
func Reporter(reporterc chan *Group, filewg *sync.WaitGroup) {
	// print blank line before first dupe group
	fmt.Println()

	for g := range reporterc {
		if g.isFinal() && g.qualifies() {
			for _, f := range g.files {
				if f.err == nil {
					//fmt.Printf("%x: %s\n", f.sum, f.path)
					fmt.Printf("%s\n", f.path)
				} else {
					fmt.Printf("ERROR FILE:%s\n", f.path)
				}
			}
			// blank line after each dupe group
			fmt.Println()
		}

		// unreference files
		filewg.Add(-len(g.files))
		// try to free up some memory
		g.files = nil
	}
}

// BySize implements sort.Interface for sorting a slice of File
// pointers by increasing File.size.
type BySize []*File

func (f BySize) Len() int           { return len(f) }
func (f BySize) Swap(i, j int)      { f[i], f[j] = f[j], f[i] }
func (f BySize) Less(i, j int) bool { return f[i].size < f[j].size }

// Launcher sets up our workflow channels last-to-first then
// calls Walker which starts pumping files into the workflow.
func Launcher(roots []string) error {

	// create common channels
	// The done channel is a catch-all to shut down all remaining goroutines
	donec := make(chan struct{})
	defer close(donec)
	// the flusher allows the Readers to hand jobs over the the Flushers to they can keep reading
	flushc := make(chan *FlushJob, flushchannelBufferSize)
	defer close(flushc)

	hashedc := make(chan *File)
	detachc := make(chan *Group)

	// reference counter for files
	var filewg sync.WaitGroup

	// the report channel conveys completed file groups to the Reporter
	reporterc := make(chan *Group, ReporterChannelBufferSize)
	defer close(reporterc)
	go Reporter(reporterc, &filewg)

	// Start a heap of Flushers, with waitgroup and joiner.
	// There is probably no benefit in having more flushers
	// than we have CPU's.
	numFlushers := runtime.NumCPU()
	var flushwg sync.WaitGroup
	flushwg.Add(numFlushers)
	for i := 0; i < numFlushers; i++ {
		go func() {
			Flusher(flushc, hashedc)
			flushwg.Done()
		}()
	}
	disks := make(map[int]*Disk)

	// set up error reporting during walk
	errc := make(chan error)
	go func() {
		for err := range errc {
			log.Printf("Walk error: %s\n", err)
		}
	}()

	log.Printf("Adding files...\n")
	files := []*File{}
	for f := range walk.FileCh(donec, errc, roots, walk.Defaults) {
		if size := f.Info.Size(); size != 0 {
			file := &File{path: f.Path,
				size:   f.Info.Size(),
				mtime:  f.Info.ModTime(),
				diskID: 1, /* TODO */
				offset: -1}
			disk := disks[file.diskID]
			if disk == nil {
				// create new disk
				disk = newDisk(file, flushc, reporterc, hashedc, &filewg)
				defer disk.close()
				disks[file.diskID] = disk
			}
			// associate file with the Disk
			file.disk = disk
			files = append(files, file)
		}
	}

	close(errc)
	log.Printf("Files found: %d\n", len(files))

	// group files[] by size and store groups with size > 1 in groups[] slice
	sort.Sort(BySize(files))
	log.Printf("Sort by size complete\n")
	var size int64 = -1 // size of current group
	var group *Group = nil
	candidates := 0
	filewg.Add(len(files))
	addc := make(chan (*File))
	go Grouper(donec, detachc, addc, hashedc, &filewg)

	for _, file := range files {
		if file.size != size {
			// detach old group
			group.detach(detachc)
			//start new group
			group = NewGroup()
			size = file.size
		}
		candidates += group.send(file, addc)
	}

	// detach last group
	group.detach(detachc)

	log.Printf("Duplicate candidates: %d\n", candidates)

	// wait for all files to finish processing
	filewg.Wait()

	log.Println("All files accounted for")

	return nil
}

func main() {
	// TODO: parse user input args / flags
	runtime.GOMAXPROCS(runtime.NumCPU())

	// call Launcher which orchestrates the file matching
	err := Launcher(os.Args[1:])

	if err != nil {
		log.Println("Launcher returned error ", err)
	}
}
