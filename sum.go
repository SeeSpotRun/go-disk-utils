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

// sum is a demo main for the hddreader package.
// Various optimisations can be tuned or turned off.
package main

/*
 * TODO:
 * [ ] write sum_test
 * [ ] benchmarking & profiling
 * [x] switch from flag to docopt :-)
 * [x] copyright etc
 * [ ] support multiple hashes
 * [ ] reflect settings in run summary
*/

import (
        "crypto"
        _ "crypto/md5"
        _ "crypto/sha1"
        _ "crypto/sha256"
        _ "crypto/sha512"
        "hash"
        "fmt"
        "os"
        "io"
        "runtime"
        "runtime/pprof"
        "log"
        "time"
        "sync"
        "strconv"
        //
        "github.com/docopt/docopt-go"
        // local packages:
        "github.com/SeeSpotRun/go-disk-utils/walk"
        "github.com/SeeSpotRun/go-disk-utils/hddreader"
)



// map hashname to crypto.Hash
var hashtypes []crypto.Hash
const defaulthash = crypto.SHA1
var hashnames []string = []string{
        crypto.MD4:               "MD4",                // import golang.org/x/crypto/md4crypto.MD4
        crypto.MD5:               "MD5",                // import crypto/md5ypto.MD5
        crypto.SHA1:              "SHA1",               // import crypto/sha1ypto.SHA1
        crypto.SHA224:            "SHA224",             // import crypto/sha256
        crypto.SHA256:            "SHA256",             // import crypto/sha256
        crypto.SHA384:            "SHA384",             // import crypto/sha512
        crypto.SHA512:            "SHA512",             // import crypto/sha512
        crypto.MD5SHA1:           "MD5SHA1",            // no implementation; MD5+SHA1 used for TLS RSA
        crypto.RIPEMD160:         "RIPEMD160",          // import golang.org/x/crypto/ripemd160
        crypto.SHA3_224:          "SHA3_224",           // import golang.org/x/crypto/sha3
        crypto.SHA3_256:          "SHA3_256",           // import golang.org/x/crypto/sha3
        crypto.SHA3_384:          "SHA3_384",           // import golang.org/x/crypto/sha3
        crypto.SHA3_512:          "SHA3_512",           // import golang.org/x/crypto/sha3
        crypto.SHA512_224:        "SHA512_224",         // import crypto/sha512
        crypto.SHA512_256:        "SHA512_256",         // import crypto/sha512
}



//////////////////////////////////////////////////////////////////


func int64arg(a string, args map[string]interface{}) (result int64, ok bool) {
        var s string
        s, ok = args[a].(string)
        if ok {
                var err error
                result, err = strconv.ParseInt(s, 10, 64)
                ok = err==nil
        }
        return
}

func intarg(a string, args map[string]interface{}) (result int, ok bool) {
        r, ok := int64arg(a, args)
        result = int(r)
        return
}

func main() {

        // start timer...
        t1 := time.Now()

        usage := `Usage:
    sum [options] [--open=N] <path>...
    sum --hdd [--read=N] [--window=N] [--ahead=N] [--behind=N] [--max=N] [options] <path>...

Options:
  -h --help     Show this screen.
  -r --recurse  Recurse paths if they are folders
  -p --procs=N  Limit number of simultaneous processes (defaults to NumCPU)
  --minsize=N   Ignore files smaller than N bytes [default: 1]
  --maxsize=N   Ignore files larger than N bytes [default: -1]
  --open=N      Limit number of open file handles to N [default: 10]
  -d --hdd      Use hddreader to optimise reads
  --read=N      Limit number of files reading simultaneously [default: 1]
  --ahead=<kB>  Files within this many kB ahead of disk head ignore 'read' limit [default: 1024]
  --behind=<kB> Files within this many kB behind disk head ignore 'read' limit [default: 0]
  --window=N    Limit number of ahead/behind file exceptions [default: 5]
  --handles=N   Limit number of open file handles to N [default: 100]
  --whilewalk   Don't wait for folder walk to finish before starting hashing
  --buffer=<kB> Use a bufferpool to buffer disk reads
  --cpuprofile=<file>  Write cpu profile to file`
        // add all available hash types to usage string
        for i, n := range(hashnames) {
                if crypto.Hash(i).Available() {
                        usage = usage + fmt.Sprintf("  --%-10s  Calculate %s hash\n", n, n)
                }
        }


        // parse args
        args, _ := docopt.Parse(usage, os.Args[1:], true, "sum 0.1", false)

        cpuprofile, ok := args["--cpuprofile"].(string)
        if ok {
                f, err := os.Create(cpuprofile)
                if err != nil {
                        panic(err)
                }
                pprof.StartCPUProfile(f)
                defer pprof.StopCPUProfile()
        }


        // set hash type[s]
        for i := range(hashnames) {
                if y, _ := args["--" + hashnames[i]].(bool); y {
                        hashtypes = append(hashtypes, crypto.Hash(i))
                }
        }
        if len(hashtypes) == 0 {
                hashtypes = append(hashtypes, defaulthash)
        }
        
        // set number of processes
        procs, ok := intarg("--procs", args)
        if ok {
                runtime.GOMAXPROCS(procs)
        } else {
                runtime.GOMAXPROCS(runtime.NumCPU())
        }
                
        // set up scheduler based on user args...
        hdd, ok := args["--hdd"].(bool);
        var disk *hddreader.Disk
        var tickets chan struct{}
        if  ok && hdd {
                // disk does scheduling if option hdd == true
                bufkB, _ := intarg("--buffer", args)
                ahead, _ := int64arg("--ahead", args)
                behind, _ := int64arg("--behind", args)
                readlimit, _ := intarg("--read", args)
                windowlimit, _ := intarg("--window", args)
                openlimit, _ := intarg("--handles", args)
                disk = hddreader.NewDisk(readlimit, windowlimit, ahead * 1024, behind * 1024, openlimit, bufkB)
                disk.Start(0)  // enables reading
        } else {
                // tickets limit number of active files when hdd==false
                openlimit, _ := intarg("--open", args)
                if openlimit <= 0 {
                        panic("Need --open > 0")
                }
                tickets = make(chan(struct{}), int(openlimit))
                for i:=0; i<int(openlimit); i++ {
                        tickets <- struct{}{}
                }
        }

        var wg sync.WaitGroup // callback for sum() calls

        // sum(path) hash file contents and prints results
        sum := func(path string) {
                defer wg.Done()

                var fi io.ReadCloser
                var err error

                if hdd {
                        // open the file using hddreader as limiter
                        fi, err = hddreader.Open(path, disk)
                } else {
                        // use tickets channel to limit number of open files
                        _ = <- tickets
                        defer func() {
                                tickets <- struct{}{}
                        }()
                        fi, err = os.Open(path)
                }

                if err != nil {
                        log.Printf("Could not open %s: %s\n", path, err)
                        return
                }
                defer fi.Close()

                // build a multiwriter to hash the file contents
                w := make([]io.Writer, 0, len(hashtypes))
                for _, t := range(hashtypes) {
                        w = append(w, t.New())
                }
                m := io.MultiWriter(w...)


                _, err = io.Copy(m, fi)

                if err != nil {
                        log.Printf("Failed hashing %s", err)
                } else {
                        // build a single line for output
                        var results string
                        for _, s := range(w) {
                                sum, ok := s.(hash.Hash)
                                if !ok {
                                        panic("Can't cast io.Writer back to hash.Hash")
                                }
                                results = results + fmt.Sprintf("%x : ", sum.Sum(nil))
                        }
                        fmt.Printf("%s%s\n", results, path)
                }
        }

        // set up for walk...
        paths, ok := args["<path>"].([]string)
        maxsize, ok := int64arg("--maxsize", args)
        minsize, ok := int64arg("--minsize", args)
        whilewalk, ok := args["--whilewalk"].(bool)
        walkopts := walk.Defaults
        if recurse, _ := args["--recurse"].(bool); !recurse {
                walkopts += walk.NoRecurse
        }
        
        // error reporting during walk:
        errc := make(chan error)
        go func() {
                for err := range errc {
                        log.Printf("Walk error: %s\n", err)
                }
        }()

        // map for paths collected during walk
        pathmap := make(map[string]struct{})

        // do the actual walk
        for f := range(walk.FileCh(nil, errc, paths, walkopts)) {
                // filter based on size
                if maxsize >= 0 && f.Info.Size() > maxsize {
                        continue}
                if f.Info.Size() < minsize {
                        continue
                }

                if whilewalk {
                        // start processing immediately
                        wg.Add(1)
                        go sum(f.Path)
                } else {
                        // process after walk finished
                        pathmap[f.Path] = struct{}{}
                }
        }

        log.Printf("Walk time %d ms\n", time.Now().Sub(t1) / time.Millisecond)

        if !whilewalk {
                for p := range(pathmap) {
                        wg.Add(1)
                        go sum(p)
                }
        }

        // wait for all sum() goroutines to finish
        wg.Wait()

        if hdd {
                disk.Close()
        }
        
        log.Printf("Total time %d ms\n", time.Now().Sub(t1) / time.Millisecond)

}

