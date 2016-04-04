package hddreader_test

import (
        "testing"
        "os"
        "github.com/SeeSpotRun/go-disk-utils/hddreader"
        "sync"
        "bytes"
        "io"
)

type testfile struct {
    name string
    data string
    fakeoffset uint64
}

var testfiles = []testfile {
    { "file1", "orld", 42 },
    { "file2", "Hel",   1 },
    { "file3", "lo W", 16 },
}

func makeFiles(t *testing.T) {
        for _, x := range(testfiles) {
                fd, err := os.Create(x.name)
                if err != nil {
                    t.Errorf("makeTree: %v", err)
                    return
                }
                _, err = fd.WriteString(x.data)
                if err != nil {
                    t.Errorf("makeTree: %v", err)
                    return
                }
                fd.Close()
        }
}

func TestRead(t *testing.T) {

        makeFiles(t)

        var wg sync.WaitGroup

        for i:=0; i<100; i++ {

                // create disk with no read buffer
                d := hddreader.NewDisk(1, 0, 0, 0, 100, 0)
                var b bytes.Buffer

                for _, x := range(testfiles) {
                        wg.Add(1)
                        go func(x testfile) {
                                defer wg.Done()
                                f, err := hddreader.Open(x.name, d)
                                if err != nil {
                                        t.Errorf("TestRead: %v", err)
                                        return
                                }
                                defer f.Close()
                                // set fake file offset so files get read in correct order
                                f.Offset = x.fakeoffset
                                // try to write data (will block until d.Start() called)
                                io.Copy(&b, f)
                        }(x)
                }
                d.Start(len(testfiles))
                wg.Wait()

                expected := "Hello World"
                got := b.String()
                if got != expected {
                        t.Errorf("TestRead: expected %s, got %s, iteration %d", expected, got, i+1)
                }
                d.Close()
        }

        // cleanup
        for _, x := range(testfiles) {
                if err := os.Remove(x.name); err != nil {
                        t.Errorf("remove %s: %v", x.name, err)
                }
        }
}
