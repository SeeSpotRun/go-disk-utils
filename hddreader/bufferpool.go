package hddreader

// this file provides the Bufferpool implementation

import (
	"errors"
	"fmt"
	"runtime"
)

type buf []byte

var bufc chan buf
var reqc chan token
var nbufs int

// InitPool initialises the buffer pool.  It takes arguments
// bufsize (buffer size in bytes) and maxbufs (a hard limit on
// the number of buffers created)
func InitPool(bufsize int, maxbufs int) {
	if bufc != nil {
		panic("hddreader/InitPool: Attempt to re-init pool")
	}
	bufc = make(chan (buf), maxbufs)
	reqc = make(chan (token))

	go func() {
		// if any requests received then make buffer, up to maxbufs limit
		for _ = range reqc {
			if nbufs < maxbufs {
				nbufs++
				bufc <- make(buf, bufsize)
			}
		}
	}()
}

// GetBuf gets a buffer from the pool
func GetBuf() buf {
	select {
	case b := <-bufc:
		// recycled buffer
		return b
	default:
		// no buffers in pool; request new one
		reqc <- token{}
		// wait for buffer to arrive
		return <-bufc
	}
}

// PutBuf returns a buffer to the pool
func PutBuf(b buf) {
	select {
	// note: b is expanded to cap(b) before returning
	case bufc <- b[:cap(b)]: // should not block
	default:
		panic("Blocked returning buffer")
	}
}

// close closes the Bufferpool and returns the number of buffers used
func ClosePool() (used int, err error) {
	close(reqc)
	close(bufc)
	used = 0
	for _ = range bufc {
		used++
	}
	runtime.GC()
	if used != nbufs {
		err = errors.New(fmt.Sprintf("Expected %d buffers, got %d", nbufs, used))
	}
	return
}
