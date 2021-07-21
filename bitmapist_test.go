package main

import (
	"bufio"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/artyom/red"
	"github.com/artyom/resp"
)

func TestServer(t *testing.T) {
	cf, cleanup := newServer(t, false)
	defer cleanup()
	conn := cf()
	defer conn.Close()
	rd := bufio.NewReader(conn)
	checkResponse(t, "setbit foo 10 1", int64(0), rd, conn)
	checkResponse(t, "setbit foo 10 1", int64(1), rd, conn)
	checkResponse(t, "setbit foo 7 1", int64(0), rd, conn)
	checkResponse(t, "setbit bar 7 1", int64(0), rd, conn)
	checkResponse(t, "setbit bar 20 1", int64(0), rd, conn)
	checkResponse(t, "setbit bar 20 0", int64(1), rd, conn)
	checkResponse(t, "setbit baz 8 1", int64(0), rd, conn)

	checkResponse(t, "get foo", "\x01 ", rd, conn)
	checkResponse(t, "get bar", "\x01", rd, conn)
	checkResponse(t, "get baz", "\x00\x80", rd, conn)

	checkResponse(t, "expire dst 60", int64(0), rd, conn)

	checkResponse(t, "bitop or dst foo bar", int64(2), rd, conn)
	checkResponse(t, "bitcount dst", int64(2), rd, conn)
	checkResponse(t, "getbit dst 10", int64(1), rd, conn)
	checkResponse(t, "getbit dst 7", int64(1), rd, conn)
	checkResponse(t, "getbit dst 0", int64(0), rd, conn)
	checkResponse(t, "get dst", "\x01 ", rd, conn)

	checkResponse(t, "ttl nonexistent", int64(-2), rd, conn)
	checkResponse(t, "ttl dst", int64(-1), rd, conn)
	checkResponse(t, "expire dst 60", int64(1), rd, conn)
	checkResponse(t, "ttl dst", int64(60), rd, conn)

	checkResponse(t, "bitop xor dst foo bar", int64(2), rd, conn)
	checkResponse(t, "bitcount dst", int64(1), rd, conn)
	checkResponse(t, "getbit dst 10", int64(1), rd, conn)
	checkResponse(t, "getbit dst 7", int64(0), rd, conn)
	checkResponse(t, "get dst", "\x00 ", rd, conn)

	// ensure that bitop does not retain ttl of its destination
	checkResponse(t, "ttl dst", int64(-1), rd, conn)

	checkResponse(t, "bitop xor dst foo bar baz", int64(2), rd, conn)
	checkResponse(t, "bitcount dst", int64(2), rd, conn)
	checkResponse(t, "getbit dst 10", int64(1), rd, conn)
	checkResponse(t, "getbit dst 8", int64(1), rd, conn)
	checkResponse(t, "getbit dst 7", int64(0), rd, conn)
	checkResponse(t, "get dst", "\x00\xa0", rd, conn)
	checkResponse(t, "set dst \x00\xa0", "OK", rd, conn)
	checkResponse(t, "get dst", "\x00\xa0", rd, conn)

	checkResponse(t, "bitop and dst foo bar", int64(1), rd, conn)
	checkResponse(t, "bitcount dst", int64(1), rd, conn)
	checkResponse(t, "getbit dst 10", int64(0), rd, conn)
	checkResponse(t, "getbit dst 7", int64(1), rd, conn)
	// this differs from redis which returns 2-byte response since it'd
	// allocate array to fit longest source
	checkResponse(t, "get dst", "\x01", rd, conn)

	checkResponse(t, "bitop not dst baz", int64(2), rd, conn)
	checkResponse(t, "bitcount dst", int64(15), rd, conn)
	checkResponse(t, "getbit dst 8", int64(0), rd, conn)
	checkResponse(t, "getbit dst 15", int64(1), rd, conn)
	checkResponse(t, "getbit dst 16", int64(0), rd, conn)
	checkResponse(t, "get dst", "\xff\x7f", rd, conn)
	checkResponse(t, "set dst \xff\x7f", "OK", rd, conn)
	checkResponse(t, "get dst", "\xff\x7f", rd, conn)

	checkResponse(t, "setbit src_rename 1 1", int64(0), rd, conn)
	checkResponse(t, "expire src_rename 60", int64(1), rd, conn)
	checkResponse(t, "rename src_rename dst_rename", "OK", rd, conn)
	checkResponse(t, "ttl dst_rename", int64(60), rd, conn)
	checkResponse(t, "getbit dst_rename 1", int64(1), rd, conn)
}

func TestServer_handleKeys(t *testing.T) {
	cf, cleanup := newServer(t, false)
	defer cleanup()
	conn := cf()
	defer conn.Close()
	rd := bufio.NewReader(conn)
	checkResponse(t, "keys foo*", resp.Array{}, rd, conn)
}

func checkResponse(t testing.TB, req string, respWanted interface{}, rd resp.BytesReader, wr io.Writer) {
	t.Helper()
	if err := resp.Encode(wr, strings.Fields(req)); err != nil {
		t.Fatal(err)
	}
	response, err := resp.Decode(rd)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(respWanted, response) {
		t.Logf("op:\t%q", req)
		t.Fatalf("mismatch:\n got:\t%#v\nwant:\t%#v", response, respWanted)
	}
}

func newServer(t testing.TB, relaxed bool) (fn clientConnFunc, cleanup func()) {
	td, err := ioutil.TempDir("", "bitmapist-test-")
	if err != nil {
		t.Fatal(err)
		return
	}
	srv, err := New(filepath.Join(td, "temp.db"), relaxed)
	if err != nil {
		os.RemoveAll(td)
		t.Fatal(err)
		return
	}
	redsrv := red.NewServer()
	srv.Register(redsrv)
	unixSocket := filepath.Join(td, "bitmapist.sock")
	ln, err := net.Listen("unix", unixSocket)
	if err != nil {
		t.Fatal(err)
		return
	}
	go func() { redsrv.Serve(ln) }()
	fn = func() net.Conn {
		conn, err := net.DialTimeout("unix", unixSocket, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
			t.Fatal(err)
		}
		return conn
	}
	cleanup = func() {
		ln.Close()
		if err := srv.Shutdown(); err != nil {
			t.Error(err)
		}
		os.RemoveAll(td)
	}
	return fn, cleanup
}

type clientConnFunc func() net.Conn

func TestServerPersistence(t *testing.T) {
	td, err := ioutil.TempDir("", "bitmapist-test-")
	if err != nil {
		t.Fatal(err)
		return
	}
	defer os.RemoveAll(td)
	newServerDirectory := func(t testing.TB) (fn clientConnFunc, cleanup func()) {
		srv, err := New(filepath.Join(td, "temp.db"), false)
		if err != nil {
			t.Fatal(err)
			return
		}
		redsrv := red.NewServer()
		srv.Register(redsrv)
		unixSocket := filepath.Join(td, "bitmapist.sock")
		ln, err := net.Listen("unix", unixSocket)
		if err != nil {
			t.Fatal(err)
			return
		}
		go func() { redsrv.Serve(ln) }()
		fn = func() net.Conn {
			conn, err := net.DialTimeout("unix", unixSocket, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
				t.Fatal(err)
			}
			return conn
		}
		cleanup = func() {
			ln.Close()
			if err := srv.Shutdown(); err != nil {
				t.Error(err)
			}
		}
		return fn, cleanup
	}
	cf, cleanup := newServerDirectory(t)
	conn := cf()
	rd := bufio.NewReader(conn)
	checkResponse(t, "setbit foo 10 1", int64(0), rd, conn)
	checkResponse(t, "setbit foo 7 1", int64(0), rd, conn)
	conn.Close()
	cleanup()

	// second launch should load data from disk
	cf, cleanup = newServerDirectory(t)
	defer cleanup()
	conn = cf()
	rd = bufio.NewReader(conn)
	defer conn.Close()
	checkResponse(t, "get foo", "\x01 ", rd, conn)
}
