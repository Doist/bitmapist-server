package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/artyom/autoflags"
	"github.com/tidwall/redcon"
)

func main() {
	args := struct {
		Addr string `flag:"addr,address to listen"`
		File string `flag:"dump,path to dump file"`
	}{
		Addr: "localhost:6379",
	}
	autoflags.Define(&args)
	flag.Parse()

	s := newSrv()

	rs := redcon.NewServer(args.Addr, s.redisHandler, nil, nil)
	if err := rs.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func newSrv() *srv {
	return &srv{
		log:     log.New(os.Stderr, "", log.LstdFlags),
		bitmaps: make(map[string]*roaring.Bitmap),
	}
}

type srv struct {
	log *log.Logger

	mu      sync.Mutex
	bitmaps map[string]*roaring.Bitmap
}

func (s *srv) redisHandler(conn redcon.Conn, cmd redcon.Command) {
	switch strings.ToLower(string(cmd.Args[0])) {
	default:
		conn.WriteError(fmt.Sprintf("ERR unsupported command '%s'", cmd.Args[0]))
	case "ping":
		conn.WriteString("PONG")
	case "quit":
		conn.WriteString("OK")
		conn.Close()
	case "keys":
		s.handleKeys(conn, cmd.Args)
	case "setbit":
		s.handleSetbit(conn, cmd.Args)
	case "getbit":
		s.handleGetbit(conn, cmd.Args)
	case "bitcount":
		s.handleBitcount(conn, cmd.Args)
	case "bitop":
		s.handleBitop(conn, cmd.Args)
	case "exists":
		s.handleExists(conn, cmd.Args)
	case "del":
		s.handleDel(conn, cmd.Args)
	case "get":
		s.handleGet(conn, cmd.Args)
	}
}

func (s *srv) handleGet(conn redcon.Conn, args [][]byte) {
	if len(args) < 2 {
		conn.WriteError(errWrongArguments)
		return
	}
	switch b := s.bitmapBytes(string(args[1])); b {
	case nil:
		conn.WriteNull()
	default:
		conn.WriteBulk(b)
	}
}

func (s *srv) handleDel(conn redcon.Conn, args [][]byte) {
	if len(args) < 2 {
		conn.WriteError(errWrongArguments)
		return
	}
	keys := make([]string, len(args[1:]))
	for i, k := range args[1:] {
		keys[i] = string(k)
	}
	conn.WriteInt(s.delete(keys...))
}

func (s *srv) handleExists(conn redcon.Conn, args [][]byte) {
	if len(args) != 2 {
		conn.WriteError(errWrongArguments)
		return
	}
	switch {
	case s.exists(string(args[1])):
		conn.WriteInt(1)
	default:
		conn.WriteInt(0)
	}
}

func (s *srv) handleBitop(conn redcon.Conn, args [][]byte) {
	if len(args) < 4 {
		conn.WriteError(errWrongArguments)
		return
	}
	op := strings.ToLower(string(args[1]))
	switch op {
	default:
		conn.WriteError(errWrongArguments)
		return
	case "and", "or", "xor":
		if len(args) < 5 {
			conn.WriteError(errWrongArguments)
			return
		}
	case "not":
		conn.WriteError("ERR bitop not is not currently unsupported")
		return
	}
	dst := string(args[2])
	sources := make([]string, len(args[3:]))
	for i, src := range args[3:] {
		sources[i] = string(src)
	}
	var out int64
	switch op {
	case "and":
		out = s.bitopAnd(dst, sources)
	case "or":
		out = s.bitopOr(dst, sources)
	case "xor":
		out = s.bitopXor(dst, sources)
	}
	conn.WriteInt64(out)
}

func (s *srv) handleBitcount(conn redcon.Conn, args [][]byte) {
	if len(args) != 2 {
		conn.WriteError(errWrongArguments)
		return
	}
	conn.WriteInt(s.cardinality(string(args[1])))
}

func (s *srv) handleGetbit(conn redcon.Conn, args [][]byte) {
	if len(args) != 3 {
		conn.WriteError(errWrongArguments)
		return
	}
	if len(args[1]) == 0 {
		conn.WriteError(errBadKeyName)
		return
	}
	offset, err := strconv.ParseUint(string(args[2]), 10, 32)
	if err != nil {
		conn.WriteError("ERR bit offset is not an integer or out of range")
		return
	}
	switch {
	case s.contains(string(args[1]), uint32(offset)):
		conn.WriteInt(1)
	default:
		conn.WriteInt(0)
	}
}

func (s *srv) handleSetbit(conn redcon.Conn, args [][]byte) {
	if len(args) != 4 || len(args[3]) == 0 {
		conn.WriteError(errWrongArguments)
		return
	}
	if len(args[1]) == 0 {
		conn.WriteError(errBadKeyName)
		return
	}
	offset, err := strconv.ParseUint(string(args[2]), 10, 32)
	if err != nil {
		conn.WriteError("ERR bit offset is not an integer or out of range")
		return
	}
	switch args[3][0] {
	case '0':
		switch {
		case s.clearBit(string(args[1]), uint32(offset)):
			conn.WriteInt(1)
		default:
			conn.WriteInt(0)
		}
	case '1':
		switch {
		case s.setBit(string(args[1]), uint32(offset)):
			conn.WriteInt(0)
		default:
			conn.WriteInt(1)
		}
	default:
		conn.WriteError(errWrongArguments)
		return
	}
}

func (s *srv) handleKeys(conn redcon.Conn, args [][]byte) {
	if len(args) != 2 {
		conn.WriteError(errWrongArguments)
		return
	}
	keys, err := s.keys(string(args[1]))
	if err != nil {
		conn.WriteError(fmt.Sprintf("ERR %v", err))
		return
	}
	conn.WriteArray(len(keys))
	for _, k := range keys {
		conn.WriteBulkString(k)
	}
}

func (s *srv) setBit(key string, offset uint32) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	bm, ok := s.bitmaps[key]
	if !ok {
		bm = roaring.New()
		s.bitmaps[key] = bm
	}
	return bm.CheckedAdd(offset)
}

func (s *srv) clearBit(key string, offset uint32) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	bm, ok := s.bitmaps[key]
	if !ok {
		bm = roaring.New()
		s.bitmaps[key] = bm
	}
	return bm.CheckedRemove(offset)
}

func (s *srv) contains(key string, offset uint32) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	bm, ok := s.bitmaps[key]
	if !ok {
		bm = roaring.New()
		s.bitmaps[key] = bm
	}
	return bm.Contains(offset)
}

func (s *srv) keys(pattern string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var keys []string
	for k := range s.bitmaps {
		ok, err := filepath.Match(pattern, k)
		if err != nil {
			return nil, err
		}
		if ok {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func (s *srv) cardinality(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	bm, ok := s.bitmaps[key]
	if !ok {
		bm = roaring.New()
		s.bitmaps[key] = bm
	}
	return int(bm.GetCardinality())
}

func (s *srv) bitopAnd(key string, sources []string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var src []*roaring.Bitmap
	for _, k := range sources {
		if bm, ok := s.bitmaps[k]; ok {
			src = append(src, bm)
		}
	}
	s.bitmaps[key] = roaring.FastAnd(src...)
	return int64(s.bitmaps[key].GetCardinality())
}

func (s *srv) bitopOr(key string, sources []string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var src []*roaring.Bitmap
	for _, k := range sources {
		if bm, ok := s.bitmaps[k]; ok {
			src = append(src, bm)
		}
	}
	s.bitmaps[key] = roaring.FastOr(src...)
	return int64(s.bitmaps[key].GetCardinality())
}

func (s *srv) bitopXor(key string, sources []string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var src []*roaring.Bitmap
	for _, k := range sources {
		if bm, ok := s.bitmaps[k]; ok {
			src = append(src, bm)
		}
	}
	s.bitmaps[key] = roaring.HeapXor(src...)
	return int64(s.bitmaps[key].GetCardinality())
}

func (s *srv) delete(keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var cnt int
	for _, k := range keys {
		if _, ok := s.bitmaps[k]; ok {
			cnt++
		}
		delete(s.bitmaps, k)
	}
	return cnt
}

func (s *srv) bitmapBytes(key string) []byte {
	s.mu.Lock()
	bm, ok := s.bitmaps[key]
	if !ok {
		s.mu.Unlock()
		return nil
	}
	bm = bm.Clone()
	s.mu.Unlock()
	var buf []byte
	var curPos int
	var cur byte
	for it := bm.Iterator(); it.HasNext(); {
		n := it.Next()
		pos, bit := int(n/8), byte(1<<byte(n%8))
		if pos == curPos {
			cur |= bit
			continue
		}
		buf = growBuf(buf, curPos)
		buf = append(buf, revbits(cur))
		curPos, cur = pos, bit
	}
	if len(buf) <= curPos {
		buf = growBuf(buf, curPos)
		buf = append(buf, revbits(cur))
	}
	return buf
}

func (s *srv) exists(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.bitmaps[key]
	return ok
}

func revbits(b byte) byte {
	b = (b&0xf0)>>4 | (b&0x0f)<<4
	b = (b&0xcc)>>2 | (b&0x33)<<2
	b = (b&0xaa)>>1 | (b&0x55)<<1
	return b
}

func growBuf(buf []byte, upTo int) []byte {
	// TODO: optimize to increase in larger chunks when possible
	for s := len(buf); s < upTo; s++ {
		buf = append(buf, 0)
	}
	return buf
}

var (
	errWrongArguments = "ERR wrong command arguments"
	errBadKeyName     = "ERR bad key name"
)
