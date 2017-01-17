package main

import (
	"archive/tar"
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/artyom/autoflags"
	"github.com/golang/snappy"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/tidwall/redcon"
)

func main() {
	args := struct {
		Addr string `flag:"addr,address to listen"`
		File string `flag:"dump,path to dump file"`
	}{
		Addr: "localhost:6379",
		File: "dump.tar.sz",
	}
	autoflags.Define(&args)
	flag.Parse()

	s := newSrv(args.File)
	if args.File != "" {
		log.Println("loading data from", args.File)
	}
	begin := time.Now()
	if err := s.restore(); err != nil {
		log.Fatal(err)
	}
	if args.File != "" {
		log.Println("state restored in", time.Since(begin))
	}

	rs := redcon.NewServer(args.Addr, s.redisHandler, nil, nil)
	if err := rs.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func newSrv(saveFile string) *srv {
	return &srv{
		saveFile: saveFile,
		log:      log.New(os.Stderr, "", log.LstdFlags),
		bitmaps:  make(map[string]*roaring.Bitmap),
	}
}

type srv struct {
	saveFile string
	log      *log.Logger

	mu      sync.Mutex
	bitmaps map[string]*roaring.Bitmap
	saving  bool
}

func (s *srv) restore() error {
	if s.saveFile == "" {
		return nil
	}
	f, err := os.Open(s.saveFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	tr := tar.NewReader(snappy.NewReader(f))
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		bm := roaring.NewBitmap()
		if _, err := bm.ReadFrom(tr); err != nil {
			return err
		}
		s.mu.Lock()
		s.bitmaps[hdr.Name] = bm
		s.mu.Unlock()
	}
	return nil
}

func (s *srv) persist() {
	if s.saveFile == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saving {
		return
	}
	s.saving = true
	go func() {
		err := func() error {
			s.log.Println("saving state...")
			defer func() {
				s.mu.Lock()
				s.saving = false
				s.mu.Unlock()
			}()
			tf, err := ioutil.TempFile(filepath.Dir(s.saveFile), "bitmapist-temp-")
			if err != nil {
				return err
			}
			defer tf.Close()
			defer os.Remove(tf.Name())
			sw := snappy.NewBufferedWriter(tf)
			defer sw.Close()
			tw := tar.NewWriter(sw)
			defer tw.Close()

			s.mu.Lock()
			keys := make([]string, 0, len(s.bitmaps))
			for k := range s.bitmaps {
				keys = append(keys, k)
			}
			s.mu.Unlock()

			begin := time.Now()
			buf := new(bytes.Buffer)
			for _, k := range keys {
				s.mu.Lock()
				bm, ok := s.bitmaps[k]
				if !ok {
					s.mu.Unlock()
					continue
				}
				bm = bm.Clone()
				s.mu.Unlock()
				buf.Reset()
				if _, err := bm.WriteTo(buf); err != nil {
					return err
				}
				hdr := &tar.Header{
					Name:    k,
					Mode:    0644,
					Size:    int64(buf.Len()),
					ModTime: time.Now(),
				}
				if err := tw.WriteHeader(hdr); err != nil {
					return err
				}
				if _, err := buf.WriteTo(tw); err != nil {
					return err
				}
			}
			if err := tw.Close(); err != nil {
				return err
			}
			if err := sw.Close(); err != nil {
				return err
			}
			if err := tf.Close(); err != nil {
				return err
			}
			if err := os.Rename(tf.Name(), s.saveFile); err != nil {
				return err
			}
			s.log.Printf("saved %d bitmaps in %v", len(keys), time.Since(begin))
			return nil
		}()
		if err != nil {
			s.log.Println("error saving state:", err)
		}
	}()
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
	case "bgsave":
		s.handleBgsave(conn, cmd.Args)
	case "slurp":
		s.handleSlurp(conn, cmd.Args)
	}
}

func (s *srv) handleSlurp(conn redcon.Conn, args [][]byte) {
	if len(args) != 2 {
		conn.WriteError(errWrongArguments)
		return
	}
	addr := string(args[1])
	go func() {
		s.log.Println("importing redis dataset from", addr)
		begin := time.Now()
		if err := s.redisImport(addr); err != nil {
			s.log.Println("redis import error:", err)
			return
		}
		s.log.Println("import from redis completed in", time.Since(begin))
	}()
	conn.WriteString("OK")
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

func (s *srv) handleBgsave(conn redcon.Conn, args [][]byte) {
	if len(args) != 1 {
		conn.WriteError(errWrongArguments)
		return
	}
	if s.saveFile == "" {
		conn.WriteError("ERR no save file configured")
		return
	}
	s.persist()
	conn.WriteString("OK")
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

func (s *srv) redisImport(addr string) error {
	client, err := redis.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer client.Close()
	mr, err := client.Cmd("KEYS", "*").Array()
	if err != nil {
		return err
	}
	var vals []uint32
	for _, r := range mr {
		key, err := r.Str()
		if err != nil {
			continue
		}
		data, err := client.Cmd("GET", key).Bytes()
		if err != nil {
			continue
		}
		vals = vals[:0]
		for i, b := range data {
			if b == 0 {
				continue
			}
			for j := byte(0); j <= 7; j++ {
				if b&(1<<(7-j)) != 0 {
					vals = append(vals, uint32(i*8)+uint32(j))
				}
			}
		}
		if len(vals) == 0 {
			continue
		}
		bm := roaring.BitmapOf(vals...)
		s.mu.Lock()
		s.bitmaps[key] = bm
		s.mu.Unlock()
	}
	return nil
}
