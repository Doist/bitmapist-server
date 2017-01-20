package main

import (
	"archive/tar"
	"bytes"
	"errors"
	"flag"
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
	"github.com/artyom/red"
	"github.com/artyom/resp"
	"github.com/golang/snappy"
	"github.com/mediocregopher/radix.v2/redis"
)

func main() {
	args := struct {
		Addr string        `flag:"addr,address to listen"`
		File string        `flag:"dump,path to dump file"`
		Save time.Duration `flag:"dump.every,period to automatically save state"`
	}{
		Addr: "localhost:6379",
		File: "dump.tar.sz",
		Save: 10 * time.Minute,
	}
	autoflags.Define(&args)
	flag.Parse()

	s := newSrv(args.File)
	if args.File != "" {
		log.Println("loading data from", args.File)
		begin := time.Now()
		if err := s.restore(); err != nil {
			log.Fatal(err)
		}
		log.Println("state restored in", time.Since(begin))
		go runEvery(args.Save, s.persist)
	}
	srv := red.NewServer()
	srv.WithLogger(log.New(os.Stderr, "", log.LstdFlags))
	srv.Handle("keys", s.handleKeys)
	srv.Handle("setbit", s.handleSetbit)
	srv.Handle("getbit", s.handleGetbit)
	srv.Handle("bitcount", s.handleBitcount)
	srv.Handle("bitop", s.handleBitop)
	srv.Handle("exists", s.handleExists)
	srv.Handle("del", s.handleDel)
	srv.Handle("get", s.handleGet)
	srv.Handle("bgsave", s.handleBgsave)
	srv.Handle("slurp", s.handleSlurp)
	srv.Handle("scan", s.handleScan)
	srv.Handle("select", handleSelect)
	log.Fatal(srv.ListenAndServe(args.Addr))
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
			if err := os.Chmod(tf.Name(), 0644); err != nil {
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

func (s *srv) handleSlurp(req red.Request) (interface{}, error) {
	if len(req.Args) != 1 {
		return nil, red.ErrWrongArgs
	}
	addr := req.Args[0]
	go func() {
		s.log.Println("importing redis dataset from", addr)
		begin := time.Now()
		if err := s.redisImport(addr); err != nil {
			s.log.Println("redis import error:", err)
			return
		}
		s.log.Println("import from redis completed in", time.Since(begin))
	}()
	return resp.OK, nil
}

func (s *srv) handleGet(req red.Request) (interface{}, error) {
	if len(req.Args) != 1 {
		return nil, red.ErrWrongArgs
	}
	return s.bitmapBytes(req.Args[0])
}

func (s *srv) handleBgsave(r red.Request) (interface{}, error) {
	if len(r.Args) != 0 {
		return nil, red.ErrWrongArgs
	}
	if s.saveFile == "" {
		return nil, errors.New("no save file configured")
	}
	s.persist()
	return resp.OK, nil
}

func (s *srv) handleDel(r red.Request) (interface{}, error) {
	if len(r.Args) < 1 {
		return nil, red.ErrWrongArgs
	}
	return int64(s.delete(r.Args...)), nil
}

func (s *srv) handleExists(r red.Request) (interface{}, error) {
	if len(r.Args) != 1 {
		return nil, red.ErrWrongArgs
	}
	return s.exists(r.Args[0]), nil
}

func (s *srv) handleBitop(r red.Request) (interface{}, error) {
	if len(r.Args) < 3 {
		return nil, red.ErrWrongArgs
	}
	op := strings.ToLower(r.Args[0])
	switch op {
	default:
		return nil, red.ErrWrongArgs
	case "and", "or", "xor":
		if len(r.Args) < 4 {
			return nil, red.ErrWrongArgs
		}
	case "not":
		if len(r.Args) != 3 {
			return nil, red.ErrWrongArgs
		}
		return s.bitopNot(r.Args[1], r.Args[2])
	}
	dst := r.Args[1]
	sources := r.Args[2:]
	var out int64
	switch op {
	case "and":
		out = s.bitopAnd(dst, sources)
	case "or":
		out = s.bitopOr(dst, sources)
	case "xor":
		out = s.bitopXor(dst, sources)
	}
	return out, nil
}

func (s *srv) handleBitcount(r red.Request) (interface{}, error) {
	if len(r.Args) != 1 {
		return nil, red.ErrWrongArgs
	}
	return int64(s.cardinality(r.Args[0])), nil
}

func (s *srv) handleGetbit(r red.Request) (interface{}, error) {
	if len(r.Args) != 2 {
		return nil, red.ErrWrongArgs
	}
	offset, err := strconv.ParseUint(r.Args[1], 10, 32)
	if err != nil {
		return nil, errors.New("bit offset is not an integer or out of range")
	}
	return s.contains(r.Args[0], uint32(offset)), nil
}

func (s *srv) handleSetbit(r red.Request) (interface{}, error) {
	if len(r.Args) != 3 {
		return nil, red.ErrWrongArgs
	}
	offset, err := strconv.ParseUint(r.Args[1], 10, 32)
	if err != nil {
		return nil, errors.New("bit offset is not an integer or out of range")
	}
	switch r.Args[2] {
	case "0":
		return s.clearBit(r.Args[0], uint32(offset)), nil
	case "1":
		return !s.setBit(r.Args[0], uint32(offset)), nil
	}
	return nil, red.ErrWrongArgs
}

func (s *srv) handleKeys(r red.Request) (interface{}, error) {
	if len(r.Args) != 1 {
		return nil, red.ErrWrongArgs
	}
	return s.keys(r.Args[0])
}

func (s *srv) handleScan(r red.Request) (interface{}, error) {
	// SCAN 0 MATCH trackist_* COUNT 2
	if len(r.Args) != 5 || r.Args[0] != "0" ||
		strings.ToLower(r.Args[1]) != "match" ||
		strings.ToLower(r.Args[3]) != "count" {
		return nil, red.ErrWrongArgs
	}
	keys, err := s.keys(r.Args[2])
	if err != nil {
		return nil, err
	}
	return resp.Array{"0", keys}, nil
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
	keys := []string{}
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
		return 0
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
			continue
		}
		if len(src) > 0 {
			// mix of found and missing keys, result would be empty
			// (but set) bitmap
			s.bitmaps[key] = roaring.NewBitmap()
			return 0
		}
	}
	if len(src) == 0 {
		delete(s.bitmaps, key)
		return 0
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
	if len(src) == 0 {
		delete(s.bitmaps, key)
		return 0
	}
	s.bitmaps[key] = roaring.FastOr(src...)
	return int64(s.bitmaps[key].GetCardinality())
}

func (s *srv) bitopXor(key string, sources []string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var src []*roaring.Bitmap
	var found bool
	for _, k := range sources {
		if bm, ok := s.bitmaps[k]; ok {
			src = append(src, bm)
			found = true
			continue
		}
		src = append(src, roaring.NewBitmap())
	}
	if !found {
		delete(s.bitmaps, key)
		return 0
	}
	s.bitmaps[key] = roaring.HeapXor(src...)
	return int64(s.bitmaps[key].GetCardinality())
}

func (s *srv) bitopNot(dst, src string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b1, ok := s.bitmaps[src]
	if !ok {
		delete(s.bitmaps, dst)
		return 0, nil
	}
	max, err := b1.Select(uint32(b1.GetCardinality() - 1))
	if err != nil {
		return 0, err
	}
	upper := uint64(max + 1) // +1 because [rangeStart,rangeEnd)
	// when redis does BITOP NOT, it operates on byte boundary, so resulting
	// bitmap may have last byte padded with ones - mimick this by moving
	// upper bound to fit byte boundaries
	if x := upper % 8; x != 0 {
		upper += (8 - x)
	}
	b2 := roaring.Flip(b1, 0, upper)
	s.bitmaps[dst] = b2
	return int64(b2.GetCardinality()), nil
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

func (s *srv) bitmapBytes(key string) ([]byte, error) {
	s.mu.Lock()
	bm, ok := s.bitmaps[key]
	if !ok {
		s.mu.Unlock()
		return nil, nil
	}
	bm = bm.Clone()
	s.mu.Unlock()
	if bm.GetCardinality() == 0 {
		return []byte{}, nil
	}
	max, err := bm.Select(uint32(bm.GetCardinality() - 1))
	if err != nil {
		return nil, err
	}
	buf := make([]byte, int(max/8)+1)
	var curPos int
	var cur byte
	for it := bm.Iterator(); it.HasNext(); {
		n := it.Next()
		pos, bit := int(n/8), byte(1<<byte(n%8))
		if pos == curPos {
			cur |= bit
			continue
		}
		buf[curPos] = revbits(cur)
		curPos, cur = pos, bit
	}
	buf[curPos] = revbits(cur)
	return buf, nil
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

func handleSelect(r red.Request) (interface{}, error) {
	if len(r.Args) != 1 {
		return nil, red.ErrWrongArgs
	}
	switch r.Args[0] {
	case "0":
		return resp.OK, nil
	default:
		return nil, errors.New("invalid DB index")
	}
}

func runEvery(d time.Duration, f func()) {
	if d <= 0 {
		return
	}
	if d < time.Minute {
		d = time.Minute
	}
	for range time.Tick(d) {
		f()
	}
}
