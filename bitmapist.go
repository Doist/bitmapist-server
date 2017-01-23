// Command bitmapist implements standalone bitmapist-compatible server
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
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
	"github.com/boltdb/bolt"
	"github.com/mediocregopher/radix.v2/redis"
)

func main() {
	args := struct {
		Addr string        `flag:"addr,address to listen"`
		File string        `flag:"dump,path to dump file"`
		Save time.Duration `flag:"dump.every,period to automatically save state"`
	}{
		Addr: "localhost:6379",
		File: "bitmapist.db",
		Save: 3 * time.Minute,
	}
	autoflags.Define(&args)
	flag.Parse()

	log.Println("loading data from", args.File)
	begin := time.Now()
	s, err := newSrv(args.File)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("loaded in", time.Since(begin))
	go runEvery(args.Save, s.persist)

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
	srv.Handle("info", s.handleInfo)
	srv.Handle("select", handleSelect)
	log.Fatal(srv.ListenAndServe(args.Addr))
}

func newSrv(dbFile string) (*srv, error) {
	db, err := bolt.Open(dbFile, 0644, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	s := &srv{
		db:    db,
		log:   log.New(os.Stderr, "", log.LstdFlags),
		keys:  make(map[string]struct{}),
		cache: make(map[string]cacheItem),
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(bucketName)
		if bkt == nil {
			return nil
		}
		fn := func(k, v []byte) error { s.keys[string(k)] = struct{}{}; return nil }
		return bkt.ForEach(fn)
	})
	if err != nil {
		s.db.Close()
		return nil, err
	}
	return s, nil
}

type cacheItem struct {
	b     *roaring.Bitmap
	aTime int64 // unix timestamp of last access
	dirty bool  // true if has unsaved modifications
}

type srv struct {
	db  *bolt.DB
	log *log.Logger

	mu    sync.Mutex
	keys  map[string]struct{}  // all known keys
	cache map[string]cacheItem // hot items
}

var bucketName = []byte("bitmapist")

func (s *srv) exists(key string) bool {
	s.mu.Lock()
	s.mu.Unlock()
	_, ok := s.keys[key]
	return ok
}

func (s *srv) putBitmap(withLock bool, key string, bm *roaring.Bitmap) {
	if withLock {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	v := cacheItem{
		b:     bm,
		aTime: time.Now().Unix(),
		dirty: true,
	}
	s.keys[key] = struct{}{}
	s.cache[key] = v
}

func (s *srv) getBitmap(key string, create, setDirty bool) (*roaring.Bitmap, error) {
	if _, ok := s.keys[key]; !ok {
		if !create {
			return nil, nil
		}
		s.keys[key] = struct{}{}
		s.cache[key] = cacheItem{
			b:     roaring.NewBitmap(),
			aTime: time.Now().Unix(),
			dirty: true,
		}
		return s.cache[key].b, nil
	}
	if v, ok := s.cache[key]; ok {
		v.aTime = time.Now().Unix()
		if !v.dirty && setDirty {
			v.dirty = true
		}
		s.cache[key] = v
		return v.b, nil
	}
	v := cacheItem{
		aTime: time.Now().Unix(),
		dirty: setDirty,
	}
	err := s.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(bucketName)
		if bkt == nil {
			return errors.New("bucket not found")
		}
		bm := roaring.NewBitmap()
		if err := bm.UnmarshalBinary(bkt.Get([]byte(key))); err != nil {
			return err
		}
		v.b = bm
		return nil
	})
	if err != nil {
		return nil, err
	}
	s.cache[key] = v
	return v.b, nil
}

func (s *srv) persist() error {
	now := time.Now().Unix()
	s.mu.Lock()
	dirty := make([]string, 0, len(s.cache))
	for k, v := range s.cache {
		if !v.dirty {
			if v.aTime < now-300 {
				delete(s.cache, k)
			}
			continue
		}
		dirty = append(dirty, k)
	}
	s.mu.Unlock()
	for _, k := range dirty {
		s.mu.Lock()
		v, ok := s.cache[k]
		if !ok {
			s.mu.Unlock()
			continue
		}
		data, err := v.b.ToBytes()
		if err != nil {
			s.mu.Unlock()
			return err
		}
		v.dirty = false
		s.cache[k] = v
		s.mu.Unlock()
		err = s.db.Update(func(tx *bolt.Tx) error {
			bkt, err := tx.CreateBucketIfNotExists(bucketName)
			if err != nil {
				return err
			}
			return bkt.Put([]byte(k), data)
		})
		if err != nil {
			s.mu.Lock()
			if v, ok := s.cache[k]; ok {
				v.dirty = true
				s.cache[k] = v
			}
			s.mu.Unlock()
			return err
		}
	}
	return nil
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
	go func() {
		if err := s.persist(); err != nil {
			s.log.Println("state save error:", err)
		}
	}()
	return resp.OK, nil
}

func (s *srv) handleDel(r red.Request) (interface{}, error) {
	if len(r.Args) < 1 {
		return nil, red.ErrWrongArgs
	}
	return int64(s.delete(true, r.Args...)), nil
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
	switch op {
	case "and":
		return s.bitopAnd(dst, sources)
	case "or":
		return s.bitopOr(dst, sources)
	case "xor":
		return s.bitopXor(dst, sources)
	}
	return 0, errors.New("unhandled operation")
}

func (s *srv) handleBitcount(r red.Request) (interface{}, error) {
	if len(r.Args) != 1 {
		return nil, red.ErrWrongArgs
	}
	return s.cardinality(r.Args[0])
}

func (s *srv) handleGetbit(r red.Request) (interface{}, error) {
	if len(r.Args) != 2 {
		return nil, red.ErrWrongArgs
	}
	offset, err := strconv.ParseUint(r.Args[1], 10, 32)
	if err != nil {
		return nil, errors.New("bit offset is not an integer or out of range")
	}
	return s.contains(r.Args[0], uint32(offset))
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
		return s.clearBit(r.Args[0], uint32(offset))
	case "1":
		ok, err := s.setBit(r.Args[0], uint32(offset))
		return !ok, err
	}
	return nil, red.ErrWrongArgs
}

func (s *srv) handleKeys(r red.Request) (interface{}, error) {
	if len(r.Args) != 1 {
		return nil, red.ErrWrongArgs
	}
	return s.matchingKeys(r.Args[0])
}

func (s *srv) handleInfo(r red.Request) (interface{}, error) {
	if len(r.Args) != 1 {
		return nil, red.ErrWrongArgs
	}
	switch strings.ToLower(r.Args[0]) {
	case "keys":
		buf := new(bytes.Buffer)
		s.mu.Lock()
		defer s.mu.Unlock()
		fmt.Fprintf(buf, "keys_total:%d\n", len(s.keys))
		fmt.Fprintf(buf, "keys_cached:%d\n", len(s.cache))
		return buf.Bytes(), nil
	default:
		return nil, red.ErrWrongArgs
	}
}

func (s *srv) handleScan(r red.Request) (interface{}, error) {
	// SCAN 0 MATCH trackist_* COUNT 2
	if len(r.Args) != 5 || r.Args[0] != "0" ||
		strings.ToLower(r.Args[1]) != "match" ||
		strings.ToLower(r.Args[3]) != "count" {
		return nil, red.ErrWrongArgs
	}
	keys, err := s.matchingKeys(r.Args[2])
	if err != nil {
		return nil, err
	}
	return resp.Array{"0", keys}, nil
}

func (s *srv) setBit(key string, offset uint32) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	bm, err := s.getBitmap(key, true, true)
	if err != nil {
		return false, err
	}
	if bm == nil {
		bm = roaring.NewBitmap()
		s.putBitmap(false, key, bm)
	}
	return bm.CheckedAdd(offset), nil
}

func (s *srv) clearBit(key string, offset uint32) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	bm, err := s.getBitmap(key, true, true)
	if err != nil {
		return false, err
	}
	if bm == nil {
		bm = roaring.NewBitmap()
		s.putBitmap(false, key, bm)
	}
	return bm.CheckedRemove(offset), nil
}

func (s *srv) contains(key string, offset uint32) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	bm, err := s.getBitmap(key, false, false)
	if err != nil {
		return false, err
	}
	if bm == nil {
		return false, nil
	}
	return bm.Contains(offset), nil
}

func (s *srv) matchingKeys(pattern string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var keys []string
	for k := range s.keys {
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

func (s *srv) cardinality(key string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	bm, err := s.getBitmap(key, false, false)
	if err != nil {
		return 0, err
	}
	if bm == nil {
		return 0, nil
	}
	return int64(bm.GetCardinality()), nil
}

func (s *srv) bitopAnd(key string, sources []string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var src []*roaring.Bitmap
	for _, k := range sources {
		bm, err := s.getBitmap(k, false, false)
		if err != nil {
			return 0, err
		}
		if bm != nil {
			src = append(src, bm)
			continue
		}
		if len(src) > 0 {
			// mix of found and missing keys, result would be empty
			// (but set) bitmap
			s.putBitmap(false, key, roaring.NewBitmap())
			return 0, nil
		}
	}
	if len(src) == 0 {
		s.delete(false, key)
		return 0, nil
	}
	xbm := roaring.FastAnd(src...)
	s.putBitmap(false, key, xbm)
	return int64(xbm.GetCardinality()), nil
}

func (s *srv) bitopOr(key string, sources []string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var src []*roaring.Bitmap
	for _, k := range sources {
		bm, err := s.getBitmap(k, false, false)
		if err != nil {
			return 0, err
		}
		if bm != nil {
			src = append(src, bm)
		}
	}
	if len(src) == 0 {
		s.delete(false, key)
		return 0, nil
	}
	xbm := roaring.FastOr(src...)
	s.putBitmap(false, key, xbm)
	return int64(xbm.GetCardinality()), nil
}

func (s *srv) bitopXor(key string, sources []string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var src []*roaring.Bitmap
	var found bool
	for _, k := range sources {
		bm, err := s.getBitmap(k, false, false)
		if err != nil {
			return 0, err
		}
		if bm != nil {
			src = append(src, bm)
			found = true
			continue
		}
		src = append(src, roaring.NewBitmap())
	}
	if !found {
		s.delete(false, key)
		return 0, nil
	}
	xbm := roaring.HeapXor(src...)
	s.putBitmap(false, key, xbm)
	return int64(xbm.GetCardinality()), nil
}

func (s *srv) bitopNot(dst, src string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b1, err := s.getBitmap(src, false, false)
	if err != nil {
		return 0, err
	}
	if b1 == nil {
		s.delete(false, dst)
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
	s.putBitmap(false, dst, b2)
	return int64(b2.GetCardinality()), nil
}

func (s *srv) delete(withLock bool, keys ...string) int {
	if withLock {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	var cnt int
	for _, k := range keys {
		if _, ok := s.keys[k]; !ok {
			continue
		}
		cnt++
		delete(s.keys, k)
		delete(s.cache, k)
	}
	if cnt > 0 {
		err := s.db.Update(func(tx *bolt.Tx) error {
			bkt := tx.Bucket(bucketName)
			if bkt == nil {
				return nil
			}
			for _, k := range keys {
				if err := bkt.Delete([]byte(k)); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			s.log.Println("error deleting keys:", err)
		}
	}
	return cnt
}

func (s *srv) bitmapBytes(key string) ([]byte, error) {
	s.mu.Lock()
	bm, err := s.getBitmap(key, false, false)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	if bm == nil {
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
	var vals []uint32
	var done bool
	cursor := "0"
	for !done {
		a, err := client.Cmd("SCAN", cursor, "count", "10000").Array()
		if err != nil {
			return err
		}
		if len(a) != 2 {
			return fmt.Errorf("bad response array length: %d", len(a))
		}
		cursor, err = a[0].Str()
		if err != nil {
			return err
		}
		if cursor == "0" {
			done = true
		}
		keys, err := a[1].List()
		if err != nil {
			return err
		}
		for _, key := range keys {
			if key == "" {
				continue
			}
			resp := client.Cmd("GET", key)
			if resp.Err != nil {
				if resp.IsType(redis.AppErr) {
					s.log.Printf("skip load of key %q: %v", key, resp.Err)
					continue
				}
				return resp.Err
			}
			if !resp.IsType(redis.BulkStr) {
				s.log.Printf("skip load of key %q: not a string", key)
				continue
			}
			data, err := resp.Bytes()
			if err != nil {
				return err
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
			v := cacheItem{
				b:     roaring.BitmapOf(vals...),
				aTime: time.Now().Unix(),
				dirty: true,
			}
			s.mu.Lock()
			s.keys[key] = struct{}{}
			s.cache[key] = v
			s.mu.Unlock()
		}
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

func runEvery(d time.Duration, f func() error) {
	if d <= 0 {
		return
	}
	if d < time.Minute {
		d = time.Minute
	}
	for t := range time.Tick(d) {
		log.Println("saving state...")
		if err := f(); err != nil {
			log.Println("error saving state:", err)
		} else {
			log.Println("saved in", time.Since(t))
		}

	}
}
