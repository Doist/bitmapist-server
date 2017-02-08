// Package bitmapist implements standalone bitmapist server that can be used
// instead of Redis for https://github.com/Doist/bitmapist library.
package bitmapist

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/artyom/red"
	"github.com/artyom/resp"
	"github.com/boltdb/bolt"
	"github.com/mediocregopher/radix.v2/redis"
)

// New returns initialized Server that loads/saves its data in dbFile
func New(dbFile string) (*Server, error) {
	db, err := bolt.Open(dbFile, 0644, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	s := &Server{
		db:    db,
		log:   noopLogger{},
		keys:  make(map[string]struct{}),
		rm:    make(map[string]struct{}),
		cache: make(map[string]cacheItem),

		done:    make(chan struct{}),
		doneAck: make(chan struct{}),
		save:    make(chan struct{}),
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		if bkt := tx.Bucket([]byte("aux")); bkt != nil {
			var keys []string
			rd := bytes.NewReader(bkt.Get([]byte("keys")))
			if err := gob.NewDecoder(rd).Decode(&keys); err == nil && len(keys) > 0 {
				for _, k := range keys {
					s.keys[k] = struct{}{}
				}
				return nil
			}
		}
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
	go s.loop()
	return s, nil
}

// Register registers supported command handlers on provided srv
func (s *Server) Register(srv *red.Server) {
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
}

// Shutdown performs saves current state on disk and closes database. Shutdown
// blocks until state is saved and database is closed. Server should not be used
// afterwards.
func (s *Server) Shutdown() error {
	s.once.Do(func() { close(s.done); <-s.doneAck })
	return s.db.Close()
}

type cacheItem struct {
	b     *roaring.Bitmap
	aTime int64 // unix timestamp of last access
	dirty bool  // true if has unsaved modifications
}

// Server is a standalone bitmapist server implementation. It's intended to be
// run on top of github.com/artyom/red.Server which handles redis protocol-level
// details and networking.
type Server struct {
	db  *bolt.DB
	log Logger

	once    sync.Once
	done    chan struct{}
	doneAck chan struct{}
	save    chan struct{}

	mu    sync.Mutex
	keys  map[string]struct{}  // all known keys
	rm    map[string]struct{}  // removed but not yet purged
	cache map[string]cacheItem // hot items
}

var bucketName = []byte("bitmapist")

func (s *Server) exists(key string) bool {
	s.mu.Lock()
	s.mu.Unlock()
	_, ok := s.keys[key]
	return ok
}

func (s *Server) putBitmap(withLock bool, key string, bm *roaring.Bitmap) {
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

func (s *Server) getBitmap(key string, create, setDirty bool) (*roaring.Bitmap, error) {
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

func (s *Server) loop() {
	ticker := time.NewTicker(3 * time.Minute)
	defer ticker.Stop()
	defer close(s.doneAck)
	fn := func(text string, f func() error) {
		s.log.Println(text)
		begin := time.Now()
		if err := f(); err != nil {
			s.log.Println(err)
			return
		}
		s.log.Println("saved in", time.Since(begin))
	}
	for {
		select {
		case <-ticker.C:
			fn("periodic saving...", s.persist)
		case <-s.save:
			fn("forced saving...", s.persist)
		case <-s.done:
			fn("final saving...", s.persist)
			return
		}
	}
}

func (s *Server) persist() error {
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
	toPurge := make([]string, 0, len(s.rm))
	for k := range s.rm {
		toPurge = append(toPurge, k)
	}
	s.rm = make(map[string]struct{})
	s.mu.Unlock()
	if len(toPurge) > 0 {
		err := s.db.Update(func(tx *bolt.Tx) error {
			bkt, err := tx.CreateBucketIfNotExists(bucketName)
			if err != nil {
				return err
			}
			for _, k := range toPurge {
				if err := bkt.Delete([]byte(k)); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
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
	s.mu.Lock()
	keys := make([]string, 0, len(s.keys))
	for k := range s.keys {
		keys = append(keys, k)
	}
	s.mu.Unlock()
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(keys); err != nil {
		return err
	}
	err := s.db.Update(func(tx *bolt.Tx) error {
		bkt, err := tx.CreateBucketIfNotExists([]byte("aux"))
		if err != nil {
			return err
		}
		return bkt.Put([]byte("keys"), buf.Bytes())
	})
	return err
}

func (s *Server) handleSlurp(req red.Request) (interface{}, error) {
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

func (s *Server) handleGet(req red.Request) (interface{}, error) {
	if len(req.Args) != 1 {
		return nil, red.ErrWrongArgs
	}
	return s.bitmapBytes(req.Args[0])
}

func (s *Server) handleBgsave(r red.Request) (interface{}, error) {
	if len(r.Args) != 0 {
		return nil, red.ErrWrongArgs
	}
	select {
	case s.save <- struct{}{}:
	default:
	}
	return resp.OK, nil
}

func (s *Server) handleDel(r red.Request) (interface{}, error) {
	if len(r.Args) < 1 {
		return nil, red.ErrWrongArgs
	}
	return int64(s.delete(true, r.Args...)), nil
}

func (s *Server) handleExists(r red.Request) (interface{}, error) {
	if len(r.Args) != 1 {
		return nil, red.ErrWrongArgs
	}
	return s.exists(r.Args[0]), nil
}

func (s *Server) handleBitop(r red.Request) (interface{}, error) {
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

func (s *Server) handleBitcount(r red.Request) (interface{}, error) {
	if len(r.Args) != 1 {
		return nil, red.ErrWrongArgs
	}
	return s.cardinality(r.Args[0])
}

func (s *Server) handleGetbit(r red.Request) (interface{}, error) {
	if len(r.Args) != 2 {
		return nil, red.ErrWrongArgs
	}
	offset, err := strconv.ParseUint(r.Args[1], 10, 32)
	if err != nil {
		return nil, errors.New("bit offset is not an integer or out of range")
	}
	return s.contains(r.Args[0], uint32(offset))
}

func (s *Server) handleSetbit(r red.Request) (interface{}, error) {
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

func (s *Server) handleKeys(r red.Request) (interface{}, error) {
	if len(r.Args) != 1 {
		return nil, red.ErrWrongArgs
	}
	return s.matchingKeys(r.Args[0])
}

func (s *Server) handleInfo(r red.Request) (interface{}, error) {
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

func (s *Server) handleScan(r red.Request) (interface{}, error) {
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

func (s *Server) setBit(key string, offset uint32) (bool, error) {
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

func (s *Server) clearBit(key string, offset uint32) (bool, error) {
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

func (s *Server) contains(key string, offset uint32) (bool, error) {
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

func (s *Server) matchingKeys(pattern string) ([]string, error) {
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

func (s *Server) cardinality(key string) (int64, error) {
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

func (s *Server) bitopAnd(key string, sources []string) (int64, error) {
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
	max, err := maxValue(xbm)
	if err != nil {
		return 0, err
	}
	s.putBitmap(false, key, xbm)
	sz := max / 8
	if max%8 > 0 {
		sz++
	}
	return int64(sz), nil
}

func (s *Server) bitopOr(key string, sources []string) (int64, error) {
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
	max, err := maxValue(xbm)
	if err != nil {
		return 0, err
	}
	s.putBitmap(false, key, xbm)
	sz := max / 8
	if max%8 > 0 {
		sz++
	}
	return int64(sz), nil
}

func (s *Server) bitopXor(key string, sources []string) (int64, error) {
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
	max, err := maxValue(xbm)
	if err != nil {
		return 0, err
	}
	s.putBitmap(false, key, xbm)
	sz := max / 8
	if max%8 > 0 {
		sz++
	}
	return int64(sz), nil
}

func (s *Server) bitopNot(dst, src string) (int64, error) {
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
	max, err := maxValue(b1)
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
	return int64(upper / 8), nil
}

func (s *Server) delete(withLock bool, keys ...string) int {
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
		s.rm[k] = struct{}{}
	}
	return cnt
}

func (s *Server) bitmapBytes(key string) ([]byte, error) {
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
	max, err := maxValue(bm)
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

// Backup writes current on-disk saved database to Writer w. It's not safe to
// copy database file while it's used, so this method can be used to get
// a consistent copy of database.
func (s *Server) Backup(w io.Writer) error {
	return s.db.View(func(tx *bolt.Tx) error {
		_, err := tx.WriteTo(w)
		return err
	})
}

func (s *Server) redisImport(addr string) error {
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

// Logger is a set of methods used to log information. *log.Logger implements
// this interface.
type Logger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

// WithLogger configures server to use provided Logger.
func (s *Server) WithLogger(l Logger) {
	if l == nil {
		s.log = noopLogger{}
		return
	}
	s.log = l
}

type noopLogger struct{}

func (noopLogger) Print(v ...interface{})                 {}
func (noopLogger) Printf(format string, v ...interface{}) {}
func (noopLogger) Println(v ...interface{})               {}

func maxValue(b *roaring.Bitmap) (uint32, error) {
	card := b.GetCardinality()
	if card == 0 {
		return 0, nil
	}
	return b.Select(uint32(card - 1))
}
