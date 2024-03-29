// Package bitmapist implements standalone bitmapist server that can be used
// instead of Redis for https://github.com/Doist/bitmapist library.
package bitmapist

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
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
	"github.com/golang/snappy"
	"github.com/mediocregopher/radix.v2/redis"
	bolt "go.etcd.io/bbolt"
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
			// new path: load packed keys as compressed steam of
			// json-encoded strings
			var rd io.Reader = snappy.NewReader(bytes.NewReader(bkt.Get([]byte("keys"))))
			var k string
			for dec := json.NewDecoder(rd); ; {
				err := dec.Decode(&k)
				if err == io.EOF {
					return nil
				}
				if err != nil {
					for k := range s.keys {
						delete(s.keys, k)
					}
					break
				}
				s.keys[k] = struct{}{}
			}

			// legacy path: load packed keys as single gob-encoded
			// []string
			var keys []string
			rd = bytes.NewReader(bkt.Get([]byte("keys")))
			if err := gob.NewDecoder(rd).Decode(&keys); err == nil && len(keys) > 0 {
				for _, k := range keys {
					s.keys[k] = struct{}{}
				}
				return nil
			}
		}
		// fallback: read all keys directly from bucket
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
	srv.Handle("set", s.handleSet)
	srv.Handle("bgsave", s.handleBgsave)
	srv.Handle("slurp", s.handleSlurp)
	srv.Handle("scan", s.handleScan)
	srv.Handle("info", s.handleInfo)
	srv.Handle("select", handleSelect)
	srv.Handle("ping", handlePing)
	srv.Handle("ttl", s.handleTTL)
	srv.Handle("pttl", s.handlePTTL)
	srv.Handle("expire", s.handleExpire)
	srv.Handle("rename", s.handleRename)
	s.stats = srv.Stats
}

// Shutdown performs saves current state on disk and closes database. Shutdown
// blocks until state is saved and database is closed. Server should not be used
// afterwards.
func (s *Server) Shutdown() error {
	s.once.Do(func() { close(s.done); <-s.doneAck })
	return s.db.Close()
}

type cacheItem struct {
	b      *roaring.Bitmap
	aTime  int64 // unix timestamp of last access
	expire int64 // nanoseconds unix timestamp of expiration time, if set
	dirty  bool  // true if has unsaved modifications
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

	stats func() []red.CmdCount

	mu    sync.Mutex
	keys  map[string]struct{}  // all known keys
	rm    map[string]struct{}  // removed but not yet purged
	cache map[string]cacheItem // hot items
}

var bucketName = []byte("bitmapist")
var expiresBucket = []byte("expires")

func (s *Server) exists(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.keys[key]
	return ok
}

func (s *Server) putBitmap(withLock bool, key string, bm *roaring.Bitmap, keepExpire bool) {
	if withLock {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	v := cacheItem{
		b:     bm,
		aTime: time.Now().Unix(),
		dirty: true,
	}
	if prev, ok := s.cache[key]; keepExpire && ok && prev.expire > time.Now().UnixNano() {
		v.expire = prev.expire
	}
	s.keys[key] = struct{}{}
	s.cache[key] = v
}

func (s *Server) getBitmap(key string, create, setDirty bool) (*roaring.Bitmap, error) {
	nanots := time.Now().UnixNano()
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
		if v.expire > 0 && nanots > v.expire { // already expired
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
		{ // expiration check
			bkt := tx.Bucket(expiresBucket)
			if bkt == nil {
				goto readValue
			}
			data := bkt.Get([]byte(key))
			if data == nil {
				goto readValue
			}
			switch exp, n := binary.Varint(data); {
			case n == 0:
				return errors.New("expire decode: buf too small")
			case n < 0:
				return errors.New("expire decode: 64 bits overflow")
			case exp < nanots: // expired
				v.b = roaring.NewBitmap()
				return nil
			default:
				v.expire = exp
			}
		}
	readValue:
		bkt := tx.Bucket(bucketName)
		if bkt == nil {
			return errors.New("bucket not found")
		}
		data := bkt.Get([]byte(key))
		if data == nil {
			return nil
		}
		bm := roaring.NewBitmap()
		if err := bm.UnmarshalBinary(data); err != nil {
			return err
		}
		v.b = bm
		return nil
	})
	if err != nil {
		return nil, err
	}
	if v.b == nil {
		if !create {
			return nil, nil
		}
		v.b = roaring.NewBitmap()
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
		s.log.Println("saved in", time.Since(begin).Round(time.Millisecond))
	}
	for {
		select {
		case <-ticker.C:
			s.sweepExpired()
			fn("periodic saving...", s.persist)
		case <-s.save:
			s.sweepExpired()
			fn("forced saving...", s.persist)
		case <-s.done:
			fn("final saving...", s.persist)
			return
		}
	}
}

func (s *Server) sweepExpired() error {
	now := time.Now().UnixNano()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond) // TODO
	defer cancel()
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(expiresBucket)
		if bkt == nil {
			return nil
		}
		return bkt.ForEach(func(k, v []byte) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if exp, n := binary.Varint(v); n <= 0 || exp > now {
				return nil
			}
			// expired
			if _, ok := s.cache[string(k)]; ok {
				// skip keys that have something cached for
				// them: cache may hold new version which
				// differs from persisted one — persist() method
				// will take care of those items
				return nil
			}
			s.delete(false, string(k))
			return nil
		})
	})
}

func (s *Server) persist() error {
	now := time.Now()
	nowUnix := now.Unix()
	nowUnixNano := now.UnixNano()
	s.mu.Lock()
	dirty := make([]string, 0, len(s.cache))
	for k, v := range s.cache {
		if v.expire > 0 && v.expire <= nowUnixNano {
			delete(s.keys, k)
			delete(s.cache, k)
			s.rm[k] = struct{}{}
			continue
		}
		if !v.dirty {
			if v.aTime < nowUnix-300 {
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
	for k := range s.rm {
		delete(s.rm, k)
	}
	s.mu.Unlock()
	if len(toPurge) > 0 {
		err := s.db.Update(func(tx *bolt.Tx) error {
			for _, name := range [][]byte{bucketName, expiresBucket} {
				bkt, err := tx.CreateBucketIfNotExists(name)
				if err != nil {
					return err
				}
				for _, k := range toPurge {
					if err := bkt.Delete([]byte(k)); err != nil {
						return err
					}
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	type keyVal struct {
		k string
		v []byte
		e int64 // expiration timestamp
	}
	save := func(batch []keyVal) error {
		if len(batch) == 0 {
			return nil
		}
		return s.db.Update(func(tx *bolt.Tx) error {
			bkt, err := tx.CreateBucketIfNotExists(bucketName)
			if err != nil {
				return err
			}
			for _, kv := range batch {
				if err := bkt.Put([]byte(kv.k), kv.v); err != nil {
					return err
				}
			}
			bkt, err = tx.CreateBucketIfNotExists(expiresBucket)
			if err != nil {
				return err
			}
			for _, kv := range batch {
				if kv.e == 0 {
					continue
				}
				buf := make([]byte, binary.MaxVarintLen64)
				n := binary.PutVarint(buf, kv.e)
				if err := bkt.Put([]byte(kv.k), buf[:n]); err != nil {
					return err
				}
			}
			return nil
		})
	}
	markDirty := func(batch []keyVal) {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, kv := range batch {
			if v, ok := s.cache[kv.k]; ok {
				v.dirty = true
				s.cache[kv.k] = v
			}
		}
	}
	const maxBatchSize = 400
	batch := make([]keyVal, 0, maxBatchSize)
	for _, k := range dirty {
		if len(batch) == maxBatchSize {
			if err := save(batch); err != nil {
				markDirty(batch)
				return err
			}
			batch = batch[:0]
		}
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
		batch = append(batch, keyVal{k: k, v: data, e: v.expire})
	}
	if err := save(batch); err != nil {
		markDirty(batch)
		return err
	}
	buf, err := func() ([]byte, error) {
		buf := new(bytes.Buffer)
		wr := snappy.NewBufferedWriter(buf)
		enc := json.NewEncoder(wr)
		s.mu.Lock()
		defer s.mu.Unlock()
		for k := range s.keys {
			if err := enc.Encode(k); err != nil {
				return nil, err
			}
		}
		if err := wr.Close(); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}()
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		bkt, err := tx.CreateBucketIfNotExists([]byte("aux"))
		if err != nil {
			return err
		}
		return bkt.Put([]byte("keys"), buf)
	})
}

func (s *Server) handleSlurp(req red.Request) (interface{}, error) {
	if l := len(req.Args); l != 1 && l != 2 {
		return nil, red.ErrWrongArgs
	}
	addr := req.Args[0]
	db := 0
	if len(req.Args) == 2 {
		var err error
		if db, err = strconv.Atoi(req.Args[1]); err != nil {
			return nil, err
		}
	}
	go func() {
		s.log.Printf("importing redis dataset from %v db %d", addr, db)
		begin := time.Now()
		stats, err := s.redisImport(addr, db)
		if err != nil {
			s.log.Println("redis import error:", err)
			return
		}
		s.log.Printf("imported %d keys from redis in %v; "+
			"skipped: %d non-strings, %d zero-only bitmaps",
			stats.Imported, time.Since(begin), stats.NonStr, stats.Zero)
	}()
	return resp.OK, nil
}

func (s *Server) handleExpire(req red.Request) (interface{}, error) {
	if len(req.Args) != 2 {
		return nil, red.ErrWrongArgs
	}
	seconds, err := strconv.ParseInt(req.Args[1], 10, 64)
	if err != nil {
		return nil, err
	}
	res, err := s.expireKey(req.Args[0], time.Duration(seconds)*time.Second)
	return res, err
}

func (s *Server) handleTTL(req red.Request) (interface{}, error) {
	if len(req.Args) != 1 {
		return nil, red.ErrWrongArgs
	}
	v, err := s.keyTTLnanos(req.Args[0])
	if err != nil {
		return nil, err
	}
	if v < 0 {
		return v, nil
	}
	d := time.Duration(v).Round(time.Millisecond)
	return int64(d / time.Second), nil
}

func (s *Server) handlePTTL(req red.Request) (interface{}, error) {
	if len(req.Args) != 1 {
		return nil, red.ErrWrongArgs
	}
	v, err := s.keyTTLnanos(req.Args[0])
	if err != nil {
		return nil, err
	}
	if v < 0 {
		return v, nil
	}
	d := time.Duration(v).Round(time.Millisecond)
	return int64(d / time.Millisecond), nil
}

func (s *Server) keyTTLnanos(key string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.keys[key]; !ok {
		return -2, nil
	}
	if _, ok := s.cache[key]; !ok {
		if _, err := s.getBitmap(key, false, false); err != nil {
			return 0, err
		}
	}
	nanots := time.Now().UnixNano()
	if v, ok := s.cache[key]; ok {
		switch {
		case v.expire == 0:
			return -1, nil
		case v.expire < nanots: // expired
			return -2, nil
		}
		return (v.expire - nanots), nil
	}
	return -2, nil
}

func (s *Server) expireKey(key string, diff time.Duration) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.keys[key]; !ok {
		return 0, nil
	}
	if diff <= 0 {
		s.delete(false, key)
		return 1, nil
	}
	now := time.Now()
	nanots := now.UnixNano()
	if _, ok := s.cache[key]; !ok {
		if _, err := s.getBitmap(key, false, false); err != nil {
			return 0, err
		}
	}
	if v, ok := s.cache[key]; ok {
		if v.expire > 0 && v.expire < nanots {
			return 0, nil
		}
		v.expire = now.Add(diff).UnixNano()
		s.cache[key] = v
		return 1, nil
	}
	return 0, nil
}

func (s *Server) handleGet(req red.Request) (interface{}, error) {
	if len(req.Args) != 1 {
		return nil, red.ErrWrongArgs
	}
	return s.bitmapBytes(req.Args[0])
}

func (s *Server) handleSet(req red.Request) (interface{}, error) {
	if len(req.Args) != 2 {
		return nil, red.ErrWrongArgs
	}
	s.setFromBytes(req.Args[0], []byte(req.Args[1]))
	return resp.OK, nil
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

func (s *Server) handleRename(r red.Request) (interface{}, error) {
	if len(r.Args) != 2 {
		return nil, red.ErrWrongArgs
	}
	src, dst := r.Args[0], r.Args[1]
	errNoKey := errors.New("no such key")
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.keys[src]; !ok {
		return nil, errNoKey
	}
	if _, ok := s.cache[src]; !ok {
		if _, err := s.getBitmap(src, false, false); err != nil {
			return nil, err
		}
	}
	v, ok := s.cache[src]
	if !ok || (v.expire > 0 && v.expire < time.Now().UnixNano()) {
		return nil, errNoKey
	}
	if src == dst {
		return resp.OK, nil
	}
	v.dirty = true
	s.delete(false, src)
	s.keys[dst] = struct{}{}
	s.cache[dst] = v
	return resp.OK, nil
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
	case "commands":
		if s.stats != nil {
			buf := new(bytes.Buffer)
			for _, st := range s.stats() {
				fmt.Fprintf(buf, "%s:%d\n", st.Name, st.Cnt)
			}
			return buf.Bytes(), nil
		}
	case "keys":
		buf := new(bytes.Buffer)
		s.mu.Lock()
		defer s.mu.Unlock()
		fmt.Fprintf(buf, "keys_total:%d\n", len(s.keys))
		fmt.Fprintf(buf, "keys_cached:%d\n", len(s.cache))
		return buf.Bytes(), nil
	}
	return nil, red.ErrWrongArgs
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
		s.putBitmap(false, key, bm, true)
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
		s.putBitmap(false, key, bm, true)
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
	keys := []string{}
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
			s.putBitmap(false, key, roaring.NewBitmap(), false)
			return 0, nil
		}
	}
	if len(src) == 0 {
		s.delete(false, key)
		return 0, nil
	}
	xbm := roaring.FastAnd(src...)
	max := maxValue(xbm)
	s.putBitmap(false, key, xbm, false)
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
	max := maxValue(xbm)
	s.putBitmap(false, key, xbm, false)
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
	max := maxValue(xbm)
	s.putBitmap(false, key, xbm, false)
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
	max := maxValue(b1)
	upper := uint64(max + 1) // +1 because [rangeStart,rangeEnd)
	// when redis does BITOP NOT, it operates on byte boundary, so resulting
	// bitmap may have last byte padded with ones - mimick this by moving
	// upper bound to fit byte boundaries
	if x := upper % 8; x != 0 {
		upper += (8 - x)
	}
	b2 := roaring.Flip(b1, 0, upper)
	s.putBitmap(false, dst, b2, false)
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

func (s *Server) setFromBytes(key string, data []byte) {
	var vals []uint32
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
	s.putBitmap(true, key, roaring.BitmapOf(vals...), false)
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
	max := maxValue(bm)
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

type importStats struct {
	Imported, NonStr, Zero int
}

func (s *Server) redisImport(addr string, db int) (*importStats, error) {
	client, err := redis.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	if db != 0 {
		resp := client.Cmd("SELECT", db)
		if resp.Err != nil {
			return nil, resp.Err
		}
	}
	var stats importStats
	var vals []uint32
	var done bool
	cursor := "0"
	for !done {
		a, err := client.Cmd("SCAN", cursor, "count", "10000").Array()
		if err != nil {
			return nil, err
		}
		if len(a) != 2 {
			return nil, fmt.Errorf("bad response array length: %d", len(a))
		}
		cursor, err = a[0].Str()
		if err != nil {
			return nil, err
		}
		if cursor == "0" {
			done = true
		}
		keys, err := a[1].List()
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			if key == "" {
				continue
			}
			resp := client.Cmd("GET", key)
			if resp.Err != nil {
				if resp.IsType(redis.AppErr) {
					s.log.Printf("skip load of key %q: %v", key, resp.Err)
					stats.NonStr++
					continue
				}
				return nil, resp.Err
			}
			data, err := resp.Bytes()
			if err != nil {
				return nil, err
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
				stats.Zero++
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
			stats.Imported++
		}
	}
	return &stats, nil
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

func handlePing(r red.Request) (interface{}, error) {
	if len(r.Args) > 1 {
		return nil, red.ErrWrongArgs
	}
	if len(r.Args) == 0 {
		return "PONG", nil
	}
	return r.Args[0], nil
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

func maxValue(b *roaring.Bitmap) uint32 {
	if b.IsEmpty() {
		return 0
	}
	return b.Maximum()
}
