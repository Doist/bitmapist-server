// Package bitmapist implements standalone bitmapist server that can be used
// instead of Redis for https://github.com/Doist/bitmapist library.
package bitmapist

import (
	"bytes"
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/artyom/red"
	"github.com/artyom/resp"
	"github.com/mediocregopher/radix.v2/redis"
	_ "modernc.org/sqlite"
)

// New returns initialized Server that loads/saves its data in dbFile
func New(dbFile string) (*Server, error) {
	var defuseClose bool
	db, err := sql.Open("sqlite", dbFile)
	if err != nil {
		return nil, err
	}
	defer func() {
		if !defuseClose {
			db.Close()
		}
	}()
	if err := initSchema(context.Background(), db); err != nil {
		return nil, err
	}
	s := &Server{
		db:  db,
		log: noopLogger{},
	}
	if s.stExistsQuery, err = db.Prepare(`SELECT 1 FROM bitmaps WHERE name=? AND (expireat=0 OR expireat>?)`); err != nil {
		return nil, err
	}
	if s.stPutBitmap1, err = db.Prepare(`INSERT OR REPLACE INTO bitmaps(name,bytes) VALUES(?,?)`); err != nil {
		return nil, err
	}
	if s.stPutBitmap2, err = db.Prepare(`INSERT INTO bitmaps(name,bytes) VALUES(?,?) ON CONFLICT(name) DO UPDATE SET bytes=excluded.bytes`); err != nil {
		return nil, err
	}
	if s.stGetBitmapSelect, err = db.Prepare(`SELECT bytes FROM bitmaps WHERE name=? AND (expireat=0 OR expireat>?)`); err != nil {
		return nil, err
	}
	if s.stGetBitmapInsert, err = db.Prepare(`INSERT OR REPLACE INTO bitmaps(name,bytes) VALUES(?,?)`); err != nil {
		return nil, err
	}
	if s.stKeyTTLnanos, err = db.Prepare(`SELECT expireat FROM bitmaps WHERE name=? AND (expireat=0 OR expireat>?)`); err != nil {
		return nil, err
	}
	if s.stExpireKey, err = db.Prepare(`UPDATE bitmaps SET expireat=@newexpire WHERE name=@name AND (expireat=0 OR expireat>@now)`); err != nil {
		return nil, err
	}
	if s.stRename, err = db.Prepare(`UPDATE OR REPLACE bitmaps SET name=@newname WHERE name=@oldname`); err != nil {
		return nil, err
	}
	if s.stInfo, err = db.Prepare(`SELECT count(*) FROM bitmaps WHERE expireat=0 OR expireat>?`); err != nil {
		return nil, err
	}
	if s.stMatchingKeys, err = db.Prepare(`SELECT name FROM bitmaps WHERE name GLOB ? AND (expireat=0 OR expireat>?)`); err != nil {
		return nil, err
	}
	if s.stDelete, err = db.Prepare(`DELETE FROM bitmaps WHERE name=? AND (expireat=0 OR expireat>?)`); err != nil {
		return nil, err
	}
	if s.stDeleteExpired, err = db.Prepare(`DELETE FROM bitmaps WHERE expireat!=0 && expireat<?`); err != nil {
		return nil, err
	}
	defuseClose = true
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
}

// Shutdown performs saves current state on disk and closes database. Shutdown
// blocks until state is saved and database is closed. Server should not be used
// afterwards.
func (s *Server) Shutdown() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Close()
}

// Server is a standalone bitmapist server implementation. It's intended to be
// run on top of github.com/artyom/red.Server which handles redis protocol-level
// details and networking.
type Server struct {
	db  *sql.DB
	log Logger
	mu  sync.Mutex

	stExistsQuery     *sql.Stmt
	stPutBitmap1      *sql.Stmt
	stPutBitmap2      *sql.Stmt
	stGetBitmapSelect *sql.Stmt
	stGetBitmapInsert *sql.Stmt
	stKeyTTLnanos     *sql.Stmt
	stExpireKey       *sql.Stmt
	stRename          *sql.Stmt
	stInfo            *sql.Stmt
	stMatchingKeys    *sql.Stmt
	stDelete          *sql.Stmt
	stDeleteExpired   *sql.Stmt
}

func (s *Server) exists(key string) bool {
	nanos := time.Now().UnixNano()
	s.mu.Lock()
	defer s.mu.Unlock()
	var sink int
	_ = s.stExistsQuery.QueryRow(key, nanos).Scan(sink)
	return sink == 1
}

func (s *Server) putBitmap(withLock bool, key string, bm *roaring.Bitmap, keepExpire bool) error {
	if withLock {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	buf, err := bm.MarshalBinary()
	if err != nil {
		return err
	}
	st := s.stPutBitmap1
	if keepExpire {
		st = s.stPutBitmap2
	}
	_, err = st.Exec(key, buf)
	return err
}

// getBitmap works on an already locked server!
func (s *Server) getBitmap(key string, create, setDirty bool) (*roaring.Bitmap, error) {
	nanots := time.Now().UnixNano()
	var buf []byte
	switch err := s.stGetBitmapSelect.QueryRow(key, nanots).Scan(&buf); err {
	case nil:
		bmap := new(roaring.Bitmap)
		if err = bmap.UnmarshalBinary(buf); err != nil {
			return nil, err
		}
		return bmap, nil
	case sql.ErrNoRows:
		if !create {
			return nil, nil
		}
		bmap := new(roaring.Bitmap)
		if buf, err = bmap.MarshalBinary(); err != nil {
			return nil, err
		}
		if _, err = s.stGetBitmapInsert.Exec(key, buf); err != nil {
			return nil, err
		}
		return bmap, nil
	default:
		return nil, err
	}
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
	d := time.Duration(v).Round(time.Second)
	return int64(d.Seconds()), nil
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
	return d.Milliseconds(), nil
	// return int64(d / time.Millisecond), nil
}

func (s *Server) keyTTLnanos(key string) (int64, error) {
	nownanos := time.Now().UnixNano()
	s.mu.Lock()
	defer s.mu.Unlock()
	var expireat int64
	if err := s.stKeyTTLnanos.QueryRow(key, nownanos).Scan(&expireat); err != nil {
		if err == sql.ErrNoRows {
			return -2, nil
		}
		return 0, err
	}
	if expireat <= 0 {
		return -1, nil
	}
	ttl := expireat - nownanos
	if ttl < 0 {
		return -2, nil
	}
	return ttl, nil
}

func (s *Server) expireKey(key string, diff time.Duration) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if diff <= 0 {
		if _, err := s.delete(false, key); err != nil {
			return 0, err
		}
		return 1, nil
	}
	now := time.Now()
	nanots := now.UnixNano()
	res, err := s.stExpireKey.Exec(
		sql.Named("name", key),
		sql.Named("newexpire", now.Add(diff).UnixNano()),
		sql.Named("now", nanots),
	)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if n != 0 {
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
	if err := s.setFromBytes(req.Args[0], []byte(req.Args[1])); err != nil {
		return nil, err
	}
	return resp.OK, nil
}

func (s *Server) handleBgsave(r red.Request) (interface{}, error) {
	if len(r.Args) != 0 {
		return nil, red.ErrWrongArgs
	}
	return resp.OK, nil
}

func (s *Server) handleDel(r red.Request) (interface{}, error) {
	if len(r.Args) < 1 {
		return nil, red.ErrWrongArgs
	}
	n, err := s.delete(true, r.Args...)
	return int64(n), err
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
	res, err := s.stRename.Exec(sql.Named("oldname", src), sql.Named("newname", dst))
	if err != nil {
		return nil, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, errNoKey
	}
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
	case "keys":
	default:
		return nil, red.ErrWrongArgs
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var cnt int
	if err := s.stInfo.QueryRow(time.Now().UnixNano()).Scan(&cnt); err != nil {
		return nil, err
	}
	buf := new(bytes.Buffer)
	fmt.Fprintf(buf, "keys_total:%d\n", cnt)
	return buf.Bytes(), nil
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
	}
	x := bm.CheckedAdd(offset)
	if err := s.putBitmap(false, key, bm, true); err != nil {
		return false, err
	}
	return x, nil
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
	}
	x := bm.CheckedRemove(offset)
	if err := s.putBitmap(false, key, bm, true); err != nil {
		return false, err
	}
	return x, nil
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
	rows, err := s.stMatchingKeys.Query(pattern, time.Now().UnixNano())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		keys = append(keys, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
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
			if err := s.putBitmap(false, key, roaring.NewBitmap(), false); err != nil {
				return 0, err
			}
			return 0, nil
		}
	}
	if len(src) == 0 {
		if _, err := s.delete(false, key); err != nil {
			return 0, err
		}
		return 0, nil
	}
	xbm := roaring.FastAnd(src...)
	max := maxValue(xbm)
	if err := s.putBitmap(false, key, xbm, false); err != nil {
		return 0, err
	}
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
		if _, err := s.delete(false, key); err != nil {
			return 0, err
		}
		return 0, nil
	}
	xbm := roaring.FastOr(src...)
	max := maxValue(xbm)
	if err := s.putBitmap(false, key, xbm, false); err != nil {
		return 0, err
	}
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
		if _, err := s.delete(false, key); err != nil {
			return 0, err
		}
		return 0, nil
	}
	xbm := roaring.HeapXor(src...)
	max := maxValue(xbm)
	if err := s.putBitmap(false, key, xbm, false); err != nil {
		return 0, err
	}
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
		if _, err := s.delete(false, dst); err != nil {
			return 0, err
		}
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
	if err := s.putBitmap(false, dst, b2, false); err != nil {
		return 0, err
	}
	return int64(upper / 8), nil
}

func (s *Server) delete(withLock bool, keys ...string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	nanos := time.Now().UnixNano()
	if withLock {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	_, _ = s.stDeleteExpired.Exec(nanos)
	if len(keys) == 1 { // most common case
		res, err := s.stDelete.Exec(keys[0], nanos)
		if err != nil {
			return 0, err
		}
		n, err := res.RowsAffected()
		return int(n), err
	}
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	st := tx.Stmt(s.stDelete)
	defer st.Close()
	var cnt int
	for _, k := range keys {
		res, err := st.Exec(k, nanos)
		if err != nil {
			return 0, err
		}
		if n, err := res.RowsAffected(); err == nil {
			cnt += int(n)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return cnt, nil
}

func (s *Server) setFromBytes(key string, data []byte) error {
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
	return s.putBitmap(true, key, roaring.BitmapOf(vals...), false)
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

// Backup writes copy of the database to a new file. It's not safe to copy
// database file while it's used, so this method can be used to get a
// consistent copy of database.
func (s *Server) Backup(name string) error {
	if name == "" {
		return errors.New("name cannot be empty")
	}
	_, err := s.db.Exec(`VACUUM INTO ?`, name)
	return err
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
			if err := s.putBitmap(true, key, roaring.BitmapOf(vals...), false); err != nil {
				return nil, err
			}
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

func initSchema(ctx context.Context, db *sql.DB) error {
	for _, initStatement := range bytes.Split(fullSchemaSQL, []byte(";")) {
		initStatement = bytes.TrimSpace(initStatement)
		if len(initStatement) == 0 {
			continue
		}
		if _, err := db.ExecContext(ctx, string(initStatement)); err != nil {
			return fmt.Errorf("database init: %w", err)
		}
	}
	return nil
}

//go:embed schema.sql
var fullSchemaSQL []byte
