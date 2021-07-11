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
	"io"
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
	db2, err := sql.Open("sqlite", dbFile)
	if err != nil {
		return nil, err
	}
	if err := initSchema(context.Background(), db2); err != nil {
		db2.Close()
		return nil, err
	}
	s := &Server{
		db:  db2,
		log: noopLogger{},
	}
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
	db  *sql.DB // TODO rename db2 -> db once the old "db" is dropped
	log Logger
	mu  sync.Mutex
}

func (s *Server) exists(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	var sink int
	_ = s.db.QueryRowContext(context.Background(), `SELECT 1 FROM bitmaps WHERE name=?`, key).Scan(&sink)
	return sink == 1
}

func (s *Server) putBitmap(withLock bool, key string, bm *roaring.Bitmap, keepExpire bool) {
	if withLock {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	buf, err := bm.MarshalBinary()
	if err != nil {
		panic(err) // TODO
	}
	const query1 = `INSERT OR REPLACE INTO bitmaps(name,bytes) VALUES(?,?)`
	const query2 = `INSERT INTO bitmaps(name,bytes) VALUES(?,?) ON CONFLICT(name) DO UPDATE SET bytes=excluded.bytes`
	query := query1
	if keepExpire {
		query = query2
	}
	// log.Printf("putBitmap: query: %q", query) // FIXME
	_, err = s.db.ExecContext(context.Background(), query, key, buf)
	if err != nil {
		panic(err) // TODO
	}
}

// getBitmap works on an already locked server!
func (s *Server) getBitmap(key string, create, setDirty bool) (*roaring.Bitmap, error) {
	nanots := time.Now().UnixNano()
	var buf []byte
	const query1 = `SELECT bytes FROM bitmaps WHERE name=? AND (expireat=0 OR expireat>?)`
	switch err := s.db.QueryRowContext(context.Background(), query1, key, nanots).Scan(&buf); err {
	case nil:
		// log.Println("getBitmap: got bitmap case") // FIXME
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
		// log.Println("getBitmap, case: no bitmap, creating/inserting one") // FIXME
		_, err = s.db.ExecContext(context.Background(), `INSERT INTO bitmaps(name,bytes) VALUES(?,?)`, key, buf)
		if err != nil {
			return nil, err
		}
		return bmap, nil
	default:
		return nil, err
	}
}

func (s *Server) sweepExpired() error {
	now := time.Now().UnixNano()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond) // TODO
	defer cancel()
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `DELETE FROM bitmaps WHERE expireat!=0 && expireat<?`, now)
	return err
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
	const query = `SELECT expireat FROM bitmaps WHERE name=? AND expireat=0 OR expireat>?`
	if err := s.db.QueryRowContext(context.Background(), query, key, nownanos).Scan(&expireat); err != nil {
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
		s.delete(false, key)
		return 1, nil
	}
	now := time.Now()
	nanots := now.UnixNano()
	const query = `UPDATE bitmaps SET expireat=@newexpire WHERE name=@name AND (expireat=0 OR (expireat!=0 AND expireat>@now))`
	res, err := s.db.ExecContext(context.Background(), query,
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
	s.setFromBytes(req.Args[0], []byte(req.Args[1]))
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
	const query = `UPDATE OR REPLACE bitmaps SET name=@newname WHERE name=@oldname`
	res, err := s.db.ExecContext(context.Background(), query, sql.Named("oldname", src), sql.Named("newname", dst))
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
	const query = `SELECT count(*) FROM bitmaps WHERE expireat=0 OR expireat>?`
	if err := s.db.QueryRowContext(context.Background(), query, time.Now().UnixNano()).Scan(&cnt); err != nil {
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
	s.putBitmap(false, key, bm, true)
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
	s.putBitmap(false, key, bm, true)
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
	const query = `SELECT name FROM bitmaps WHERE name GLOB ? AND (expireat=0 OR expireat > ?)`
	rows, err := s.db.QueryContext(context.Background(), query, pattern, time.Now().UnixNano())
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
	if len(keys) == 0 {
		return 0
	}
	if withLock {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	// TODO common case for a single key w/o explicit TX
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		panic(err) // TODO
	}
	defer tx.Rollback()
	st, err := tx.PrepareContext(context.Background(), `DELETE FROM bitmaps WHERE name=?`)
	if err != nil {
		panic(err) // TODO
	}
	var cnt int
	for _, k := range keys {
		res, err := st.ExecContext(context.Background(), k)
		if err != nil {
			panic(err)
		}
		if n, err := res.RowsAffected(); err == nil {
			cnt += int(n)
		}
	}
	if err := tx.Commit(); err != nil {
		panic(err) // TODO
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
	return errors.New("not implemented") // TODO
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
			s.putBitmap(true, key, roaring.BitmapOf(vals...), false)
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
