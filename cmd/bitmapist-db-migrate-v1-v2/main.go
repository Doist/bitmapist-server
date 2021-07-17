// Command bitmapist-db-migrate-v1-v2 converts bitmapist-server v1 database to
// v2 format database.
package main

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	bolt "go.etcd.io/bbolt"
	"golang.org/x/sync/errgroup"

	_ "modernc.org/sqlite"
)

func main() {
	log.SetFlags(0)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	var input, output string
	flag.StringVar(&input, "in", input, "`path` to existing v1 database")
	flag.StringVar(&output, "out", output, "`path` to the new database in v2 format")
	flag.Parse()
	if err := run(ctx, input, output); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, v1Name, v2Name string) error {
	if v1Name == "" || v2Name == "" {
		return errors.New("both -v1db and -v2db must be set")
	}
	if v1Name == v2Name {
		return errors.New("-v1db and -v2db cannot point to the same file")
	}
	// if _, err := os.Stat(v2Name); !errors.Is(err, os.ErrNotExist) {
	// 	return fmt.Errorf("file %s must not exist", v2Name)
	// }
	db, err := sql.Open("sqlite", v2Name)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := initSchema(ctx, db); err != nil {
		return fmt.Errorf("database schema init: %w", err)
	}
	defer func(begin time.Time) { log.Printf("process took %v", time.Since(begin).Round(time.Second)) }(time.Now())
	chEntries := make(chan *dbEntry)
	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		defer close(chEntries)
		return emitStream(ctx, v1Name, chEntries)
	})
	group.Go(func() error {
		return persistStream(ctx, db, chEntries)
	})
	return group.Wait()
}

func persistStream(ctx context.Context, db *sql.DB, entries <-chan *dbEntry) error {
	var doNewline bool
	defer func() {
		if doNewline {
			fmt.Println()
		}
	}()
	var tx *sql.Tx
	var st *sql.Stmt
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	const maxTxSizeInBytes = 50 << 20
	var approxWriteSize int // approximate number of bytes to commit in a single tx
	var longestInsert, longestTx, latestTx time.Duration
	var maxBodySize int
	emitStat := func(n int) {
		doNewline = true
		fmt.Printf("%c[2K\rrows: %d, latest tx: %v, longest tx: %v, longest insert: %v, max body size: %d Mb",
			27, // VT100 clear line code \33[2K
			n, latestTx.Round(time.Millisecond),
			longestTx.Round(time.Millisecond),
			longestInsert.Round(time.Millisecond),
			maxBodySize>>20)
	}
	var err error
	for i := 0; ; i++ {
		var ent *dbEntry
		var ok bool
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ent, ok = <-entries:
		}
		if !ok {
			if tx != nil {
				if err := tx.Commit(); err != nil {
					return err
				}
			}
			tx = nil // defuse rollback
			emitStat(i)
			return nil
		}
		if ent.Name == "" {
			return fmt.Errorf("got entry with an empty name: %#v", ent)
		}
		approxWriteSize += len(ent.Data)
		if l := len(ent.Data); l > maxBodySize {
			maxBodySize = l
		}
		if tx == nil {
			if tx, err = db.BeginTx(ctx, nil); err != nil {
				return err
			}
			if st, err = tx.PrepareContext(ctx, `INSERT OR IGNORE INTO bitmaps VALUES(?,?,?)`); err != nil {
				return err
			}
		}
		begin := time.Now()
		if _, err = st.ExecContext(ctx, ent.Name, ent.ExpireAt, ent.Data); err != nil {
			return err
		}
		if d := time.Since(begin); longestInsert < d {
			longestInsert = d
		}
		if i%100 == 0 {
			emitStat(i)
		}
		if approxWriteSize > maxTxSizeInBytes {
			approxWriteSize = 0
			st.Close()
			begin := time.Now()
			if err = tx.Commit(); err != nil {
				return err
			}
			latestTx = time.Since(begin)
			if longestTx < latestTx {
				longestTx = latestTx
			}
			tx, st = nil, nil
			emitStat(i)
		}
	}
}

func emitStream(ctx context.Context, v1Name string, sink chan<- *dbEntry) error {
	bdb, err := bolt.Open(v1Name, 0644, &bolt.Options{
		ReadOnly: true,
		Timeout:  time.Second,
		OpenFile: func(name string, flag int, perm os.FileMode) (*os.File, error) {
			return os.OpenFile(name, os.O_RDONLY, perm)
		},
	})
	if err != nil {
		return err
	}
	defer bdb.Close()
	return bdb.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(bucketName))
		if bkt == nil {
			return fmt.Errorf("no %q bucket found", bucketName)
		}
		expBkt := tx.Bucket([]byte(expiresBucket))
		if expBkt == nil {
			return fmt.Errorf("no %q bucket found", expiresBucket)
		}
		fn := func(k, v []byte) error {
			// if keyCount < 10 {
			// 	log.Printf("key example: %s", k)
			// }
			var expireat int64
			if data := expBkt.Get(k); data != nil {
				switch exp, n := binary.Varint(data); {
				case n == 0:
					return errors.New("expire decode: buf too small")
				case n < 0:
					return errors.New("expire decode: 64 bits overflow")
				default:
					expireat = exp
				}
			}
			ent := &dbEntry{Name: string(k), ExpireAt: expireat, Data: make([]byte, len(v))}
			copy(ent.Data, v) // because v is only valid until v returns
			select {
			case sink <- ent:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return bkt.ForEach(fn)
	})
}

type dbEntry struct {
	Name     string
	ExpireAt int64
	Data     []byte
}

const bucketName = "bitmapist"
const expiresBucket = "expires"

func initSchema(ctx context.Context, db *sql.DB) error {
	for _, initStatement := range [...]string{
		`PRAGMA journal_mode=WAL`,
		`PRAGMA synchronous=OFF`,
		`CREATE TABLE IF NOT EXISTS bitmaps(
			name TEXT PRIMARY KEY,
			expireat INTEGER NOT NULL DEFAULT 0,
			bytes BLOB NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_bitmaps_expireat ON bitmaps(expireat)`,
	} {
		if _, err := db.ExecContext(ctx, string(initStatement)); err != nil {
			return err
		}
	}
	return nil
}
