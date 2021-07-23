// Command bitmapist implements standalone bitmapist-compatible server
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
	"modernc.org/sqlite"
)

var explicitVersion string // to be set by CI with -ldflags="-X=main.explicitVersion=v1.2.3"

func main() {
	args := runArgs{
		Addr: "localhost:6379",
		File: "bitmapist.db",
	}
	flag.StringVar(&args.Addr, "addr", args.Addr, "`address` to listen")
	flag.StringVar(&args.File, "db", args.File, "`path` to database file")
	flag.StringVar(&args.Bak, "bak", args.Bak, "optional `path` to backup file; send SIGUSR1 to trigger online backup")
	flag.BoolVar(&args.Dbg, "debug", args.Dbg, "log all commands")
	flag.BoolVar(&args.Rel, "unsafe", args.Rel, "enable stale GETBIT reads and delayed SETBIT writes: this helps"+
		"\nget better throughput on high GETBIT and SETBIT rates, when it's"+
		"\nacceptable to get results which may be up to few minutes stale")
	var versionOnly bool
	flag.BoolVar(&versionOnly, "v", versionOnly, "print version and exit")
	flag.BoolVar(&versionOnly, "version", versionOnly, "print version and exit")
	flag.Parse()
	if versionOnly || (len(os.Args) == 2 && os.Args[1] == "version") {
		v := "unknown"
		if explicitVersion != "" {
			v = explicitVersion
		} else if bi, ok := debug.ReadBuildInfo(); ok {
			v = bi.Main.Version
		}
		fmt.Printf("bitmapist-server %s\n", v)
		return
	}
	log.SetFlags(0)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, args); err != nil {
		log.Fatal(err)
	}
}

type runArgs struct {
	Addr string
	File string
	Bak  string
	Dbg  bool
	Rel  bool
}

func run(ctx context.Context, args runArgs) error {
	srv, err := New(args.File, args.Rel)
	if err != nil {
		var sErr *sqlite.Error
		if errors.As(err, &sErr) && sErr.Code() == 26 {
			//lint:ignore ST1005 this error must be descriptive enough for the end user, so using newlines here
			return errors.New("Database file has unsupported format." +
				"\nIf you upgraded from v1.x version, make sure to convert database to the new format first." +
				"\nSee https://github.com/Doist/bitmapist-server#readme for details.")
		}
		return err
	}
	defer srv.Shutdown()
	srv.WithLogger(log.Default())

	ln, err := net.Listen("tcp", args.Addr)
	if err != nil {
		return err
	}
	defer ln.Close()

	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		<-ctx.Done()
		return ln.Close()
	})
	group.Go(func() error {
		err := srv.Serve(ln, args.Dbg)
		if err != nil && strings.Contains(err.Error(), "use of closed network connection") {
			return nil
		}
		return err
	})
	if args.Bak != "" && args.Bak != args.File {
		group.Go(func() error {
			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, syscall.SIGUSR1)
			defer signal.Stop(sigCh)
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-sigCh:
				}
				log.Printf("backing up database to %q", args.Bak)
				begin := time.Now()
				switch err := doBackup(srv, args.Bak); err {
				case nil:
					log.Printf("backup successfully saved in %v", time.Since(begin).Round(500*time.Millisecond))
				default:
					log.Println("error doing backup:", err)
				}
			}
		})
	}

	if err := group.Wait(); err != nil {
		return err
	}
	return srv.Shutdown()
}

// doBackup creates temporary file, calls s.Backup on it and renames temporary
// file to dst if backup completed successfully.
func doBackup(s *Server, dst string) error {
	dir, err := os.MkdirTemp(filepath.Dir(dst), "bitmapist-backup-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	name := filepath.Join(dir, "dump.db")
	if err := s.Backup(name); err != nil {
		return err
	}
	return os.Rename(name, dst)
}
