// Command bitmapist implements standalone bitmapist-compatible server
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/Doist/bitmapist-server/v2/internal/bitmapist"
	"github.com/artyom/red"
	"modernc.org/sqlite"
)

var explicitVersion string // to be set by CI with -ldflags="-X=main.explicitVersion=v1.2.3"

func main() {
	args := struct {
		Addr string
		File string
		Bak  string
		Dbg  bool
		Rel  bool
	}{
		Addr: "localhost:6379",
		File: "bitmapist.db",
	}
	flag.StringVar(&args.Addr, "addr", args.Addr, "`address` to listen")
	flag.StringVar(&args.File, "db", args.File, "`path` to database file")
	flag.StringVar(&args.Bak, "bak", args.Bak, "optional `path` to backup file; send SIGUSR1 to trigger online backup")
	flag.BoolVar(&args.Dbg, "debug", args.Dbg, "log all commands")
	flag.BoolVar(&args.Rel, "relaxed", args.Rel, "enable stale GETBIT reads and delayed SETBIT writes: this helps"+
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

	log := log.New(os.Stderr, "", 0)

	s, err := bitmapist.New(args.File, args.Rel)
	if err != nil {
		var sErr *sqlite.Error
		if errors.As(err, &sErr) && sErr.Code() == 26 {
			log.Fatal("Database file has unsupported format." +
				"\nIf you upgraded from v1.x version, make sure to convert database to the new format first." +
				"\nSee https://github.com/Doist/bitmapist-server#readme for details.")
		}
		log.Fatal(err)
	}
	s.WithLogger(log)

	srv := red.NewServer()
	srv.WithLogger(log)
	if args.Dbg {
		srv.WithCommands()
	}
	s.Register(srv)
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		log.Println(<-sigCh)
		signal.Reset()
		if err := s.Shutdown(); err != nil {
			log.Fatal(err)
		}
		os.Exit(0)
	}()
	if args.Bak != "" && args.Bak != args.File {
		go func() {
			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, syscall.SIGUSR1)
			for range sigCh {
				log.Printf("backing up database to %q", args.Bak)
				begin := time.Now()
				switch err := doBackup(s, args.Bak); err {
				case nil:
					log.Printf("backup successfully saved in %v", time.Since(begin).Round(500*time.Millisecond))
				default:
					log.Println("error doing backup:", err)
				}
			}
		}()
	}
	log.Fatal(srv.ListenAndServe(args.Addr))
}

// doBackup creates temporary file, calls s.Backup on it and renames temporary
// file to dst if backup completed successfully.
func doBackup(s *bitmapist.Server, dst string) error {
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
