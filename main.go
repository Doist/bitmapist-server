// Command bitmapist implements standalone bitmapist-compatible server
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/Doist/bitmapist-server/internal/bitmapist"
	"github.com/artyom/autoflags"
	"github.com/artyom/red"
)

var explicitVersion string // to be set by CI with -ldflags="-X=main.explicitVersion=v1.2.3"

func main() {
	args := struct {
		Addr string `flag:"addr,address to listen"`
		File string `flag:"db,path to database file"`
		Bak  string `flag:"bak,file to save backup to on SIGUSR1"`
		Dbg  bool   `flag:"debug,log incoming commands"`
	}{
		Addr: "localhost:6379",
		File: "bitmapist.db",
	}
	autoflags.Define(&args)
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

	log := log.New(os.Stderr, "", log.LstdFlags)
	if os.Getppid() == 1 && os.Getenv("INVOCATION_ID") != "" {
		log.SetFlags(0) // systemd takes care of log line timestamps
	}

	log.Println("loading data from", args.File)
	begin := time.Now()
	s, err := bitmapist.New(args.File)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("loaded in", time.Since(begin))
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
				switch err := doBackup(s, args.Bak); err {
				case nil:
					log.Println("backup successfully saved")
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
	f, err := os.CreateTemp(filepath.Dir(dst), "bitmapist-backup-")
	if err != nil {
		return err
	}
	defer f.Close()
	defer os.Remove(f.Name())
	if err := s.Backup(f); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Chmod(f.Name(), 0644); err != nil {
		return err
	}
	return os.Rename(f.Name(), dst)
}
