// Package red implements server speaking redis serialization protocol.
package red

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/artyom/resp"
)

// HandlerFunc is a type of command processing function. It should return value
// that's passed to resp.Encode function, so it's expected such function would
// return values of resp package types.
type HandlerFunc func(req Request) (interface{}, error)

// Request holds information about single redis command.
type Request struct {
	Name string   // lowercase command itself (first word)
	Args []string // command arguments
}

// NewServer returns initialized server.
func NewServer() *Server { return new(Server) }

// WithLogger configures server to use provided Logger.
func (s *Server) WithLogger(l Logger) {
	if l != nil {
		s.log = l
	}
}

// WithCommands enables logging of incoming commands. Logging should be enabled
// using WithLogger
func (s *Server) WithCommands() { s.verbose = true }

// WithUnsafeTx disables command processing serialization that guarantees
// redis-like transaction safety.
func (s *Server) WithUnsafeTx() { s.unsafe = true }

// Handle registers handler for command with given name (case-insensitive)
func (s *Server) Handle(name string, h HandlerFunc) {
	if name == "" {
		panic("Handle called with empty name")
	}
	if h == nil {
		panic("Handle called with nil HandlerFunc")
	}
	if s.handlers == nil {
		s.handlers = make(map[string]HandlerFunc)
	}
	s.handlers[strings.ToLower(name)] = h
}

// Server implements server speaking RESP (REdis Serialization Protocol). Server
// automatically handles MULTI, EXEC, DISCARD commands for transactions, QUIT for
// client-initiated disconnect, other commands are expected to be implemented
// separately and registered with Handle method.
type Server struct {
	log      Logger
	handlers map[string]HandlerFunc
	nextid   uint64
	verbose  bool       // whether to log incoming commands
	mu       sync.Mutex // used to serialize transactions
	unsafe   bool       // whether to skip transaction serialization
}

func (s *Server) txLock() {
	if s.unsafe {
		return
	}
	s.mu.Lock()
}
func (s *Server) txUnlock() {
	if s.unsafe {
		return
	}
	s.mu.Unlock()
}

// HandleConn processes single client connection, automatically handling
// following commands MULTI/EXEC/DISCARD (transactions), QUIT (client disconnect). It
// calls user-provided handlers for registered commands.
func (s *Server) HandleConn(conn io.ReadWriteCloser) error {
	defer conn.Close()
	connid := atomic.AddUint64(&s.nextid, 1)
	rd := bufio.NewReader(conn)
	var tx []Request
	var inTx bool  // if we're inside transaction
	var errTx bool // true if transaction seen error and should be discarded
	var err error
	for {
		if err != nil {
			return err
		}
		req, err := resp.DecodeRequest(rd)
		switch err {
		case nil:
		case resp.ErrInvalidRequest:
			err = resp.Encode(conn, resp.Error("ERR unknown command"))
			continue
		default:
			return err
		}
		if s.log != nil && s.verbose {
			s.log.Printf("conn:%d\t%v", connid, req)
		}
		cmd := strings.ToLower(req[0])
		switch cmd {
		case "quit":
			return resp.Encode(conn, resp.OK)
		case "discard":
			if len(req) != 1 {
				if inTx {
					errTx = true
				}
				err = resp.Encode(conn, errWrongArgs(cmd))
				continue
			}
			if !inTx {
				err = resp.Encode(conn, resp.Error("ERR DISCARD without MULTI"))
				continue
			}
			inTx, errTx, tx = false, false, tx[:0]
			err = resp.Encode(conn, resp.OK)
			continue
		case "multi":
			if len(req) != 1 {
				if inTx {
					errTx = true
				}
				err = resp.Encode(conn, errWrongArgs(cmd))
				continue
			}
			if inTx {
				errTx = true
				err = resp.Encode(conn, resp.Error("ERR MULTI calls can not be nested"))
				continue
			}
			inTx, errTx = true, false
			err = resp.Encode(conn, resp.OK)
			continue
		case "exec":
			if len(req) != 1 {
				if inTx {
					errTx = true
				}
				err = resp.Encode(conn, errWrongArgs(cmd))
				continue
			}
			if !inTx {
				err = resp.Encode(conn, resp.Error("ERR EXEC without MULTI"))
				continue
			}
			if errTx {
				inTx, errTx = false, false
				tx = tx[:0]
				err = resp.Encode(conn, resp.Error("EXECABORT Transaction discarded because of previous errors."))
				continue
			}
		default:
			h, ok := s.handlers[cmd]
			if !ok {
				if inTx {
					errTx = true
				}
				err = resp.Encode(conn, errNoCmd(cmd))
				continue
			}
			if inTx {
				if !errTx {
					tx = append(tx, Request{Name: cmd, Args: req[1:]})
				}
				err = resp.Encode(conn, resp.SimpleString("QUEUED"))
				continue
			}
			s.txLock()
			err = resp.Encode(conn, runHandler(h, Request{Name: cmd, Args: req[1:]}))
			s.txUnlock()
			continue
		}

		txReplies := make(resp.Array, 0, len(tx))
		s.txLock()
		for _, r := range tx {
			h, ok := s.handlers[r.Name]
			if !ok {
				txReplies = append(txReplies, errNoCmd(r.Name))
				continue
			}
			txReplies = append(txReplies, runHandler(h, r))
		}
		s.txUnlock()
		inTx, errTx = false, false
		tx = tx[:0]
		err = resp.Encode(conn, txReplies)
	}
}

// ListenAndServe listens on TCP network address addr and then calls Serve to
// handle requests on incoming connections.
func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(tcpKeepAliveListener{ln.(*net.TCPListener)})
}

// Serve accepts incoming connections on the Listener l, creating a new service
// goroutine for each.
func (s *Server) Serve(l net.Listener) error {
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}
		go func(c net.Conn) {
			switch err := s.HandleConn(c); err {
			case nil, io.EOF:
			default:
				if s.log != nil {
					s.log.Println(err)
				}
			}
		}(conn)
	}
}

// Logger is a set of methods used to log information. *log.Logger implements
// this interface.
type Logger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

func errNoCmd(name string) resp.Error { return resp.Error("ERR unknown command '" + name + "'") }

// ErrWrongArgs are expected to be returned by HandlerFunc implementations when
// number of arguments are wrong. This error is automatically passed to client
// with command name annotated.
var ErrWrongArgs = errors.New("wrong number of arguments")

func errWrongArgs(name string) resp.Error {
	return resp.Error("ERR wrong number of arguments for '" + name + "' command")
}

// runHandler calls HandlerFunc h with given Request r and returns single value
// that is expected to be passed to resp.Encode.
//
// If h returns a non-nil error or panics, then runHandler returns resp.Error
// type, otherwise it's an interface{} returned by h.
func runHandler(h HandlerFunc, r Request) interface{} {
	var panicked bool
	v, err := func() (interface{}, error) {
		defer func() {
			if x := recover(); x != nil {
				panicked = true
				fmt.Fprintln(os.Stderr, "panic recovered:", x)
				debug.PrintStack()
			}
		}()
		return h(r)
	}()
	if panicked {
		return resp.Error("ERR internal server error (panic)")
	}
	if err != nil {
		if err == ErrWrongArgs {
			return errWrongArgs(r.Name)
		}
		text := err.Error()
		if strings.ContainsAny(text, "\r\n") {
			text = strings.Replace(text, "\r", " ", -1)
			text = strings.Replace(text, "\n", " ", -1)
		}
		return resp.Error("ERR " + text)
	}
	return v
}

// tcpKeepAliveListener sets TCP keep-alive timeouts on accepted
// connections. It's used by ListenAndServe and ListenAndServeTLS so
// dead TCP connections (e.g. closing laptop mid-download) eventually
// go away.
type tcpKeepAliveListener struct {
	*net.TCPListener
}

func (ln tcpKeepAliveListener) Accept() (c net.Conn, err error) {
	tc, err := ln.AcceptTCP()
	if err != nil {
		return
	}
	tc.SetKeepAlive(true)
	tc.SetKeepAlivePeriod(3 * time.Minute)
	return tc, nil
}
