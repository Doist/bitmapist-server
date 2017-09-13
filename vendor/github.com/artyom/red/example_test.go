package red

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
)

func Example() {
	srv := NewServer()
	srv.Handle("ping", func(req Request) (interface{}, error) {
		if len(req.Args) > 0 {
			return req.Args[0], nil
		}
		return "PONG", nil
	})
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer ln.Close()
	go func() { srv.Serve(ln) }()
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		fmt.Println(err)
		return
	}
	// See https://redis.io/topics/protocol
	// this matches sequence of the following commands run with redis-cli
	// tool:
	//
	// PING "Hello, world"
	// QUIT
	fmt.Fprintf(conn, "*2\r\n$4\r\nPING\r\n")
	fmt.Fprintf(conn, "$12\r\nHello, world\r\n")
	fmt.Fprintf(conn, "*1\r\n$4\r\nQUIT\r\n")
	buf := new(bytes.Buffer)
	io.Copy(buf, conn)
	os.Stdout.Write(bytes.Replace(buf.Bytes(), []byte{'\r'}, []byte{}, -1))
	// Output:
	// $12
	// Hello, world
	// +OK
}
