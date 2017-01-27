package bitmapist

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/artyom/resp"
)

func TestAgainstRedis(t *testing.T) {
	if _, err := exec.LookPath("redis-server"); err != nil {
		t.Skip("redis-server not found in path, skipping test")
	}
	cf, cleanup := newServer(t)
	defer cleanup()

	cfRedis, cleanupRedis := newRedisServer(t)
	defer cleanupRedis()

	cmds := []string{
		"multi",
		"setbit foo 10 1",
		"setbit foo 7 1",
		"bitcount foo",
		"setbit bar 7 1",
		"setbit bar 6 1",
		"setbit bar 6 0",
		"bitcount bar",
		"exec",
		"get foo",
		"get bar",

		"bitop not dst foo",
		"get dst",
		"bitcount dst",
		"getbit dst 5",

		"bitop xor dst foo bar",
		"get dst",

		"bitop or dst foo bar",
		"get dst",

		"del foo bar",
		"setbit foo 10 1",
		"setbit foo 7 1",
		"setbit bar 10 1",

		"bitop and dst foo bar",
		"get dst",

		"quit",
	}

	resBitmapist, err := collectOutput(cmds, cf)
	if err != nil {
		t.Fatal(err)
	}
	resRedis, err := collectOutput(cmds, cfRedis)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(resBitmapist, resRedis) {
		if out, _ := diff(resBitmapist, resRedis); out != nil {
			t.Logf("diff -u:\n%s\n", out)
		}
		t.Fatal("bitmapist and redis output differ")
	}
}

func collectOutput(cmds []string, cf clientConnFunc) ([]byte, error) {
	conn := cf()
	defer conn.Close()
	rd := bufio.NewReader(conn)
	buf := new(bytes.Buffer)
	for _, cmd := range cmds {
		fmt.Fprintf(buf, "> %s\n", cmd)
		if err := resp.Encode(conn, strings.Fields(cmd)); err != nil {
			return nil, err
		}
		response, err := resp.Decode(rd)
		if err != nil {
			return nil, err
		}
		fmt.Fprintf(buf, "< %#v\n", response)
	}
	return buf.Bytes(), nil
}

func newRedisServer(t testing.TB) (fn clientConnFunc, cleanup func()) {
	td, err := ioutil.TempDir("", "bitmapist-test-")
	if err != nil {
		t.Fatal(err)
		return
	}
	buf := new(bytes.Buffer)
	fmt.Fprintln(buf, redisConf)
	unixSocket := filepath.Join(td, "redis.sock")
	fmt.Fprintf(buf, "\nunixsocket %s\n", unixSocket)
	redisConf := filepath.Join(td, "redis.conf")
	if err := ioutil.WriteFile(redisConf, buf.Bytes(), 0644); err != nil {
		os.RemoveAll(td)
		t.Fatal(err)
		return
	}
	ctx, cancelFunc := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "redis-server", redisConf)
	if err := cmd.Start(); err != nil {
		cancelFunc()
		t.Fatal(err)
		return
	}
	// give redis time to create socket
	for i := 0; i < 10; i++ {
		if _, err := os.Stat(unixSocket); !os.IsNotExist(err) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	fn = func() net.Conn {
		conn, err := net.DialTimeout("unix", unixSocket, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
			t.Fatal(err)
		}
		return conn
	}
	cleanup = func() {
		cancelFunc()
		os.RemoveAll(td)
		cmd.Wait()
	}
	return fn, cleanup
}

func diff(resBitmapist, resRedis []byte) ([]byte, error) {
	td, err := ioutil.TempDir("", "")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(td)
	if err := ioutil.WriteFile(filepath.Join(td, "bmpst.txt"), resBitmapist, 0644); err != nil {
		return nil, err
	}
	if err := ioutil.WriteFile(filepath.Join(td, "redis.txt"), resRedis, 0644); err != nil {
		return nil, err
	}
	cmd := exec.Command("diff", "-u", "bmpst.txt", "redis.txt")
	cmd.Dir = td
	return cmd.CombinedOutput()
}

const redisConf = `
port 0
save ""
`
