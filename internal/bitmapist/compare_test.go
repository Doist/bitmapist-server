package bitmapist

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
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

	type testCase struct {
		name     string
		commands []string
	}
	for _, test := range []testCase{
		{name: "basic", commands: []string{
			"ping",
			"ping hello",
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
		}},
		{name: "bitop xor", commands: []string{
			"setbit foo 10 1",
			"setbit foo 7 1",
			"setbit bar 7 1",
			"setbit bar 6 1",
			"setbit bar 6 0",
			"bitop xor dst foo bar",
			"get dst",
		}},
		{name: "bitop not", commands: []string{
			"setbit foo 10 1",
			"bitop not dst foo",
			"get dst",
			"bitcount dst",
			"getbit dst 5",
		}},
		{name: "bitop or", commands: []string{
			"setbit foo 10 1",
			"setbit bar 6 1",
			"bitop or dst foo bar",
			"get dst",
		}},
		{name: "bitop and", commands: []string{
			"setbit foo 10 1",
			"setbit foo 7 1",
			"setbit bar 10 1",
			"bitop and dst foo bar",
			"get dst",
			"set dst \xff\x7f",
			"get dst",
		}},
		{name: "ttl", commands: []string{
			"set dst \xff\x7f",
			"ttl nonexistent",
			"ttl dst",
			"expire dst 60",
			"ttl dst",
		}},
		{name: "rename", commands: []string{
			"setbit src_rename 1 1",
			"expire src_rename 60",
			"rename src_rename dst_rename",
			"ttl dst_rename",
			"getbit dst_rename 1",
		}},
		{name: "bitop and nonexistent", commands: []string{
			"bitop and xxx_dst xxx_nonexistent xxx_nonexistent",
			"bitcount xxx_dst",
			"exists xxx_dst",
			"del xxx_dst",
			"setbit xxx_somekey 256 1",
			"bitop and xxx_dst xxx_nonexistent xxx_somekey",
			"bitcount xxx_dst",
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			resBitmapist, err := flushAndCollectOutput(test.commands, cf)
			if err != nil {
				t.Fatal(err)
			}
			resRedis, err := flushAndCollectOutput(test.commands, cfRedis)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(resBitmapist, resRedis) {
				if out, _ := diff(t.TempDir(), resBitmapist, resRedis); out != nil {
					t.Logf("diff -u:\n%s\n", out)
				}
				t.Fatal("bitmapist and redis output differ")
			}
		})
	}
}

func flushAndCollectOutput(cmds []string, cf clientConnFunc) ([]byte, error) {
	conn := cf()
	defer conn.Close()
	rd := bufio.NewReader(conn)
	buf := new(bytes.Buffer)
	if err := resp.Encode(conn, []string{"flushall"}); err != nil {
		return nil, fmt.Errorf("flushall: %w", err)
	}
	if _, err := resp.Decode(rd); err != nil {
		return nil, fmt.Errorf("flushall response: %w", err)
	}
	for _, cmd := range cmds {
		fmt.Fprintf(buf, "(client)> %s\n", cmd)
		if err := resp.Encode(conn, strings.Fields(cmd)); err != nil {
			return nil, err
		}
		response, err := resp.Decode(rd)
		if err != nil {
			return nil, err
		}
		fmt.Fprintf(buf, "(server)> %#v\n", response)
	}
	return buf.Bytes(), nil
}

func newRedisServer(t testing.TB) (fn clientConnFunc, cleanup func()) {
	td := t.TempDir()
	buf := bytes.NewBufferString(redisConf)
	unixSocket := filepath.Join(td, "redis.sock")
	fmt.Fprintf(buf, "\nunixsocket %s\n", unixSocket)
	redisConf := filepath.Join(td, "redis.conf")
	if err := os.WriteFile(redisConf, buf.Bytes(), 0644); err != nil {
		os.RemoveAll(td)
		t.Fatal(err)
		return
	}
	ctx, cancelFunc := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "redis-server", redisConf)
	cmd.Dir = td
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

func diff(td string, resBitmapist, resRedis []byte) ([]byte, error) {
	if err := os.WriteFile(filepath.Join(td, "bitmapist-server.txt"), resBitmapist, 0644); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(td, "redis.txt"), resRedis, 0644); err != nil {
		return nil, err
	}
	cmd := exec.Command("diff", "-u", "bitmapist-server.txt", "redis.txt")
	cmd.Dir = td
	return cmd.CombinedOutput()
}

const redisConf = `
port 0
save ""
`
