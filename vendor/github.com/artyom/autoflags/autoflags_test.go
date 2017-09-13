package autoflags

import (
	"flag"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"
	"unsafe"
)

func ResetForTesting(usage func()) {
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	flag.Usage = usage
}

func TestDefineErrPointerWanted(t *testing.T) {
	ResetForTesting(nil)
	defer func() {
		if x := recover(); x != errPointerWanted {
			t.Fatalf("should have panicked with errPointerWanted, got %v", x)
		}
	}()
	Define(1)
}

func TestDefineErrInvalidArgument(t *testing.T) {
	ResetForTesting(nil)
	var testConfig *struct{}
	defer func() {
		if x := recover(); x != errInvalidArgument {
			t.Fatalf("should have panicked with errInvalidArgument, got %v", x)
		}
	}()
	Define(testConfig)
}

func TestDefineParseEmpty(t *testing.T) {
	ResetForTesting(nil)
	reference := config{
		String: "foo",
		Int:    42,
	}
	conf := reference
	Define(&conf)
	if err := flag.CommandLine.Parse([]string{}); err != nil {
		t.Fatal("parsing failed:", err)
	}
	if !reflect.DeepEqual(reference, conf) {
		t.Fatalf("result differs after parsing empty arguments; "+
			"want: %+v, got %+v", reference, conf)
	}
}

func TestDefineParse(t *testing.T) {
	ResetForTesting(nil)
	reference := configBig{
		String:   "whales",
		Int:      42,
		Int64:    100 << 30,
		Uint:     7,
		Uint64:   24,
		Float64:  1.55,
		Bool:     true,
		Duration: 15 * time.Minute,
		MySlice:  CustomFlag{"a", "b"},
	}
	conf := configBig{}
	Define(&conf)
	args := []string{
		"-string", "whales", "-int", "42",
		"-int64", "107374182400", "-uint", "7",
		"-uint64", "24", "-float64", "1.55", "-bool",
		"-duration", "15m",
		"-slice", "a",
		"-slice", "b",
	}
	if err := flag.CommandLine.Parse(args); err != nil {
		t.Fatal("parsing failed:", err)
	}
	if !reflect.DeepEqual(reference, conf) {
		t.Fatalf("result differs after parsing arguments; "+
			"want: %+v, got %+v", reference, conf)
	}
}

func TestDefineFlagSetErrInvalidFlagSet(t *testing.T) {
	defer func() {
		if x := recover(); x != errInvalidFlagSet {
			t.Fatalf("should have panicked with errInvalidFlagSet, got %v", x)
		}
	}()
	DefineFlagSet(nil, &struct{}{})
}

func TestUnsupportedFlagType(t *testing.T) {
	ResetForTesting(nil)
	defer func() {
		if x := recover(); x == nil {
			t.Fatalf("should have panicked with unsupported type, got %v", x)
		}
	}()
	config := struct {
		NonAddressable unsafe.Pointer `flag:"nil"` // non-addressable
	}{}
	Define(&config)
}

func ExampleDefineFlagSet() {
	fs := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	var config = struct {
		Name    string `flag:"name,user name"`
		Age     uint   `flag:"age"`
		Married bool   // this won't be exposed
	}{
		Name: "John Doe", // default values
		Age:  34,
	}
	DefineFlagSet(fs, &config)

	args := []string{"-name", "Jane Roe", "-age", "29"}

	fmt.Printf("before parsing flags:\n%+v\n", config)
	fs.Parse(args)
	fmt.Printf("\nafter parsing flags:\n%+v\n", config)
	// Output:
	//
	// before parsing flags:
	// {Name:John Doe Age:34 Married:false}
	//
	// after parsing flags:
	// {Name:Jane Roe Age:29 Married:false}
}

type config struct {
	String string `flag:"name"`
	Int    int    `flag:"num,integer number"`
}

type configBig struct {
	String   string        `flag:"string,string flag example"`
	Int      int           `flag:"int,int flag example"`
	Int64    int64         `flag:"int64,int64 flag example"`
	Uint     uint          `flag:"uint,uint flag example"`
	Uint64   uint64        `flag:"uint64"`
	Float64  float64       `flag:"float64"`
	Bool     bool          `flag:"bool"`
	Duration time.Duration `flag:"duration"`
	MySlice  CustomFlag    `flag:"slice"` // custom flag.Value implementation

	Empty      bool `flag:""` // empty flag definition
	NonExposed int  // does not have flag attached
}

// CustomFlag implements flag.Value interface and provides building string slice
// by specifying flag multiple times
type CustomFlag []string

func (c *CustomFlag) String() string { return fmt.Sprint(*c) }
func (c *CustomFlag) Set(value string) error {
	*c = append(*c, value)
	return nil
}
