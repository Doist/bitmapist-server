// Package autoflags provides a convenient way of exposing struct fields as
// command line flags. Exposed fields should have special tag attached:
//
//	var config = struct {
//		Name    string `flag:"name,user name"`
//		Age     uint   `flag:"age"`
//		Married bool   // this won't be exposed
//	}{
//		// default values
//		Name: "John Doe",
//		Age:  34,
//	}
//
// After declaring your flags and their default values as above, just register
// flags with autoflags.Define and call flag.Parse() as usual:
//
// 	autoflags.Define(&config)
// 	flag.Parse()
//
// Now config struct has its fields populated from command line flags. Call the
// program with flags to override default values:
//
// 	progname -name "Jane Roe" -age 29
//
// Package autoflags understands all basic types supported by flag's package
// xxxVar functions: int, int64, uint, uint64, float64, bool, string,
// time.Duration. Types implementing flag.Value interface are also supported.
// Attaching non-empty `flag` tag to field of unsupported type would result in
// panic.
package autoflags // import "github.com/artyom/autoflags"

import (
	"errors"
	"flag"
	"fmt"
	"reflect"
	"strings"
	"time"
)

var (
	// errPointerWanted is returned when passed argument is not a pointer
	errPointerWanted = errors.New("autoflags: pointer expected")
	// errInvalidArgument is returned when passed argument is nil pointer or
	// pointer to a non-struct value
	errInvalidArgument = errors.New("autoflags: non-nil pointer to struct expected")
	// errInvalidFlagSet is returned when FlagSet argument passed to
	// DefineFlagSet is nil
	errInvalidFlagSet = errors.New("autoflags: non-nil FlagSet expected")
	errInvalidField   = errors.New("autoflags: field is of unsupported type")
)

// Define takes pointer to struct and declares flags for its flag-tagged fields.
// Valid tags have one of the following formats:
//
// 	`flag:"flagname"`
// 	`flag:"flagname,usage string"`
//
// Define would panic if given unsupported/invalid argument  (anything but
// non-nil pointer to struct) or if any config attribute with `flag` tag is of
// type unsupported by the flag package (consider implementing flag.Value
// interface for such attributes).
func Define(config interface{}) { DefineFlagSet(flag.CommandLine, config) }

// Parse is a shortcut for:
//
// 	autoflags.Define(&args)
// 	flag.Parse()
func Parse(config interface{}) { Define(config); flag.Parse() }

// DefineFlagSet takes pointer to struct and declares flags for its flag-tagged
// fields on given FlagSet. Valid tags have one of the following formats:
//
// 	`flag:"flagname"`
// 	`flag:"flagname,usage string"`
//
// DefineFlagSet would panic if given unsupported/invalid config argument
// (anything but non-nil pointer to struct) or if any config attribute with
// `flag` tag is of type unsupported by the flag package (consider implementing
// flag.Value interface for such attrubutes).
func DefineFlagSet(fs *flag.FlagSet, config interface{}) {
	if fs == nil {
		panic(errInvalidFlagSet)
	}
	st := reflect.ValueOf(config)
	if st.Kind() != reflect.Ptr {
		panic(errPointerWanted)
	}
	st = reflect.Indirect(st)
	if !st.IsValid() || st.Type().Kind() != reflect.Struct {
		panic(errInvalidArgument)
	}
	flagValueType := reflect.TypeOf((*flag.Value)(nil)).Elem()
	for i := 0; i < st.NumField(); i++ {
		typ := st.Type().Field(i)
		var name, usage string
		tag := typ.Tag.Get("flag")
		if tag == "" {
			continue
		}
		val := st.Field(i)
		if !val.CanAddr() {
			panic(errInvalidField)
		}
		flagData := strings.SplitN(tag, ",", 2)
		switch len(flagData) {
		case 1:
			name = flagData[0]
		case 2:
			name, usage = flagData[0], flagData[1]
		}
		addr := val.Addr()
		if addr.Type().Implements(flagValueType) {
			fs.Var(addr.Interface().(flag.Value), name, usage)
			continue
		}
		switch d := val.Interface().(type) {
		case int:
			fs.IntVar(addr.Interface().(*int), name, d, usage)
		case int64:
			fs.Int64Var(addr.Interface().(*int64), name, d, usage)
		case uint:
			fs.UintVar(addr.Interface().(*uint), name, d, usage)
		case uint64:
			fs.Uint64Var(addr.Interface().(*uint64), name, d, usage)
		case float64:
			fs.Float64Var(addr.Interface().(*float64), name, d, usage)
		case bool:
			fs.BoolVar(addr.Interface().(*bool), name, d, usage)
		case string:
			fs.StringVar(addr.Interface().(*string), name, d, usage)
		case time.Duration:
			fs.DurationVar(addr.Interface().(*time.Duration), name, d, usage)
		default:
			panic(fmt.Sprintf("autoflags: field with flag tag value %q is of unsupported type", name))
		}
	}
}
