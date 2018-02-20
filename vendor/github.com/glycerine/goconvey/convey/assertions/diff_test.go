package assertions

import (
	"fmt"
	"testing"
)

func TestDiffB(t *testing.T) {

	fmt.Printf("Given two different (possibly long, differing in whitespace) strings a and b\n")
	fmt.Printf("then DiffB(a, b) should return the output of diff -b to consisely summarize their differences\n")

	a := `
1
2
3
4
5
type   s1    struct {
  MyInts    []int
}`
	b := `
1
2
3
4
type s1 struct {
  MyInts []int
}`
	if string(Diffb(a, b)) != `6d5
< 5
` {
		t.Fatalf("mismatch if diff_test!")
	}

}
