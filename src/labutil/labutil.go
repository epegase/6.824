package labutil

import (
	crand "crypto/rand"
	"fmt"
	"math/big"
	mrand "math/rand"
)

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// random range
func RandRange(from, to int) int {
	return mrand.Intn(to-from) + from
}

func Nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

// convert int to subscript, e.g. 12 -> ₁₂
func ToSubscript(i int) (r string) {
	for r, i = fmt.Sprintf("%c", 8320+(i%10)), i/10; i != 0; r, i = fmt.Sprintf("%c", 8320+(i%10))+r, i/10 {
	}
	return
}

func Suffix(s string, cnt int) string {
	return s[Max(0, len(s)-cnt):]
}
