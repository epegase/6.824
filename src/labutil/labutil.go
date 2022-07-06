package labutil

import (
	crand "crypto/rand"
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
