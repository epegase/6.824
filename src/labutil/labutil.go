package labutil

import (
	"math/rand"
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
	return rand.Intn(to-from) + from
}
