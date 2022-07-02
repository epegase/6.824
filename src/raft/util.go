package raft

import (
	"math/rand"
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// random range
func randRange(from, to int) int {
	return rand.Intn(to-from) + from
}
