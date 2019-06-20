package rrr

import (
	"math/big"
	"testing"
)

func TestCorrect(t *testing.T) {
	z := big.NewInt(0)
	var n, k uint64
	for n = 0; n < 64; n++ {
		for k = 0; k < 64; k++ {
			if binomialCoefs[n*64+k] != z.Binomial(int64(n), int64(k)).Uint64() {
				t.Fatalf("binomialTable is wrong for %d,%d. expected %d, got %d", n, k, z.Binomial(int64(n), int64(k)).Uint64(), binomialCoefs[n*64+k])
			}
		}
	}
}
