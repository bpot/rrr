package rrr

import (
	"math/bits"
)

// classSize is the number of bits required to represent all members of the class?
var classSizes [64]uint64      // t + 1
var binomialCoefs [4096]uint64 // (t+1) * (t+1) (55bits for largest entry)

// Build coefficients lookup table and a table to lookup space usage for a specific class.
func init() {
	// Set first column
	for i := 0; i <= t; i++ {
		binomialCoefs[i*(t+1)] = 1
	}

	for r := 1; r <= t; r++ {
		for c := 1; c <= t; c++ {
			binomialCoefs[r*(t+1)+c] = binomialCoefs[(r-1)*(t+1)+(c-1)] + binomialCoefs[(r-1)*(t+1)+c]
			if binomialCoefs[(r-1)*(t+1)+c] == 0 {
				break
			}
		}
	}

	// Generate class sizes
	for r := 0; r <= t; r++ {
		classSizes[r] = uint64(log2(binomialCoefs[63*64+r]) + 1)
	}
}

func binCoef(n uint64, k uint64) uint64 {
	return binomialCoefs[n*64+k]
}

// log2 computes the integer binary logarithm of x.
// The result is the integer n for which 2^n <= x < 2^(n+1).
// If x == 0, the result is -1.
func log2(x uint64) int {
	return bits.Len(uint(x)) - 1
}
