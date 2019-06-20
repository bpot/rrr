package rrr

// indexForBlock returns the index of block in size class k
func indexForBlock(k uint64, block uint64) (r uint64) {
	if block == 0 {
		return 0
	}

	nn := t
	for block != 0 {
		if 1&block == 1 {
			r += binCoef(uint64(nn-1), k)
			k--
		}
		block = (block >> 1)
		nn--
	}
	return r
}

// blockForIndex returns the value/block for index r in class k
func blockForIndex(k uint64, r uint64) (block uint64) {
	if k == 0 {
		return 0
	}

	blockLength := uint64(t)
	var i uint
	for k > 1 {
		coef := binomialCoefs[(blockLength-1)*64+k]
		if r >= coef {
			r -= coef
			block |= (1 << (i & 63))
			k--
		}
		i++
		blockLength--
	}

	if t-r-1 >= 0 {
		block |= (1 << (t - r - 1))
	}
	return block
}
