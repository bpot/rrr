package rrr

import (
	"encoding/binary"
	"io"
	"math/bits"
	"reflect"
	"sort"
	"unsafe"

	"github.com/bpot/bv"
)

// t is the number of bits per block,
// also the number of size classes.
const t = 63

// blockTypeWidth is the number of bits required to represent a block type (is type class?!)
const blockTypeWidth = 6

// blocksPerSuperblock is the number of blocks per superblock, used for the rank/
// select overlay structure.
const blocksPerSuperblock = 32

// RRR is a static compressed bit vector represented as
//
// Succinct indexable dictionaries with applications to encoding k-ary trees and multisets
type RRR struct {
	// size is the number of bits in the vector.
	size int
	// blockTypes is a bitvector containing the block type (size class) for each block.
	blockTypes *bv.BV
	// blockRanks contains the index of the block in the size class.
	blockRanks *bv.BV
	// superBlockBTRPtrs points to the start of each super block in blockType?
	superBlockBTRPtrs []uint64
	// superBlockRank points to the start of each super block in blockRanks?
	superBlockRank []uint64
}

// NewFromBitVector builds a compressed bitvector from the bits in bv
func NewFromBitVector(uncompressed *bv.BV) (*RRR, error) {
	blockCount := (uncompressed.Size() + (t - 1)) / t
	blockTypes := bv.New(int(blockTypeWidth * blockCount))
	superBlockBTRPtrs := make([]uint64, (blockCount+blocksPerSuperblock-1)/blocksPerSuperblock)
	superBlockRank := make([]uint64, (blockCount+blocksPerSuperblock-1)/blocksPerSuperblock) // XXX Dummy final block, etc?

	var blockIndex uint64
	var blockRankOff uint64

	// Set block types and determine size of block ranks bitvector
	for (blockIndex+1)*t <= uint64(uncompressed.Size()) {
		block := uncompressed.GetInt(uint(blockIndex*t), t)

		k := popcount(block)
		blockRankOff += classSizes[k]

		blockTypes.SetInt(int(blockTypeWidth*blockIndex), blockTypeWidth, k)

		blockIndex++
	}

	// Handle final partial block.
	trailingBits := uint64(uncompressed.Size()) - (blockIndex * t)
	if trailingBits > 0 {
		block := uncompressed.GetInt(uint(blockIndex*t), uint8(trailingBits))
		k := popcount(block)
		blockRankOff += classSizes[k]
		blockTypes.SetInt(int(blockTypeWidth*blockIndex), blockTypeWidth, k)
	}

	ranks := bv.New(int(blockRankOff))
	blockIndex = 0
	blockRankOff = 0
	rankSum := 0

	// Populate ranks which is the position of a block in its class.
	for (blockIndex+1)*t <= uint64(uncompressed.Size()) {
		if blockIndex%blocksPerSuperblock == 0 {
			// Starting a new super block. Set offset and prefix sum up to this
			// point.
			superBlockBTRPtrs[blockIndex/blocksPerSuperblock] = blockRankOff
			superBlockRank[blockIndex/blocksPerSuperblock] = uint64(rankSum)
		}

		block := uncompressed.GetInt(uint(blockIndex*t), t)
		k := popcount(block)
		rankSum += int(k)
		sz := classSizes[k]
		r := indexForBlock(k, block)
		ranks.SetInt(int(blockRankOff), uint8(sz), r)
		blockRankOff += sz

		blockIndex++
	}

	if trailingBits > 0 {
		block := uncompressed.GetInt(uint(blockIndex*t), uint8(trailingBits))
		k := popcount(block)
		sz := classSizes[k]
		r := indexForBlock(k, block)
		ranks.SetInt(int(blockRankOff), uint8(sz), r)
	}

	return &RRR{
		size:              uncompressed.Size(),
		blockTypes:        blockTypes,
		blockRanks:        ranks,
		superBlockBTRPtrs: superBlockBTRPtrs,
		superBlockRank:    superBlockRank,
	}, nil
}

// NewFromSerialized returns a RRR value which was serialized via WriteTo
func NewFromSerialized(buf []byte) (r *RRR, lengthBytes int, err error) {
	rrr := &RRR{}
	rrr.size = int(binary.LittleEndian.Uint64(buf))
	buf = buf[8:]
	lengthBytes += 8

	blockCount := (rrr.size + (t - 1)) / t
	superBlockCount := (blockCount + blocksPerSuperblock - 1) / blocksPerSuperblock

	// index pointers
	rrr.superBlockBTRPtrs = byteSliceAsUint64Slice(buf[:8*superBlockCount])
	buf = buf[8*superBlockCount:]
	lengthBytes += 8 * superBlockCount

	// rank prefix sums
	rrr.superBlockRank = byteSliceAsUint64Slice(buf[:8*superBlockCount])
	buf = buf[8*superBlockCount:]
	lengthBytes += 8 * superBlockCount

	// block classes
	var bvLength uint64
	rrr.blockTypes, bvLength, err = bv.NewByteBacked(buf)
	if err != nil {
		return nil, 0, err
	}
	buf = buf[bvLength:]
	lengthBytes += int(bvLength)

	// block indexes
	rrr.blockRanks, bvLength, err = bv.NewByteBacked(buf)
	if err != nil {
		return nil, 0, err
	}
	lengthBytes += int(bvLength)

	return rrr, lengthBytes, nil
}

// WriteTo serializes RRR to w
func (r *RRR) WriteTo(w io.Writer) (err error) {
	// Write size as uint64
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(r.size))
	_, err = w.Write(buf)
	if err != nil {
		return err
	}

	// Write super block index ptrs
	_, err = w.Write(uint64SliceAsByteSlice(r.superBlockBTRPtrs))
	if err != nil {
		return err
	}
	// Write super block rank prefix sums
	_, err = w.Write(uint64SliceAsByteSlice(r.superBlockRank))
	if err != nil {
		return err
	}

	// Write serialized classes
	_, err = r.blockTypes.WriteTo(w)
	if err != nil {
		return err
	}

	// Write serialized indexes
	_, err = r.blockRanks.WriteTo(w)
	if err != nil {
		return err
	}

	return nil
}

// Access returns true if i is set
func (r *RRR) Access(i uint64) bool {
	if i >= uint64(r.size) {
		panic("unable to access bit larger than size")
	}

	block := i / t
	superBlock := block / blocksPerSuperblock

	var blockType, rankOffset, b uint64
	rankOffset = r.superBlockBTRPtrs[superBlock]
	for b = superBlock * blocksPerSuperblock; b < block; b++ {
		blockType = r.blockTypes.GetInt(uint(b)*blockTypeWidth, blockTypeWidth)
		rankOffset += classSizes[blockType]
	}
	blockType = r.blockTypes.GetInt(uint(b)*blockTypeWidth, blockTypeWidth)
	blockRank := r.blockRanks.GetInt(uint(rankOffset), uint8(classSizes[blockType]))
	uncompressed := blockForIndex(blockType, blockRank)
	return uncompressed&(1<<(i%t)) > 0
}

// Rank1 returns the number of set bits up-to and including the ith bit.
func (r *RRR) Rank1(i uint64) uint64 {
	block := i / t
	superBlock := block / blocksPerSuperblock

	rank := r.superBlockRank[superBlock]
	var b, blockType uint64
	indexOffset := r.superBlockBTRPtrs[superBlock]
	for b = superBlock * blocksPerSuperblock; b < block; b++ {
		blockType = r.blockTypes.GetInt(uint(b)*blockTypeWidth, blockTypeWidth)
		indexOffset += classSizes[blockType]
		rank += blockType
	}

	// Handle next block UNLESS i is a multiple of the block size
	if i%t != 0 || i == 0 {
		blockType = r.blockTypes.GetInt(uint(block)*blockTypeWidth, blockTypeWidth)
		blockIndex := r.blockRanks.GetInt(uint(indexOffset), uint8(classSizes[blockType]))
		//fmt.Println("blockForIndex", blockType, blockIndex)
		uncompressed := blockForIndex(blockType, blockIndex)

		// Get rid of bytes we're not interested in: the bit @ i and all larger.
		var shift uint64
		if i == 0 {
			shift = 64
		} else {
			shift = t - ((i - 1) % t)
		}

		uncompressed = uncompressed << shift
		rank += popcount(uncompressed)
	}

	return rank
}

// Rank0 returns the number of set bits up-to and including the ith bit.
func (r *RRR) Rank0(i uint64) uint64 {
	return i - r.Rank1(i)
}

// Select1 returns the number of index of the ith set bit.
func (r *RRR) Select1(i uint64) uint64 {
	// Find superblock which contains what we are looking for
	superBlockIndex := sort.Search(len(r.superBlockRank), func(idx int) bool {
		return r.superBlockRank[idx] >= uint64(i)
	}) - 1

	var b, blockType uint64

	// Iterate through all of the blocks in this super block until we've found
	// the one containing the bit we're interested in.
	indexOffset := r.superBlockBTRPtrs[superBlockIndex]
	rank := r.superBlockRank[superBlockIndex]
	for b = uint64(superBlockIndex * blocksPerSuperblock); b < uint64((1+superBlockIndex)*blocksPerSuperblock); b++ {
		blockType = r.blockTypes.GetInt(uint(b)*blockTypeWidth, blockTypeWidth)
		// We've found the block!!!
		if rank+blockType >= i {
			break
		}
		rank += blockType
		indexOffset += classSizes[blockType]
	}

	// Decode the block containing our bit
	blockIndex := r.blockRanks.GetInt(uint(indexOffset), uint8(classSizes[blockType]))
	uncompressed := blockForIndex(blockType, blockIndex)

	bitsNeeded := i - rank
	for i := 0; i < t; i++ {
		if uncompressed&1 == 1 {
			bitsNeeded--
			if bitsNeeded == 0 {
				return uint64(i) + (b * t)
			}
		}
		uncompressed = uncompressed >> 1
	}

	return 0
}

// Select0 returns the number of index of the ith unset bit.
// TODO verify correctness
func (r *RRR) Select0(i uint64) uint64 {
	// Find superblock which contains what we are looking for
	superBlockIndex := sort.Search(len(r.superBlockRank), func(idx int) bool {
		zeroes := uint64(idx*t*blocksPerSuperblock) - uint64(r.superBlockRank[idx])
		return zeroes > uint64(i)
	}) - 1

	// Determine how many 0s were prior to this superblock
	totalBits := uint64(superBlockIndex * t * blocksPerSuperblock)
	rank0 := totalBits - r.superBlockRank[superBlockIndex]

	var b, blockType, indexOffset uint64
	// TODO set indexOffset based on super block.
	// Iterate through all of the blocks in this super block until we've found
	// the one containing the bit we're interested in.
	for b = uint64(superBlockIndex * blocksPerSuperblock); b < uint64((1+superBlockIndex)*blocksPerSuperblock); b++ {
		blockType = r.blockTypes.GetInt(uint(b)*blockTypeWidth, blockTypeWidth)
		zeroes := t - blockType
		// We've found the block!!!
		if rank0+zeroes >= i {
			break
		}
		rank0 += zeroes
		indexOffset += classSizes[blockType]
	}

	// Decode the block containing our bit
	blockIndex := r.blockRanks.GetInt(uint(indexOffset), uint8(classSizes[blockType]))
	uncompressed := blockForIndex(blockType, blockIndex)

	zeroesNeeded := i - rank0
	for i := 0; i < t; i++ {
		if uncompressed&1 == 0 {
			zeroesNeeded--
			if zeroesNeeded == 0 {
				return uint64(i) + (b * t)
			}
		}
		uncompressed = uncompressed >> 1
	}

	return 0
}

// Size returns the number of bits in r
func (r *RRR) Size() int {
	return r.size
}

// Uncompress returns an uncompressed bit vector
func (r *RRR) Uncompress() *bv.BV {
	uncompressed := bv.New(r.size)
	blockIndexOffset := uint(0)
	blockCount := r.blockTypes.Size() / blockTypeWidth
	for b := 0; b < blockCount-1; b++ {
		blockType := r.blockTypes.GetInt(uint(b)*blockTypeWidth, blockTypeWidth)
		blockIndex := r.blockRanks.GetInt(blockIndexOffset, uint8(classSizes[blockType]))
		block := blockForIndex(blockType, blockIndex)
		uncompressed.SetInt(b*t, t, block)

		blockIndexOffset += uint(classSizes[blockType])
	}

	// Handle final block
	blockType := r.blockTypes.GetInt(uint(blockCount-1)*blockTypeWidth, blockTypeWidth)
	blockIndex := r.blockRanks.GetInt(blockIndexOffset, uint8(classSizes[blockType]))
	block := blockForIndex(blockType, blockIndex)
	uncompressed.SetInt(int(blockCount-1)*t, uint8(r.size%t), block)

	return uncompressed
}

// SizeInBytes returns the size of r if it was serialized.
func (r *RRR) SizeInBytes() uint64 {
	return r.blockTypes.SizeInBytes() + r.blockRanks.SizeInBytes() +
		uint64(len(r.superBlockBTRPtrs)*8) + uint64(len(r.superBlockRank)*8)
}

func popcount(x uint64) uint64 {
	return uint64(bits.OnesCount64(x))
	/*
		x -= (x >> 1) & 0x5555555555555555
		x = (x>>2)&0x3333333333333333 + x&0x3333333333333333
		x += x >> 4
		x &= 0x0f0f0f0f0f0f0f0f
		x *= 0x0101010101010101
		return x >> 56
	*/
}

func uint64SliceAsByteSlice(slice []uint64) []byte {
	// make a new slice header
	header := *(*reflect.SliceHeader)(unsafe.Pointer(&slice))

	// update its capacity and length
	header.Len *= 8
	header.Cap *= 8

	// return it
	return *(*[]byte)(unsafe.Pointer(&header))
}

func byteSliceAsUint64Slice(b []byte) []uint64 {
	header := *(*reflect.SliceHeader)(unsafe.Pointer(&b))

	header.Len /= 8
	header.Cap /= 8

	return *(*[]uint64)(unsafe.Pointer(&header))
}
