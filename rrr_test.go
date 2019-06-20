package rrr

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/bpot/bv"
)

func TestRRR(t *testing.T) {
	bv := bv.New(64)
	bv.Set(1, true)

	rrr, err := NewFromBitVector(bv)
	if err != nil {
		t.Fatal(err)
	}

	if rrr.Access(0) {
		t.Errorf("expected 0 to not be set")
	}

	if !rrr.Access(1) {
		t.Errorf("expected 1 to be set")
	}
}

func TestRRRCompression(t *testing.T) {
	bv := bv.New(64 * 1024)
	rrr, err := NewFromBitVector(bv)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("SIZE", rrr.SizeInBytes())
}

func TestRoundTrip(t *testing.T) {
	v := bv.New(1024)
	v.Set(5, true)
	v.Set(555, true)

	rrr, err := NewFromBitVector(v)
	if err != nil {
		t.Fatal(err)
	}

	if !v.Equals(rrr.Uncompress()) {
		t.Errorf("Bitmaps are not equal!")
	}

	bv := bv.New(7)
	bv.Set(0, true)
	bv.Set(2, true)
	bv.Set(4, true)
	bv.Set(6, true)
	rrr, err = NewFromBitVector(bv)
	if err != nil {
		t.Fatal(err)
	}

	if !bv.Equals(rrr.Uncompress()) {
		t.Errorf("Bitmaps are not equal!")
	}
}

func TestRank(t *testing.T) {
	bv := bv.New(1024)
	bv.Set(5, true)
	bv.Set(555, true)

	rrr, err := NewFromBitVector(bv)
	if err != nil {
		t.Fatal(err)
	}

	if 0 != rrr.Rank1(0) {
		t.Errorf("expected rank 0; got %d", rrr.Rank1(0))
	}
	if 0 != rrr.Rank1(4) {
		t.Errorf("expected rank 0; got %d", rrr.Rank1(4))
	}
	if 0 != rrr.Rank1(5) {
		t.Errorf("expected rank 0; got %d", rrr.Rank1(5))
	}
	if 1 != rrr.Rank1(6) {
		t.Errorf("expected rank 1; got %d", rrr.Rank1(6))
	}
	if 1 != rrr.Rank1(7) {
		t.Errorf("expected rank 1; got %d", rrr.Rank1(7))
	}
	if 1 != rrr.Rank1(63) {
		t.Errorf("expected rank 1; got %d", rrr.Rank1(63))
	}

	if 1 != rrr.Rank1(554) {
		t.Errorf("expected rank 1; got %d", rrr.Rank1(554))
	}
	if 1 != rrr.Rank1(555) {
		t.Errorf("expected rank 1; got %d", rrr.Rank1(555))
	}
	if 2 != rrr.Rank1(556) {
		t.Errorf("expected rank 2; got %d", rrr.Rank1(556))
	}

	// TODO: test edge cases!
	// Set (almost) all bits and check to make sure rank is monotonic

}

func TestSelect(t *testing.T) {
	bv := bv.New(4096)
	bv.Set(5, true)
	bv.Set(555, true)
	bv.Set(4000, true)

	rrr, err := NewFromBitVector(bv)
	if err != nil {
		t.Fatal(err)
	}

	if 5 != rrr.Select1(1) {
		t.Errorf("expected %d; got %d", 5, rrr.Select1(1))
	}
	if 555 != rrr.Select1(2) {
		t.Errorf("expected %d; got %d", 555, rrr.Select1(2))
	}
	if 4000 != rrr.Select1(3) {
		t.Errorf("expected %d; got %d", 4000, rrr.Select1(3))
	}

	if 0 != rrr.Select0(1) {
		t.Errorf("expected %d; got %d", 0, rrr.Select0(1))
	}
	if 1 != rrr.Select0(2) {
		t.Errorf("expected %d; got %d", 1, rrr.Select0(2))
	}
	if 2 != rrr.Select0(3) {
		t.Errorf("expected %d; got %d", 2, rrr.Select0(3))
	}
	if 3 != rrr.Select0(4) {
		t.Errorf("expected %d; got %d", 3, rrr.Select0(4))
	}
	if 4 != rrr.Select0(5) {
		t.Errorf("expected %d; got %d", 5, rrr.Select0(5))
	}
	if 6 != rrr.Select0(6) {
		t.Errorf("expected %d; got %d", 6, rrr.Select0(6))
	}
}

func TestWTF(t *testing.T) {
	bv := bv.New(6)
	bv.Set(0, true)
	bv.Set(2, true)
	bv.Set(4, true)
	fmt.Println(bv)

	rrr, err := NewFromBitVector(bv)
	if err != nil {
		t.Fatal(err)
	}
	if !rrr.Access(0) {
		t.Errorf("expected 0 to be set")
	}
	if !rrr.Access(2) {
		t.Errorf("expected 2 to be set")
	}
	if !rrr.Access(4) {
		t.Errorf("expected 4 to be set")
	}
}

func TestWTF2(t *testing.T) {
	bv := bv.New(11)
	bv.Set(1, true)
	bv.Set(2, true)
	bv.Set(3, true)
	bv.Set(6, true)
	bv.Set(8, true)
	bv.Set(9, true)

	fmt.Println(bv.Get(6))

	rrr, err := NewFromBitVector(bv)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(rrr.Access(6))
}

func TestSerialization(t *testing.T) {
	bv := bv.New(4096)
	for i := 0; i < 100; i++ {
		bv.Set(uint64(rand.Intn(4096)-1), true)
	}

	rrr, err := NewFromBitVector(bv)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	err = rrr.WriteTo(&buf)
	if err != nil {
		t.Fatal(err)
	}

	rrrRT, length, err := NewFromSerialized(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	if length != buf.Len() {
		t.Errorf("expected %d; got %d", length, buf.Len())
	}

	if rrrRT.Size() != rrr.Size() {
		t.Errorf("expected %d; got %d", rrr.Size(), rrrRT.Size())
	}

	for i := uint64(0); i < 4096; i++ {
		if rrr.Access(i) != rrrRT.Access(i) {
			t.Errorf("Access(%d). expected %t; got %t", i, rrr.Access(i), rrrRT.Access(i))
		}
		if rrr.Rank1(i) != rrrRT.Rank1(i) {
			t.Errorf("Access(%d). expected %t; got %t", i, rrr.Access(i), rrrRT.Access(i))
		}
	}
}

func TestMultipleOfSuperBlockSize(t *testing.T) {
	bv := bv.New(425376)
	_, err := NewFromBitVector(bv)
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkRRR(b *testing.B) {
	density := 80
	size := 1 << 20

	bv := bv.New(size)
	for i := 0; i < size; i++ {
		if rand.Intn(100) < density {
			bv.Set(uint64(i), true)
		}
	}

	r, err := NewFromBitVector(bv)
	if err != nil {
		b.Fatal(err)
	}

	rands := make([]int, 0, b.N)
	for i := 0; i < b.N; i++ {
		rands = append(rands, rand.Intn(size))
	}

	b.ResetTimer()
	n := 0
	for i := 0; i < b.N; i++ {
		set := r.Access(uint64(rands[i]))
		if set {
			n++
		}
	}
}

func BenchmarkBV(b *testing.B) {
	//density := 80
	density := 20
	size := 1 << 20

	bv := bv.New(size)
	for i := 0; i < size; i++ {
		if rand.Intn(100) < density {
			bv.Set(uint64(i), true)
		}
	}

	rands := make([]int, 0, b.N)
	for i := 0; i < b.N; i++ {
		rands = append(rands, rand.Intn(size))
	}

	b.ResetTimer()
	n := 0
	for j := 0; j < 10; j++ {
		for i := 0; i < b.N; i++ {
			set := bv.Get(uint64(rands[i]))
			if set {
				n++
			}
		}
	}
}
