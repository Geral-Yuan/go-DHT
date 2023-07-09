package chord

import (
	"crypto/sha1"
	"math/big"
)

const M = 160 // the size of the hash table is 2 << M

func Hash(addr string) *big.Int {
	hash := sha1.New()
	hash.Write([]byte(addr))
	return (&big.Int{}).SetBytes(hash.Sum(nil))
}
