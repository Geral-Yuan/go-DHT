package chord

import (
	"crypto/sha1"
	"math/big"
)

const M uint = 160 // the size of the hash table is 1 << M

func Hash(addr string) *big.Int {
	hash := sha1.New()
	hash.Write([]byte(addr))
	return (&big.Int{}).SetBytes(hash.Sum(nil))
}
