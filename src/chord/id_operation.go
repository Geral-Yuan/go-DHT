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

// if id is in (begin, end) circularly then return true, o.w. return false
func in_range(id *big.Int, begin *big.Int, end *big.Int) bool {
	return end.Cmp(id)+id.Cmp(begin)+begin.Cmp(end) == 1 || (begin.Cmp(end) == 0 && begin.Cmp(id) != 0)
}

// return (id + (1 << i)) % (1 << M)
func calcID(id *big.Int, i uint) *big.Int {
	offset := new(big.Int).Lsh(big.NewInt(1), i)
	mod := new(big.Int).Lsh(big.NewInt(1), M)
	return new(big.Int).Mod(new(big.Int).Add(id, offset), mod)
}
