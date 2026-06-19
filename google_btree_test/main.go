// google_btree_test/main.go
//
// mkdir google_btree_test && cd google_btree_test
// go mod init google_btree_test
// go get github.com/google/btree
//
// go run main.go

package main

import (
	"fmt"

	"github.com/google/btree"
)

func main() {
	// Your B-Tree demo code goes here
	tr := btree.NewG[int](2, func(a, b int) bool { return a < b })
	tr.ReplaceOrInsert(42)
	fmt.Println("Success! Length:", tr.Len())
}
