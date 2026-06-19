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

// 1. Define the item structure.
// To use Google's modern generic BTree, the type must implement btree.Ordered,
// or we can write a custom comparison function. Let's use a custom struct with an ID.
type Record struct {
	ID    int
	Value string
}

// 2. Define a comparison function to satisfy the Generic B-Tree requirements.
// The function must return true if a is less than b.
func compareRecords(a, b Record) bool {
	return a.ID < b.ID
}

func main() {
	// Initialize a new B-Tree with a degree of 2.
	// The degree determines the maximum number of items per node (interior nodes max = 2*degree).
	// Higher degrees are better for memory-locality and cache-line efficiency.
	tr := btree.NewG[Record](2, compareRecords)

	fmt.Println("--- 1. Ingestion / Inserts ---")
	mockData := []Record{
		{ID: 45, Value: "Operational Metric A"},
		{ID: 12, Value: "System Log Alpha"},
		{ID: 99, Value: "Security Alert Critical"},
		{ID: 32, Value: "Network Packet Payload"},
		{ID: 57, Value: "Firmware State Vector"},
	}

	for _, rec := range mockData {
		// ReplaceOrInsert returns the old item if an item with the same key existed, or nil/zero-value otherwise.
		tr.ReplaceOrInsert(rec)
		fmt.Printf("Inserted Record ID: %d\n", rec.ID)
	}
	fmt.Printf("Tree structure length: %d entries\n\n", tr.Len())

	fmt.Println("--- 2. Point Lookups ---")
	searchTarget := Record{ID: 32, Value: ""} // Only the ID matters for comparison
	if found, exists := tr.Get(searchTarget); exists {
		fmt.Printf("Match Found! ID: %d -> Value: %s\n\n", found.ID, found.Value)
	} else {
		fmt.Println("Record not found.\n")
	}

	fmt.Println("--- 3. In-Order Invalidation / Iteration ---")
	// Ascend walks the tree in sorted order. Returning false from the iterator terminates the walk.
	fmt.Println("Walking entire tree in sorted order:")
	tr.Ascend(func(r Record) bool {
		fmt.Printf("  [ID: %d] %s\n", r.ID, r.Value)
		return true // Keep iterating
	})
	fmt.Println()

	fmt.Println("--- 4. Fast Range Queries (Slicing) ---")
	// AscendRange allows you to efficiently scan a subset of data.
	// It scans from pivot Greater-Than-Or-Equal-To (GTE) up to Less-Than (LT) target pivot.
	startPivot := Record{ID: 30, Value: ""}
	endPivot := Record{ID: 60, Value: ""}

	fmt.Println("Scanning for records where 30 <= ID < 60:")
	tr.AscendRange(startPivot, endPivot, func(r Record) bool {
		fmt.Printf("  -> Range Hit: [ID: %d] %s\n", r.ID, r.Value)
		return true
	})
	fmt.Println()

	fmt.Println("--- 5. Deletion ---")
	deleteTarget := Record{ID: 12, Value: ""}
	if deleted, success := tr.Delete(deleteTarget); success {
		fmt.Printf("Successfully purged Record ID: %d from index.\n", deleted.ID)
	}
	fmt.Printf("Final Tree count: %d\n", tr.Len())
}
