package job

import "testing"

// Counter is simple enough that a single simple test seems like
// sufficient coverage here.

func TestIDsAreIncreasing(t *testing.T) {
	for range [100]int{} {
		first := GetNumber()
		second := GetNumber()

		if first > second {
			t.Error("numbers are not increasing")
		}
		if first+1 != second {
			t.Error("numbers are not increasing by one")
		}
	}
}
