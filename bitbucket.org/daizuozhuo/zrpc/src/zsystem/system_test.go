package zsystem

import (
	"fmt"
	"testing"
)

func TestDistribute(t *testing.T) {
	stats := GetStatus()
	fmt.Print(stats)
}
