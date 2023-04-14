package task

import "testing"

func TestSlice(t *testing.T) {
	s := make([]byte, 1, 1)
	_ = s[0]
	_ = s[1:]
}
