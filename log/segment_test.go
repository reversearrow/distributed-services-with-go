package log

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestSegment(t *testing.T) {
	dir, _ := os.CreateTemp("", "segment-test")
	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			t.Log("error removing temp directory")
		}
	}(dir.Name())

	//want := &log_v1.Record{Value: []byte("hello world")}

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entWidth * 3

	s, err := newSegment(dir.Name(), 16, c)
	require.NoError(t, err)
	fmt.Println(s.baseOffset)
	fmt.Println(s.nextOffset)

}
