package log

import (
	"fmt"
	log_v1 "github.com/reversearrow/distributed-computing-in-go/api/v1"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestSegment(t *testing.T) {
	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	//defer func(path string) {
	//	err := os.RemoveAll(path)
	//	if err != nil {
	//		t.Log("error removing temp directory")
	//	}
	//}(dir)

	want := &log_v1.Record{Value: []byte("hello world")}

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entWidth * 3

	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)
	require.Equal(t, uint64(16), s.nextOffset, s.baseOffset)
	require.False(t, s.IsMaxed())

	for i := uint64(0); i < 1; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)
		fmt.Println(off)

		got, err := s.Read(off)
		require.NoError(t, err)
		fmt.Println("got", got)
		//require.Equal(t, want.Value, got.Value)
	}

}
