package log

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestIndex(t *testing.T) {
	f, err := ioutil.TempFile(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	c := Config{}
	c.Segment.MaxIndexBytes = 1024

	idx, err := newIndex(f, c)
	require.NoError(t, err)

	_, _, err = idx.Read(-1)
	require.Error(t, err)
	require.Equal(t, f.Name(), idx.Name())

	entries := []struct{
		Off uint32
		Pos uint64
	}{
		{Off:0, Pos: 0},
		{Off:1, Pos: 10},
	}

	for _, want := range entries {
		err = idx.Write(want.Off, want.Pos)
		require.NoError(t, err)

		_, pos, err := idx.Read(int64(want.Off))
		require.NoError(t, err)
		require.Equal(t, want.Pos, pos)
	}

	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)
	err = idx.Close()
	require.NoError(t, err)

	fmt.Println("closed file")
	fmt.Println("reopening file")
	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = newIndex(f, c)
	require.NoError(t, err)

	off, pos, err := idx.Read(-1)
	require.NoError(t, err)
	fmt.Println(off)
	fmt.Println(pos)
	require.Equal(t, uint32(1), off)
	require.Equal(t, entries[1].Pos, pos)
}