package log

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

const (
	// iterations to run write and read for loop
	iterations = 4
)

var (
	write         = []byte("hello world")
	expectedWidth = uint64(len(write)) + binaryLengthWidth
)

func TestStoreAppendRead(t *testing.T) {
	t.Logf("creating a tempory file at: %v\n", os.TempDir())
	f, err := os.CreateTemp(os.TempDir(), "store_append_read_test")
	require.NoError(t, err)
	t.Logf("file created: %v\n", f.Name())

	defer func(name string) {
		err := os.Remove(name)
		if err != nil {
			t.Logf("error removing the file: %v\n", err)
		}
	}(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)
	t.Log("new store created")

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < iterations; i++ {
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		require.Equal(t, expectedWidth, n)
		require.Equal(t, expectedWidth*i, pos)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(0); i < iterations-1; i++ {
		read, err := s.Read(i * expectedWidth)
		require.NoError(t, err)
		require.Equal(t, write, read)
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, off := uint64(1), int64(0); i < 4; i++ {
		b := make([]byte, binaryLengthWidth)
		n, err := s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, binaryLengthWidth, n)
		off += int64(n)

		size := enc.Uint64(b)
		b = make([]byte, size)
		n, err = s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, write, b)
		require.Equal(t, int(size), n)
		off += int64(n)
	}
}
