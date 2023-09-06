package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offsetWidth   uint64 = 4
	positionWidth uint64 = 8
	entWidth             = offsetWidth + positionWidth
)

// index defines persisted and memory-mapped file
type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

// newIndex creates a new index file, persistent file based on
// the configuration.
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())

	// pre-emptively grows the file to max index bytes
	if err := os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

// Close makes sure the memory-mapped file has synced its data
// to the persisted file and that the persisted file has
// flushed its contents to the stable storage.
func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	if err := i.file.Sync(); err != nil {
		return err
	}

	// re-sizes the file to the actual file size
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	return i.file.Close()
}

// Read takes in an offset and returns the associated record's position
// in the store.
// O is always the offset of the index's first entry.
func (i *index) Read(offset int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	out = uint32(offset)
	if offset == -1 {
		out = uint32((i.size / entWidth) - 1)
	}

	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	out = enc.Uint32(i.mmap[pos : pos+offsetWidth])
	pos = enc.Uint64(i.mmap[pos+offsetWidth : pos+entWidth])
	return out, pos, nil
}

// Write appends the given offset and position to the index.
func (i *index) Write(offset uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}

	enc.PutUint32(i.mmap[i.size:i.size+offsetWidth], offset)
	enc.PutUint64(i.mmap[i.size+offsetWidth:i.size+entWidth], pos)
	i.size += entWidth
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}
