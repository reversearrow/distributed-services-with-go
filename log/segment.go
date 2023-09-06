package log

import (
	"fmt"
	log_v1 "github.com/reversearrow/distributed-computing-in-go/api/v1"
	"google.golang.org/protobuf/proto"
	"os"
	"path"
)

// segment wraps index and store types to coordinate operation
// across the two.
// For to write to the active segment,
// the segment needs to write the data to its store and add
// new entry to the index.
// For reads, the segment needs to lookup entry from the index
// and then fetch the data from the store.
type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}

	storeFileName := fmt.Sprintf("%d%s", baseOffset, ".store")

	filePath := path.Join(dir, storeFileName)
	storeFile, err := os.OpenFile(
		filePath,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, fmt.Errorf("error opening/creating store file: %w", err)
	}

	if s.store, err = newStore(storeFile); err != nil {
		return nil, fmt.Errorf("failed to create store file: %w", err)
	}

	indexFileName := fmt.Sprintf("%d%s", baseOffset, ".index")
	indexFile, err := os.OpenFile(
		path.Join(dir, indexFileName),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}

	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, fmt.Errorf("faile to open new index file: %w", err)
	}

	off, _, err := s.index.Read(-1)
	switch err {
	case nil:
		s.nextOffset = baseOffset + uint64(off) + 1
	default:
		s.nextOffset = baseOffset
	}

	return s, nil
}

// Append writes the record to the segment and returns' newly appended
// records' offset.
func (s *segment) Append(record *log_v1.Record) (offset uint64, err error) {
	currentOffset := s.nextOffset
	record.Offset = currentOffset

	p, err := proto.Marshal(record)
	if err != nil {
		return 0, fmt.Errorf("error marshalling record: %w", err)
	}

	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}

	if err := s.index.Write(
		uint32(s.nextOffset-s.baseOffset),
		pos,
	); err != nil {
		return 0, nil
	}
	s.nextOffset++
	return currentOffset, nil
}

// Read returns record for the given offset.
func (s *segment) Read(off uint64) (*log_v1.Record, error) {
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}

	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &log_v1.Record{}
	if err := proto.Unmarshal(p, record); err != nil {
		return nil, err
	}

	return record, nil
}

// IsMaxed is used to know if service needs to create a new segment.
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes
}

// Close closes the index and store files.
func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}

	if err := s.store.Close(); err != nil {
		return err
	}

	return nil
}

// Remove closes and removes index and store files.
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}

	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}

	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}

	return nil
}

// nearestMultiple nearest lesser multiple of k in J.
// for example (9, 4) == 8 ((4 * 2) < 9)
func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}

	return ((j - k + 1) / k) * k
}
