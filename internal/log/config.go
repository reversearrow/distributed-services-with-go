package log

// Config to centralize configuration for the log package
type Config struct {
	Segment Segment
}

// Segment stores configuration for the segment.
type Segment struct {
	MaxStoreBytes uint64
	MaxIndexBytes uint64
	InitialOffset uint64
}
