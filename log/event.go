package log

import (
	"time"
)

type Event struct {
	Offset int64  // byte offset in log file, start of event
	Ts time.Time  // if present in log file, often times not
	Admin bool    // Query is admin command not SQL query
	Query string  // SQL query or admin command
	Class string  // fingerprint of Query
	Id uint32     // CRC32 checksum of Class
	Alias string  // very short form of Query (distill)
	User string
	Host string
	Db string
	MicroMetrics map[string]float32  // *_time and *_wait metrics
	IntMetrics map[string]uint32     // most metrics
	SizeMetrics map[string]uint64    // bytes and sizes metrics
	BoolMetrics map[string]bool      // yes/no metrics
}
