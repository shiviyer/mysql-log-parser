package log

type Event struct {
	Offset uint64 // byte offset in log file, start of event
	Ts string     // if present in log file, often times not
	Admin bool    // Query is admin command not SQL query
	Query string  // SQL query or admin command
	User string
	Host string
	Db string
	TimeMetrics map[string]float32   // *_time and *_wait metrics
	NumberMetrics map[string]uint64  // most metrics
	BoolMetrics map[string]bool      // yes/no metrics
}

func NewEvent() *Event {
	event := new(Event)
	event.TimeMetrics = make(map[string]float32)
	event.NumberMetrics = make(map[string]uint64)
	event.BoolMetrics = make(map[string]bool)
	return event
}

type EventDescription struct {
	Class string  // fingerprint of Query
	Id uint32     // CRC32 checksum of Class
	Alias string  // very short form of Query (distill)
}
