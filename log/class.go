package log

type EventClass struct {
	Name string  // fingerprint of Event.Query
	stats *ClassStats
	totalEvents uint64
}

func NewEventClass() *EventClass {
	class := new(EventClass)
	stats := new(ClassStats)
	class.stats = stats
	return class
}

func (c *EventClass) AddEvent(e *Event) {
	c.totalEvents++
	c.stats.Add(e)
}
