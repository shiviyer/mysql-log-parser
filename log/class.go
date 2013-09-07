package log

/////////////////////////////////////////////////////////////////////////////
// Global class
/////////////////////////////////////////////////////////////////////////////

type GlobalClass struct {
	stats *EventStats
	totalEvents uint64
}

func NewGlobalClass(event *Event) *GlobalClass {
	class := &GlobalClass{
		stats: NewEventStats(),
	}
	return class
}

func (c *GlobalClass) AddEvent(e *Event) {
	c.totalEvents++
	c.stats.Add(e)
}

/////////////////////////////////////////////////////////////////////////////
// Query class
/////////////////////////////////////////////////////////////////////////////

type QueryClass struct {
	Id string
	Fingerprint string
	Distill string
	stats *EventStats
	totalEvents uint64
}

func NewQueryClass(classId string, fingerprint string) *QueryClass {
	class := &QueryClass{
		Id: classId,
		Fingerprint: fingerprint,
		Distill: "",
		stats: NewEventStats(),
		totalEvents: 0,
	}
	return class
}

func (c *QueryClass) AddEvent(e *Event) {
	c.totalEvents++
	c.stats.Add(e)
}

