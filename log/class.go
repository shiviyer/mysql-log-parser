package log

import (
	"time"
)

/////////////////////////////////////////////////////////////////////////////
// Global class
/////////////////////////////////////////////////////////////////////////////

type GlobalClass struct {
	startTs time.Time
	endTs time.Time
	TotalQueries uint64
	UniqueQueriess uint64
	Metrics *EventStats
}

func NewGlobalClass() *GlobalClass {
	class := &GlobalClass{
		TotalQueries: 0,
		UniqueQueriess: 0,
		Metrics: NewEventStats(),
	}
	return class
}

func (c *GlobalClass) AddEvent(e *Event) {
	c.TotalQueries++
	c.Metrics.Add(e)
}

func (c *GlobalClass) Finalize(UniqueQueriess uint64) {
	c.UniqueQueriess = UniqueQueriess
	c.Metrics.Current()
}

/////////////////////////////////////////////////////////////////////////////
// Query class
/////////////////////////////////////////////////////////////////////////////

type QueryClass struct {
	Id string
	Fingerprint string
	Metrics *EventStats
	TotalQueries uint64
}

func NewQueryClass(classId string, fingerprint string) *QueryClass {
	class := &QueryClass{
		Id: classId,
		Fingerprint: fingerprint,
		Metrics: NewEventStats(),
		TotalQueries: 0,
	}
	return class
}

func (c *QueryClass) AddEvent(e *Event) {
	c.TotalQueries++
	c.Metrics.Add(e)
}

func (c *QueryClass) Finalize() {
	c.Metrics.Current()
}
