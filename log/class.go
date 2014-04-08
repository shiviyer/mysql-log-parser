package log

import (
	"time"
)

/////////////////////////////////////////////////////////////////////////////
// Global class
/////////////////////////////////////////////////////////////////////////////

type GlobalClass struct {
	startTs       time.Time
	endTs         time.Time
	TotalQueries  uint64
	UniqueQueries uint64
	Metrics       *EventStats
}

func NewGlobalClass() *GlobalClass {
	class := &GlobalClass{
		TotalQueries:  0,
		UniqueQueries: 0,
		Metrics:       NewEventStats(),
	}
	return class
}

func (c *GlobalClass) AddEvent(e *Event) {
	c.TotalQueries++
	c.Metrics.Add(e)
}

func (c *GlobalClass) Finalize(UniqueQueries uint64) {
	c.UniqueQueries = UniqueQueries
	c.Metrics.Current()
}

/////////////////////////////////////////////////////////////////////////////
// Query class
/////////////////////////////////////////////////////////////////////////////

type QueryClass struct {
	Id           string
	Fingerprint  string
	Metrics      *EventStats
	TotalQueries uint64
	Example      Example
}

type Example struct {
	QueryTime float64
	Query     string
	Ts        string `json:",omitempty"`
}

func NewQueryClass(classId string, fingerprint string) *QueryClass {
	class := &QueryClass{
		Id:           classId,
		Fingerprint:  fingerprint,
		Metrics:      NewEventStats(),
		TotalQueries: 0,
	}
	return class
}

func (c *QueryClass) AddEvent(e *Event) {
	c.TotalQueries++
	c.Metrics.Add(e)
	if n, ok := e.TimeMetrics["Query_time"]; ok {
		if float64(n) > c.Example.QueryTime {
			c.Example.QueryTime = float64(n)
			c.Example.Query = e.Query
			if e.Ts != "" {
				if t, err := time.Parse("060102 15:04:05", e.Ts); err != nil {
					c.Example.Ts = ""
				} else {
					c.Example.Ts = t.Format("2006-01-02 15:04:05")
				}
			} else {
				c.Example.Ts = ""
			}
		}
	}
}

func (c *QueryClass) Finalize() {
	c.Metrics.Current()
}
