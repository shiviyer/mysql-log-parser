package log

import (
	"fmt"
	"time"
)

/////////////////////////////////////////////////////////////////////////////
// Global class
/////////////////////////////////////////////////////////////////////////////

type GlobalClass struct {
	TotalQueries  uint64
	UniqueQueries uint64
	RateType      string `json:",omitempty"`
	RateLimit     byte   `json:",omitempty"`
	Metrics       *EventStats
}

type MixedRateLimitsError struct {
	PrevRateType  string
	PrevRateLimit byte
	CurRateType   string
	CurRateLimit  byte
}

func (e MixedRateLimitsError) Error() string {
	return fmt.Sprintf("Mixed rate limits: have %s:%d, got %s:%d",
		e.PrevRateType, e.PrevRateLimit, e.CurRateType, e.CurRateLimit)
}

func NewGlobalClass() *GlobalClass {
	class := &GlobalClass{
		TotalQueries:  0,
		UniqueQueries: 0,
		Metrics:       NewEventStats(),
	}
	return class
}

func (c *GlobalClass) AddEvent(e *Event) error {
	var err error
	if e.RateType != "" {
		if c.RateType == "" {
			// Set rate limit for this gg
			c.RateType = e.RateType
			c.RateLimit = e.RateLimit
		} else {
			// Make sure the rate limit hasn't changed because it's not clear
			// how to handle a mix of rate limits.
			if c.RateType != e.RateType && c.RateLimit != e.RateLimit {
				err = MixedRateLimitsError{c.RateType, c.RateLimit, e.RateType, e.RateLimit}
			}
		}
	}
	c.TotalQueries++
	c.Metrics.Add(e)
	return err
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
	Example      Example `json:",omitempty"`
	example      bool
}

type Example struct {
	QueryTime float64
	Query     string
	Ts        string `json:",omitempty"`
}

func NewQueryClass(classId string, fingerprint string, example bool) *QueryClass {
	class := &QueryClass{
		Id:           classId,
		Fingerprint:  fingerprint,
		Metrics:      NewEventStats(),
		TotalQueries: 0,
		example:      example,
	}
	return class
}

func (c *QueryClass) AddEvent(e *Event) {
	c.TotalQueries++
	c.Metrics.Add(e)

	if c.example {
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
}

func (c *QueryClass) Finalize() {
	c.Metrics.Current()
}
