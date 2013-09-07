package log

type EventStats struct {
	TimeMetrics map[string]TimeStats
	NumberMetrics map[string]NumberStats
	BoolMetrics map[string]BoolStats
}

type TimeStats struct {
	Cnt float32
	Sum float32
	Min float32
	Max float32
	Avg float32
}

type NumberStats struct {
	Cnt uint64
	Sum uint64
	Min uint64
	Max uint64
	Avg uint64
}

type BoolStats struct {
	Cnt uint64
	True uint8
}

func NewEventStats() *EventStats {
	s := &EventStats{
		TimeMetrics: make(map[string]TimeStats),
		NumberMetrics: make(map[string]NumberStats),
		BoolMetrics: make(map[string]BoolStats),
	}
	return s
}

func (s *EventStats) Add(e *Event) {
	for key, val := range e.TimeMetrics {
		m := s.TimeMetrics[key]
		m.Cnt++
		m.Sum += val
		if val < m.Min {
			m.Min = val
		}
		if val > m.Max {
			m.Max = val
		}
	}
	for key, val := range e.NumberMetrics {
		m := s.NumberMetrics[key]
		m.Cnt++
		m.Sum += val
		if val < m.Min {
			m.Min = val
		}
		if val > m.Max {
			m.Max = val
		}
	}
	for key, val := range e.BoolMetrics {
		m := s.BoolMetrics[key]
		m.Cnt++
		if val {
			m.True++
		}
	}
}
