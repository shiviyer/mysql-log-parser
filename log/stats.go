package log

type EventStats struct {
	TimeMetrics map[string]*TimeStats `json:",omitempty"`
	NumberMetrics map[string]*NumberStats `json:",omitempty"`
	BoolMetrics map[string]*BoolStats `json:",omitempty"`
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
		TimeMetrics: make(map[string]*TimeStats),
		NumberMetrics: make(map[string]*NumberStats),
		BoolMetrics: make(map[string]*BoolStats),
	}
	return s
}

func (s *EventStats) Add(e *Event) {
	for metric, val := range e.TimeMetrics {
		stats, seenMetric := s.TimeMetrics[metric]
		if seenMetric {
			// We've seen this metric before; update its stats.
			stats.Cnt++
			stats.Sum += val
			if val < stats.Min {
				stats.Min = val
			}
			if val > stats.Max {
				stats.Max = val
			}
		} else {
			// First time we've seen this metric; create its stats.
			s.TimeMetrics[metric] = &TimeStats{
				Cnt: 1,
				Sum: val,
				Min: val,
				Max: val,
			}
		}
	}
	for metric, val := range e.NumberMetrics {
		stats, seenMetric := s.NumberMetrics[metric]
		if seenMetric {
			// We've seen this metric before; update its stats.
			stats.Cnt++
			stats.Sum += val
			if val < stats.Min {
				stats.Min = val
			}
			if val > stats.Max {
				stats.Max = val
			}
		} else {
			// First time we've seen this metric; create its stats.
			s.NumberMetrics[metric] = &NumberStats{
				Cnt: 1,
				Sum: val,
				Min: val,
				Max: val,
			}
		}
	}
	for metric, val := range e.BoolMetrics {
		stats, seenMetric := s.BoolMetrics[metric]
		if seenMetric {
			// We've seen this metric before; update its stats.
			stats.Cnt++
			if val {
				stats.True++
			}
		} else {
			// First time we've seen this metric; create its stats.
			stats := &BoolStats{
				Cnt: 1,
			}
			if val {
				stats.True++
			}
			s.BoolMetrics[metric] = stats
		}
	}
}

// Make the stats current because some values (e.g. average) change as events are added.
// Call this function before accessing the stats, else some stats will be zero or incorrect.
// @todo median, stddev, 95th percentile
func (s *EventStats) Current() {
	for _, stats := range s.TimeMetrics {
		stats.Avg = stats.Sum / stats.Cnt
	}
	for _, stats := range s.NumberMetrics {
		stats.Avg = stats.Sum / stats.Cnt
	}
}
