package log

import (
	"sort"
	"github.com/vadimtk/gkquantile"
)

type EventStats struct {
	TimeMetrics   map[string]*TimeStats   `json:",omitempty"`
	NumberMetrics map[string]*NumberStats `json:",omitempty"`
	BoolMetrics   map[string]*BoolStats   `json:",omitempty"`
}

type TimeStats struct {
	vals   []float64 `json:"-"`
	Cnt    uint
	Sum    float64
	Min    float64
	Avg    float64
	Pct95  float64
	Stddev uint64
	Med    float64
	Max    float64
	GKq    *gkquantile.GKSummary
}

type NumberStats struct {
	vals   []uint64 `json:"-"`
	Cnt    uint
	Sum    uint64
	Min    uint64
	Avg    uint64
	Pct95  uint64
	Stddev uint64
	Med    uint64
	Max    uint64
}

type BoolStats struct {
	Cnt  uint
	True uint
}

func NewEventStats() *EventStats {
	s := &EventStats{
		TimeMetrics:   make(map[string]*TimeStats),
		NumberMetrics: make(map[string]*NumberStats),
		BoolMetrics:   make(map[string]*BoolStats),
	}
	return s
}

func (s *TimeStats) GetVals() []float64 {
	return s.vals

}

func (s *EventStats) Add(e *Event) {

	for metric, val := range e.TimeMetrics {
		stats, seenMetric := s.TimeMetrics[metric]
		if !seenMetric {
			s.TimeMetrics[metric] = &TimeStats{
				vals: []float64{},
				GKq: gkquantile.NewGKSummary(0.01),
			}
			stats = s.TimeMetrics[metric]
		}
		stats.Cnt++
		stats.Sum += float64(val)
		stats.vals = append(stats.vals, float64(val))
		stats.GKq.Add(float64(val))
	}

	for metric, val := range e.NumberMetrics {
		stats, seenMetric := s.NumberMetrics[metric]
		if !seenMetric {
			s.NumberMetrics[metric] = &NumberStats{
				vals: []uint64{},
			}
			stats = s.NumberMetrics[metric]
		}
		stats.Cnt++
		stats.Sum += val
		stats.vals = append(stats.vals, val)
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

type ByUint64 []uint64

func (a ByUint64) Len() int      { return len(a) }
func (a ByUint64) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByUint64) Less(i, j int) bool {
	return a[i] < a[j] // ascending order
}

// Make the stats current because some values (e.g. average) change as events are added.
// Call this function before accessing the stats, else some stats will be zero or incorrect.
// @todo median, stddev, 95th percentile
func (s *EventStats) Current() {
	for _, s := range s.TimeMetrics {
		sort.Float64s(s.vals)

		s.Min = s.vals[0]
		s.Avg = s.Sum / float64(s.Cnt)
		s.Pct95 = s.vals[(95*s.Cnt)/100]
		// s.Stddev = @todo
		s.Med = s.vals[(50*s.Cnt)/100] // median = 50th percentile
		s.Max = s.vals[s.Cnt-1]
		s.GKq.Compress()
	}

	for _, s := range s.NumberMetrics {
		sort.Sort(ByUint64(s.vals))

		s.Min = s.vals[0]
		s.Avg = s.Sum / uint64(s.Cnt)
		s.Pct95 = s.vals[(95*s.Cnt)/100]
		// s.Stddev = @todo
		s.Med = s.vals[(50*s.Cnt)/100] // median = 50th percentile
		s.Max = s.vals[s.Cnt-1]
	}
}
