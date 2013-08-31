package parser_test

import (
	"github.com/percona/percona-go-mysql/log"
	"github.com/percona/percona-go-mysql/log/parser"
	. "github.com/percona/percona-go-mysql/test"
	. "launchpad.net/gocheck"
	"testing"
)

// Hook gocheck into the "go test" runner.
// http://labix.org/gocheck
func Test(t *testing.T) { TestingT(t) }

/////////////////////////////////////////////////////////////////////////////
// Slow log test suite
// //////////////////////////////////////////////////////////////////////////

type SlowLogTestSuite struct {
	p *parser.SlowLogParser
}

var _ = Suite(&SlowLogTestSuite{})

func (s *SlowLogTestSuite) TestSlowLogParser(t *C) {
	got := ParseSlowLog("slow001.log")
	expect := []log.Event{
		{
			Ts:     "071015 21:43:52",
			Admin:  false,
			Query:  `select sleep(2) from n`,
			User:   "root",
			Host:   "localhost",
			Db:     "test",
			TimeMetrics: map[string]float32{
				"Query_time": 2,
				"Lock_time":  0,
			},
			NumberMetrics: map[string]uint64{
				"Rows_sent":     1,
				"Rows_examined": 0,
			},
			BoolMetrics: map[string]bool{},
		},
		{
			Ts:     "071015 21:45:10",
			Admin:  false,
			Query:  `select sleep(2) from test.n`,
			User:   "root",
			Host:   "localhost",
			Db:     "sakila",
			TimeMetrics: map[string]float32{
				"Query_time": 2,
				"Lock_time":  0,
			},
			NumberMetrics: map[string]uint64{
				"Rows_sent":     1,
				"Rows_examined": 0,
			},
			BoolMetrics: map[string]bool{},
		},
	}
	t.Check(got, EventsEqual, &expect)
}

func (s *SlowLogTestSuite) TestParseSlowLog002(t *C) {
	got := ParseSlowLog("slow002.log")
	expect := []log.Event{
		{
			Query:  "BEGIN",
			Ts:     "071218 11:48:27",
			Admin:  false,
			User:   "[SQL_SLAVE]",
			Host:   "",
			TimeMetrics: map[string]float32{
				"Query_time": 0.000012,
				"Lock_time":  0.000000,
			},
			NumberMetrics: map[string]uint64{
				"Merge_passes":  0,
				"Thread_id":     10,
				"Rows_examined": 0,
				"Rows_sent":     0,
			},
			BoolMetrics: map[string]bool{
				"Filesort":          false,
				"Full_scan":         false,
				"Filesort_on_disk":  false,
				"Full_join":         false,
				"Tmp_table_on_disk": false,
				"Tmp_table":         false,
				"QC_Hit":            false,
			},
		},
		{
			Db: "db1",
			Query: `update db2.tuningdetail_21_265507 n
      inner join db1.gonzo a using(gonzo) 
      set n.column1 = a.column1, n.word3 = a.word3`,
			Admin:  false,
			User:   "[SQL_SLAVE]",
			Host:   "",
			TimeMetrics: map[string]float32{
				"Query_time": 0.726052,
				"Lock_time":  0.000091,
			},
			NumberMetrics: map[string]uint64{
				"Merge_passes":  0,
				"Thread_id":     10,
				"Rows_examined": 62951,
				"Rows_sent":     0,
			},
			BoolMetrics: map[string]bool{
				"Filesort":          false,
				"Full_scan":         true,
				"Filesort_on_disk":  false,
				"Full_join":         false,
				"Tmp_table_on_disk": false,
				"Tmp_table":         false,
				"QC_Hit":            false,
			},
		},
		{
			Query: `INSERT INTO db3.vendor11gonzo (makef, bizzle)
VALUES ('', 'Exact')`,
			Admin:  false,
			User:   "[SQL_SLAVE]",
			Host:   "",
			TimeMetrics: map[string]float32{
				"InnoDB_queue_wait":    0.000000,
				"Lock_time":            0.000077,
				"InnoDB_rec_lock_wait": 0.000000,
				"Query_time":           0.000512,
				"InnoDB_IO_r_wait":     0.000000,
			},
			NumberMetrics: map[string]uint64{
				"InnoDB_IO_r_bytes":     0,
				"Merge_passes":          0,
				"InnoDB_pages_distinct": 24,
				"Rows_sent":             0,
				"Thread_id":             10,
				"Rows_examined":         0,
				"InnoDB_IO_r_ops":       0,
			},
			BoolMetrics: map[string]bool{
				"Filesort":          false,
				"Full_scan":         false,
				"Filesort_on_disk":  false,
				"Full_join":         false,
				"Tmp_table_on_disk": false,
				"Tmp_table":         false,
				"QC_Hit":            false,
			},
		},
		{
			Query: `UPDATE db4.vab3concept1upload
SET    vab3concept1id = '91848182522'
WHERE  vab3concept1upload='6994465'`,
			Admin:  false,
			User:   "[SQL_SLAVE]",
			Host:   "",
			TimeMetrics: map[string]float32{
				"Query_time":           0.033384,
				"InnoDB_IO_r_wait":     0.000000,
				"InnoDB_queue_wait":    0.000000,
				"Lock_time":            0.000028,
				"InnoDB_rec_lock_wait": 0.000000,
			},
			NumberMetrics: map[string]uint64{
				"InnoDB_IO_r_bytes":     0,
				"Merge_passes":          0,
				"InnoDB_pages_distinct": 11,
				"Rows_sent":             0,
				"Thread_id":             10,
				"Rows_examined":         0,
				"InnoDB_IO_r_ops":       0,
			},
			BoolMetrics: map[string]bool{
				"Filesort":          false,
				"Full_scan":         false,
				"Filesort_on_disk":  false,
				"Full_join":         false,
				"Tmp_table_on_disk": false,
				"Tmp_table":         false,
				"QC_Hit":            false,
			},
		},
		{
			Query: `INSERT INTO db1.conch (word3, vid83)
VALUES ('211', '18')`,
			Admin:  false,
			User:   "[SQL_SLAVE]",
			Host:   "",
			TimeMetrics: map[string]float32{
				"InnoDB_queue_wait":    0.000000,
				"Query_time":           0.000530,
				"InnoDB_IO_r_wait":     0.000000,
				"Lock_time":            0.000027,
				"InnoDB_rec_lock_wait": 0.000000,
			},
			NumberMetrics: map[string]uint64{
				"InnoDB_IO_r_bytes":     0,
				"Merge_passes":          0,
				"InnoDB_pages_distinct": 18,
				"Rows_sent":             0,
				"Thread_id":             10,
				"Rows_examined":         0,
				"InnoDB_IO_r_ops":       0,
			},
			BoolMetrics: map[string]bool{
				"Filesort":          false,
				"Full_scan":         false,
				"Filesort_on_disk":  false,
				"Full_join":         false,
				"Tmp_table_on_disk": false,
				"Tmp_table":         false,
				"QC_Hit":            false,
			},
		},
		{
			Query: `UPDATE foo.bar
SET    biz = '91848182522'`,
			Admin:  false,
			User:   "[SQL_SLAVE]",
			Host:   "",
			TimeMetrics: map[string]float32{
				"Lock_time":            0.000027,
				"InnoDB_rec_lock_wait": 0.000000,
				"InnoDB_queue_wait":    0.000000,
				"Query_time":           0.000530,
				"InnoDB_IO_r_wait":     0.000000,
			},
			NumberMetrics: map[string]uint64{
				"InnoDB_IO_r_bytes":     0,
				"Merge_passes":          0,
				"InnoDB_pages_distinct": 18,
				"Rows_sent":             0,
				"Thread_id":             10,
				"Rows_examined":         0,
				"InnoDB_IO_r_ops":       0,
			},
			BoolMetrics: map[string]bool{
				"Filesort":          false,
				"Full_scan":         false,
				"Filesort_on_disk":  false,
				"Full_join":         false,
				"Tmp_table_on_disk": false,
				"Tmp_table":         false,
				"QC_Hit":            false,
			},
		},
		{
			Query: `UPDATE bizzle.bat
SET    boop='bop: 899'
WHERE  fillze='899'`,
			Admin:  false,
			User:   "[SQL_SLAVE]",
			Host:   "",
			TimeMetrics: map[string]float32{
				"Query_time":           0.000530,
				"InnoDB_IO_r_wait":     0.000000,
				"InnoDB_queue_wait":    0.000000,
				"Lock_time":            0.000027,
				"InnoDB_rec_lock_wait": 0.000000,
			},
			NumberMetrics: map[string]uint64{
				"InnoDB_IO_r_bytes":     0,
				"Merge_passes":          0,
				"InnoDB_pages_distinct": 18,
				"Rows_sent":             0,
				"Thread_id":             10,
				"Rows_examined":         0,
				"InnoDB_IO_r_ops":       0,
			},
			BoolMetrics: map[string]bool{
				"Filesort":          false,
				"Full_scan":         false,
				"Filesort_on_disk":  false,
				"Full_join":         false,
				"Tmp_table_on_disk": false,
				"Tmp_table":         false,
				"QC_Hit":            false,
			},
		},
		{
			Query: `UPDATE foo.bar
SET    biz = '91848182522'`,
			Admin:  false,
			User:   "[SQL_SLAVE]",
			Host:   "",
			TimeMetrics: map[string]float32{
				"Query_time":           0.000530,
				"Lock_time":            0.000027,
				"InnoDB_rec_lock_wait": 0.000000,
				"InnoDB_queue_wait":    0.000000,
				"InnoDB_IO_r_wait":     0.000000,
			},
			NumberMetrics: map[string]uint64{
				"InnoDB_IO_r_bytes":     0,
				"Merge_passes":          0,
				"InnoDB_pages_distinct": 18,
				"Rows_sent":             0,
				"Thread_id":             10,
				"Rows_examined":         0,
				"InnoDB_IO_r_ops":       0,
			},
			BoolMetrics: map[string]bool{
				"Filesort":          false,
				"Full_scan":         false,
				"Filesort_on_disk":  false,
				"Full_join":         false,
				"Tmp_table_on_disk": false,
				"Tmp_table":         false,
				"QC_Hit":            false,
			},
		},
	}
	t.Check(got, EventsEqual, &expect)
}
