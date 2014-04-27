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
	p   *parser.SlowLogParser
	opt parser.Options
}

var _ = Suite(&SlowLogTestSuite{})

// No input, no events.
func (s *SlowLogTestSuite) TestParserEmptySlowLog(t *C) {
	got := ParseSlowLog("empty.log", s.opt)
	expect := []log.Event{}
	t.Check(got, EventsEqual, &expect)
}

// slow001 is a most basic basic, normal slow log--nothing exotic.
func (s *SlowLogTestSuite) TestParserSlowLog001(t *C) {
	got := ParseSlowLog("slow001.log", s.opt)
	expect := []log.Event{
		{
			Ts:     "071015 21:43:52",
			Admin:  false,
			Query:  `select sleep(2) from n`,
			User:   "root",
			Host:   "localhost",
			Db:     "test",
			Offset: 200,
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
			Offset: 359,
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
	if same, diff := IsDeeply(got, &expect); !same {
		Dump(got)
		t.Error(diff)
	}
}

// slow002 is a basic slow log like slow001 but with more metrics, multi-line queries, etc.
func (s *SlowLogTestSuite) TestParseSlowLog002(t *C) {
	got := ParseSlowLog("slow002.log", s.opt)
	expect := []log.Event{
		{
			Query:  "BEGIN",
			Ts:     "071218 11:48:27",
			Admin:  false,
			User:   "[SQL_SLAVE]",
			Host:   "",
			Offset: 0,
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
			Offset: 338,
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
			Offset: 815,
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
			Offset: 1334,
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
			Offset: 1864,
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
			Offset: 2393,
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
			Offset: 2861,
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
			Offset: 3374,
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
	if same, diff := IsDeeply(got, &expect); !same {
		Dump(got)
		t.Error(diff)
	}
}

// slow003 starts with a blank line.  I guess this once messed up SlowLogParser.pm?
func (s *SlowLogTestSuite) TestParserSlowLog003(t *C) {
	got := ParseSlowLog("slow003.log", s.opt)
	expect := []log.Event{
		{
			Query:  "BEGIN",
			Admin:  false,
			Host:   "",
			Ts:     "071218 11:48:27",
			User:   "[SQL_SLAVE]",
			Offset: 2,
			BoolMetrics: map[string]bool{
				"Filesort_on_disk":  false,
				"Tmp_table_on_disk": false,
				"Filesort":          false,
				"Full_join":         false,
				"Full_scan":         false,
				"QC_Hit":            false,
				"Tmp_table":         false,
			},
			TimeMetrics: map[string]float32{
				"Lock_time":  0.000000,
				"Query_time": 0.000012,
			},
			NumberMetrics: map[string]uint64{
				"Merge_passes":  0,
				"Rows_examined": 0,
				"Rows_sent":     0,
				"Thread_id":     10,
			},
		},
	}
	if same, diff := IsDeeply(got, &expect); !same {
		Dump(got)
		t.Error(diff)
	}
}

// I don't know what's special about this slow004.
func (s *SlowLogTestSuite) TestParserSlowLog004(t *C) {
	got := ParseSlowLog("slow004.log", s.opt)
	expect := []log.Event{
		{
			Query:       "select 12_13_foo from (select 12foo from 123_bar) as 123baz",
			Admin:       false,
			Host:        "localhost",
			Ts:          "071015 21:43:52",
			User:        "root",
			Offset:      200,
			BoolMetrics: map[string]bool{},
			TimeMetrics: map[string]float32{
				"Lock_time":  0.000000,
				"Query_time": 2.000000,
			},
			NumberMetrics: map[string]uint64{
				"Rows_sent":     1,
				"Rows_examined": 0,
			},
		},
	}
	if same, diff := IsDeeply(got, &expect); !same {
		Dump(got)
		t.Error(diff)
	}
}

// slow005 has a multi-line query with tabs in it.  A pathological case that
// would probably break the parser is a query like:
//   SELECT * FROM foo WHERE col = "Hello
//   # Query_time: 10
//   " LIMIT 1;
// There's no easy way to detect that "# Query_time" is part of the query and
// not part of the next event's header.
func (s *SlowLogTestSuite) TestParserSlowLog005(t *C) {
	got := ParseSlowLog("slow005.log", s.opt)
	expect := []log.Event{
		{
			Query:  "foo\nbar\n\t\t\t0 AS counter\nbaz",
			Admin:  false,
			Host:   "",
			Ts:     "071218 11:48:27",
			User:   "[SQL_SLAVE]",
			Offset: 0,
			BoolMetrics: map[string]bool{
				"Filesort_on_disk":  false,
				"Tmp_table_on_disk": false,
				"Filesort":          false,
				"Full_join":         false,
				"Full_scan":         false,
				"QC_Hit":            false,
				"Tmp_table":         false,
			},
			TimeMetrics: map[string]float32{
				"Query_time": 0.000012,
				"Lock_time":  0.000000,
			},
			NumberMetrics: map[string]uint64{
				"Merge_passes":  0,
				"Rows_examined": 0,
				"Rows_sent":     0,
				"Thread_id":     10,
			},
		},
	}
	if same, diff := IsDeeply(got, &expect); !same {
		Dump(got)
		t.Error(diff)
	}
}

// slow006 has the Schema: db metric _or_ use db; lines before the queries.
// Schema value should be used for log.Event.Db is no use db; line is present.
func (s *SlowLogTestSuite) TestParserSlowLog006(t *C) {
	got := ParseSlowLog("slow006.log", s.opt)
	expect := []log.Event{
		{
			Query:  "SELECT col FROM foo_tbl",
			Db:     "foo",
			Admin:  false,
			Host:   "",
			Ts:     "071218 11:48:27",
			User:   "[SQL_SLAVE]",
			Offset: 0,
			BoolMetrics: map[string]bool{
				"Filesort_on_disk":  false,
				"Tmp_table_on_disk": false,
				"Filesort":          false,
				"Full_join":         false,
				"Full_scan":         false,
				"QC_Hit":            false,
				"Tmp_table":         false,
			},
			TimeMetrics: map[string]float32{
				"Query_time": 0.000012,
				"Lock_time":  0.000000,
			},
			NumberMetrics: map[string]uint64{
				"Merge_passes":  0,
				"Rows_examined": 0,
				"Rows_sent":     0,
				"Thread_id":     10,
			},
		},
		{
			Query:  "SELECT col FROM foo_tbl",
			Db:     "foo",
			Admin:  false,
			Host:   "",
			Ts:     "071218 11:48:57",
			User:   "[SQL_SLAVE]",
			Offset: 369,
			BoolMetrics: map[string]bool{
				"Filesort_on_disk":  false,
				"Tmp_table_on_disk": false,
				"Filesort":          false,
				"Full_join":         false,
				"Full_scan":         false,
				"QC_Hit":            false,
				"Tmp_table":         false,
			},
			TimeMetrics: map[string]float32{
				"Query_time": 0.000012,
				"Lock_time":  0.000000,
			},
			NumberMetrics: map[string]uint64{
				"Merge_passes":  0,
				"Rows_examined": 0,
				"Rows_sent":     0,
				"Thread_id":     10,
			},
		},
		{
			Query:  "SELECT col FROM bar_tbl",
			Db:     "bar",
			Admin:  false,
			Host:   "",
			Ts:     "071218 11:48:57",
			User:   "[SQL_SLAVE]",
			Offset: 737,
			BoolMetrics: map[string]bool{
				"Filesort_on_disk":  false,
				"Tmp_table_on_disk": false,
				"Filesort":          false,
				"Full_join":         false,
				"Full_scan":         false,
				"QC_Hit":            false,
				"Tmp_table":         false,
			},
			TimeMetrics: map[string]float32{
				"Query_time": 0.000012,
				"Lock_time":  0.000000,
			},
			NumberMetrics: map[string]uint64{
				"Merge_passes":  0,
				"Rows_examined": 0,
				"Rows_sent":     0,
				"Thread_id":     20,
			},
		},
		{
			Query:  "SELECT col FROM bar_tbl",
			Db:     "bar",
			Admin:  false,
			Host:   "",
			Ts:     "071218 11:49:05",
			User:   "[SQL_SLAVE]",
			Offset: 1101,
			BoolMetrics: map[string]bool{
				"Filesort_on_disk":  false,
				"Tmp_table_on_disk": false,
				"Filesort":          false,
				"Full_join":         false,
				"Full_scan":         false,
				"QC_Hit":            false,
				"Tmp_table":         false,
			},
			TimeMetrics: map[string]float32{
				"Query_time": 0.000012,
				"Lock_time":  0.000000,
			},
			NumberMetrics: map[string]uint64{
				"Merge_passes":  0,
				"Rows_examined": 0,
				"Rows_sent":     0,
				"Thread_id":     10,
			},
		},
		{
			Query:  "SELECT col FROM bar_tbl",
			Db:     "bar",
			Admin:  false,
			Host:   "",
			Ts:     "071218 11:49:07",
			User:   "[SQL_SLAVE]",
			Offset: 1469,
			BoolMetrics: map[string]bool{
				"Filesort_on_disk":  false,
				"Tmp_table_on_disk": false,
				"Filesort":          false,
				"Full_join":         false,
				"Full_scan":         false,
				"QC_Hit":            false,
				"Tmp_table":         false,
			},
			TimeMetrics: map[string]float32{
				"Query_time": 0.000012,
				"Lock_time":  0.000000,
			},
			NumberMetrics: map[string]uint64{
				"Merge_passes":  0,
				"Rows_examined": 0,
				"Rows_sent":     0,
				"Thread_id":     20,
			},
		},
		{
			Query:  "SELECT col FROM foo_tbl",
			Db:     "foo",
			Admin:  false,
			Host:   "",
			Ts:     "071218 11:49:30",
			User:   "[SQL_SLAVE]",
			Offset: 1833,
			BoolMetrics: map[string]bool{
				"Filesort_on_disk":  false,
				"Tmp_table_on_disk": false,
				"Filesort":          false,
				"Full_join":         false,
				"Full_scan":         false,
				"QC_Hit":            false,
				"Tmp_table":         false,
			},
			TimeMetrics: map[string]float32{
				"Query_time": 0.000012,
				"Lock_time":  0.000000,
			},
			NumberMetrics: map[string]uint64{
				"Merge_passes":  0,
				"Rows_examined": 0,
				"Rows_sent":     0,
				"Thread_id":     30,
			},
		},
	}
	if same, diff := IsDeeply(got, &expect); !same {
		Dump(got)
		t.Error(diff)
	}
}

// slow007 has Schema: db1 _and_ use db2;.  db2 should be used.
func (s *SlowLogTestSuite) TestParserSlowLog007(t *C) {
	got := ParseSlowLog("slow007.log", s.opt)
	expect := []log.Event{
		{
			Query:       "SELECT fruit FROM trees",
			Db:          "db2",
			Admin:       false,
			Host:        "",
			Ts:          "071218 11:48:27",
			User:        "[SQL_SLAVE]",
			Offset:      0,
			BoolMetrics: map[string]bool{},
			TimeMetrics: map[string]float32{
				"Query_time": 0.000012,
				"Lock_time":  0.000000,
			},
			NumberMetrics: map[string]uint64{
				"Rows_examined": 0,
				"Rows_sent":     0,
				"Thread_id":     3,
			},
		},
	}
	if same, diff := IsDeeply(got, &expect); !same {
		Dump(got)
		t.Error(diff)
	}
}

// slow008 has 4 interesting things (which makes it a poor test case since we're
// testing many things at once):
//   1) an admin command, e.g.: # administrator command: Quit;
//   2) a SET NAMES query; SET <certain vars> are ignored
//   3) No Time metrics
//   4) IPs in the host metric, but we don't currently support these
func (s *SlowLogTestSuite) TestParserSlowLog008(t *C) {
	got := ParseSlowLog("slow008.log", s.opt)
	expect := []log.Event{
		{
			Query:       "Quit",
			Db:          "db1",
			Admin:       true,
			Host:        "",
			User:        "meow",
			Offset:      0,
			BoolMetrics: map[string]bool{},
			TimeMetrics: map[string]float32{
				"Query_time": 0.000002,
				"Lock_time":  0.000000,
			},
			NumberMetrics: map[string]uint64{
				"Rows_examined": 0,
				"Rows_sent":     0,
				"Thread_id":     5,
			},
		},
		{
			Query:       "SET NAMES utf8",
			Db:          "db",
			Admin:       false,
			Host:        "",
			User:        "meow",
			Offset:      221,
			BoolMetrics: map[string]bool{},
			TimeMetrics: map[string]float32{
				"Query_time": 0.000899,
				"Lock_time":  0.000000,
			},
			NumberMetrics: map[string]uint64{
				"Rows_examined": 0,
				"Rows_sent":     0,
				"Thread_id":     6,
			},
		},
		{
			Query:       "SELECT MIN(id),MAX(id) FROM tbl",
			Db:          "db2",
			Admin:       false,
			Host:        "",
			User:        "meow",
			Offset:      435,
			BoolMetrics: map[string]bool{},
			TimeMetrics: map[string]float32{
				"Query_time": 0.018799,
				"Lock_time":  0.009453,
			},
			NumberMetrics: map[string]uint64{
				"Rows_examined": 0,
				"Rows_sent":     0,
				"Thread_id":     6,
			},
		},
	}
	if same, diff := IsDeeply(got, &expect); !same {
		Dump(got)
		t.Error(diff)
	}
}

// Filter admin commands
func (s *SlowLogTestSuite) TestParserSlowLog009(t *C) {
	opt := parser.Options{
		FilterAdminCommand: map[string]bool{
			"Quit": true,
		},
	}
	got := ParseSlowLog("slow009.log", opt)
	expect := []log.Event{
		{
			Query:  "Refresh",
			Db:     "",
			Admin:  true,
			Host:   "localhost",
			User:   "root",
			Offset: 197,
			Ts:     "090311 18:11:50",
			TimeMetrics: map[string]float32{
				"Query_time": 0.017850,
				"Lock_time":  0.000000,
			},
			NumberMetrics: map[string]uint64{
				"Rows_examined": 0,
				"Rows_sent":     0,
				"Thread_id":     47,
			},
			BoolMetrics: map[string]bool{
				"QC_Hit":            false,
				"Full_scan":         false,
				"Full_join":         false,
				"Tmp_table":         false,
				"Tmp_table_on_disk": false,
				"Filesort":          false,
				"Filesort_on_disk":  false,
			},
		},
	}
	if same, diff := IsDeeply(got, &expect); !same {
		Dump(got)
		t.Error(diff)
	}
}

// Rate limit
func (s *SlowLogTestSuite) TestParserSlowLog011(t *C) {
	got := ParseSlowLog("slow011.log", parser.Options{})
	expect := []log.Event{
		{
			Offset:    0,
			Query:     "SELECT foo FROM bar WHERE id=1",
			Db:        "maindb",
			Host:      "localhost",
			User:      "user1",
			Ts:        "131128  1:05:31",
			RateType:  "query",
			RateLimit: 2,
			TimeMetrics: map[string]float32{
				"Query_time":           0.000228,
				"Lock_time":            0.000114,
				"InnoDB_IO_r_wait":     0.000000,
				"InnoDB_rec_lock_wait": 0.000000,
				"InnoDB_queue_wait":    0.000000,
			},
			NumberMetrics: map[string]uint64{
				"Rows_sent":             1,
				"Rows_examined":         1,
				"Rows_affected":         0,
				"Bytes_sent":            545,
				"Tmp_tables":            0,
				"Tmp_disk_tables":       0,
				"Tmp_table_sizes":       0,
				"Merge_passes":          0,
				"InnoDB_IO_r_ops":       0,
				"InnoDB_IO_r_bytes":     0,
				"InnoDB_pages_distinct": 2,
			},
			BoolMetrics: map[string]bool{
				"QC_Hit":            false,
				"Full_scan":         false,
				"Full_join":         false,
				"Tmp_table":         false,
				"Tmp_table_on_disk": false,
				"Filesort":          false,
				"Filesort_on_disk":  false,
			},
		},
		{
			Offset:    733,
			Query:     "SELECT foo FROM bar WHERE id=2",
			Db:        "maindb",
			Host:      "localhost",
			User:      "user1",
			RateType:  "query",
			RateLimit: 2,
			TimeMetrics: map[string]float32{
				"Query_time":           0.000237,
				"Lock_time":            0.000122,
				"InnoDB_IO_r_wait":     0.000000,
				"InnoDB_rec_lock_wait": 0.000000,
				"InnoDB_queue_wait":    0.000000,
			},
			NumberMetrics: map[string]uint64{
				"Rows_sent":             1,
				"Rows_examined":         1,
				"Rows_affected":         0,
				"Bytes_sent":            545,
				"Tmp_tables":            0,
				"Tmp_disk_tables":       0,
				"Tmp_table_sizes":       0,
				"Merge_passes":          0,
				"InnoDB_IO_r_ops":       0,
				"InnoDB_IO_r_bytes":     0,
				"InnoDB_pages_distinct": 2,
			},
			BoolMetrics: map[string]bool{
				"QC_Hit":            false,
				"Full_scan":         false,
				"Full_join":         false,
				"Tmp_table":         false,
				"Tmp_table_on_disk": false,
				"Filesort":          false,
				"Filesort_on_disk":  false,
			},
		},
		{
			Offset:    1441,
			Query:     "INSERT INTO foo VALUES (NULL, 3)",
			Db:        "maindb",
			Host:      "localhost",
			User:      "user1",
			RateType:  "query",
			RateLimit: 2,
			TimeMetrics: map[string]float32{
				"Query_time":           0.000165,
				"Lock_time":            0.000048,
				"InnoDB_IO_r_wait":     0.000000,
				"InnoDB_rec_lock_wait": 0.000000,
				"InnoDB_queue_wait":    0.000000,
			},
			NumberMetrics: map[string]uint64{
				"Rows_sent":             5,
				"Rows_examined":         10,
				"Rows_affected":         0,
				"Bytes_sent":            481,
				"Tmp_tables":            0,
				"Tmp_disk_tables":       0,
				"Tmp_table_sizes":       0,
				"Merge_passes":          0,
				"InnoDB_IO_r_ops":       0,
				"InnoDB_IO_r_bytes":     0,
				"InnoDB_pages_distinct": 3,
			},
			BoolMetrics: map[string]bool{
				"QC_Hit":            false,
				"Full_scan":         false,
				"Full_join":         false,
				"Tmp_table":         false,
				"Tmp_table_on_disk": false,
				"Filesort":          true,
				"Filesort_on_disk":  false,
			},
		},
	}
	if same, diff := IsDeeply(got, &expect); !same {
		Dump(got)
		t.Error(diff)
	}
}

func (s *SlowLogTestSuite) TestParserSlowLog012(t *C) {
	got := ParseSlowLog("slow012.log", s.opt)
	expect := []log.Event{
		{
			Query:  "select * from mysql.user",
			Db:     "",
			Host:   "localhost",
			User:   "msandbox",
			Offset: 0,
			TimeMetrics: map[string]float32{
				"Query_time": 0.000214,
				"Lock_time":  0.000086,
			},
			NumberMetrics: map[string]uint64{
				"Rows_sent":     2,
				"Rows_examined": 2,
			},
		},
		{
			Query:  "Quit",
			Admin:  true,
			Db:     "",
			Host:   "localhost",
			User:   "msandbox",
			Offset: 186,
			TimeMetrics: map[string]float32{
				"Query_time": 0.000016,
				"Lock_time":  0.000000,
			},
			NumberMetrics: map[string]uint64{
				"Rows_sent":     2,
				"Rows_examined": 2,
			},
		},
		{
			Query:  "SELECT @@max_allowed_packet",
			Db:     "dev_pct",
			Host:   "localhost",
			User:   "msandbox",
			Offset: 376,
			Ts:     "140413 19:34:13",
			TimeMetrics: map[string]float32{
				"Query_time": 0.000127,
				"Lock_time":  0.000000,
			},
			NumberMetrics: map[string]uint64{
				"Rows_sent":     1,
				"Rows_examined": 0,
			},
		},
	}
	if same, diff := IsDeeply(got, &expect); !same {
		Dump(got)
		t.Error(diff)
	}
}

// Stack overflow bug due to meta lines.
func (s *SlowLogTestSuite) TestParserSlowLog013(t *C) {
	got := ParseSlowLog("slow013.log", parser.Options{Debug: false})
	expect := []log.Event{
		{
			Offset: 0,
			Ts:     "140224 22:39:34",
			Query:  "select 950,q.* from qcm q INTO OUTFILE '/mnt/pct/exp/qcm_db950.txt'",
			User:   "root",
			Host:   "localhost",
			Db:     "db950",
			TimeMetrics: map[string]float32{
				"Query_time": 21.876617,
				"Lock_time":  0.002991,
			},
			NumberMetrics: map[string]uint64{
				"Bytes_sent":    14,
				"Killed":        0,
				"Last_errno":    0,
				"Rows_affected": 1605306,
				"Rows_examined": 1605306,
				"Rows_sent":     1605306,
			},
		},
		{
			Offset: 354,
			Ts:     "140224 22:39:59",
			Query:  "select 961,q.* from qcm q INTO OUTFILE '/mnt/pct/exp/qcm_db961.txt'",
			User:   "root",
			Host:   "localhost",
			Db:     "db961",
			TimeMetrics: map[string]float32{
				"Query_time": 20.304537,
				"Lock_time":  0.103324,
			},
			NumberMetrics: map[string]uint64{
				"Bytes_sent":    14,
				"Killed":        0,
				"Last_errno":    0,
				"Rows_affected": 1197472,
				"Rows_examined": 1197472,
				"Rows_sent":     1197472,
			},
		},
		{
			Offset: 6139,
			Ts:     "140311 16:07:40",
			Query:  "select count(*) into @discard from `information_schema`.`PARTITIONS`",
			User:   "debian-sys-maint",
			Host:   "localhost",
			Db:     "",
			TimeMetrics: map[string]float32{
				"Query_time": 94.38144,
				"Lock_time":  0.000174,
			},
			NumberMetrics: map[string]uint64{
				"Bytes_sent":    11,
				"Killed":        0,
				"Last_errno":    1146,
				"Rows_affected": 1,
				"Rows_examined": 17799,
				"Rows_sent":     0,
			},
		},
		{
			Offset: 6667,
			Ts:     "140312 20:28:40",
			Query:  "select 1,q.* from qcm q INTO OUTFILE '/mnt/pct/exp/qcm_db1.txt'",
			User:   "root",
			Host:   "localhost",
			Db:     "db1",
			TimeMetrics: map[string]float32{
				"Query_time": 407.54025,
				"Lock_time":  0.122377,
			},
			NumberMetrics: map[string]uint64{
				"Bytes_sent":    19,
				"Killed":        0,
				"Last_errno":    0,
				"Rows_affected": 34621308,
				"Rows_examined": 34621308,
				"Rows_sent":     34621308,
			},
		},
		{
			Offset: 7015,
			Ts:     "140312 20:29:40",
			Query:  "select 1006,q.* from qcm q INTO OUTFILE '/mnt/pct/exp/qcm_db1006.txt'",
			User:   "root",
			Host:   "localhost",
			Db:     "db1006",
			TimeMetrics: map[string]float32{
				"Query_time": 60.507698,
				"Lock_time":  0.002719,
			},
			NumberMetrics: map[string]uint64{
				"Bytes_sent":    14,
				"Killed":        0,
				"Last_errno":    0,
				"Rows_affected": 4937738,
				"Rows_examined": 4937738,
				"Rows_sent":     4937738,
			},
		},
	}
	if same, diff := IsDeeply(got, &expect); !same {
		Dump(got)
		t.Error(diff)
	}
}

// Query line looks like header line.
func (s *SlowLogTestSuite) TestParserSlowLog014(t *C) {
	got := ParseSlowLog("slow014.log", s.opt)
	expect := []log.Event{
		{
			Offset: 0,
			Admin:  false,
			Query:  "SELECT * FROM cache\n WHERE `cacheid` IN ('id15965')",
			User:   "root",
			Host:   "localhost",
			Db:     "db1",
			TimeMetrics: map[string]float32{
				"InnoDB_IO_r_wait":     0,
				"InnoDB_queue_wait":    0,
				"InnoDB_rec_lock_wait": 0,
				"Lock_time":            4.7e-05,
				"Query_time":           0.000179,
			},
			NumberMetrics: map[string]uint64{
				"Bytes_sent":            2004,
				"InnoDB_IO_r_bytes":     0,
				"InnoDB_IO_r_ops":       0,
				"InnoDB_pages_distinct": 3,
				"InnoDB_trx_id":         0,
				"Killed":                0,
				"Last_errno":            0,
				"Merge_passes":          0,
				"Rows_affected":         0,
				"Rows_examined":         1,
				"Rows_read":             1,
				"Rows_sent":             1,
				"Thread_id":             103375137,
				"Tmp_disk_tables":       0,
				"Tmp_table_sizes":       0,
				"Tmp_tables":            0,
			},
			BoolMetrics: map[string]bool{
				"Filesort":          false,
				"Filesort_on_disk":  false,
				"Full_join":         false,
				"Full_scan":         false,
				"QC_Hit":            false,
				"Tmp_table":         false,
				"Tmp_table_on_disk": false,
			},
		},
		{
			/**
			 * Here it is:
			 */
			Offset: 691,
			Admin:  false,
			Query:  "### Channels ###\n\u0009\u0009\u0009\u0009\u0009SELECT sourcetable, IF(f.lastcontent = 0, f.lastupdate, f.lastcontent) AS lastactivity,\n\u0009\u0009\u0009\u0009\u0009f.totalcount AS activity, type.class AS type,\n\u0009\u0009\u0009\u0009\u0009(f.nodeoptions \u0026 512) AS noUnsubscribe\n\u0009\u0009\u0009\u0009\u0009FROM node AS f\n\u0009\u0009\u0009\u0009\u0009INNER JOIN contenttype AS type ON type.contenttypeid = f.contenttypeid \n\n\u0009\u0009\u0009\u0009\u0009INNER JOIN subscribed AS sd ON sd.did = f.nodeid AND sd.userid = 15965\n UNION  ALL \n\n\u0009\u0009\u0009\u0009\u0009### Users ###\n\u0009\u0009\u0009\u0009\u0009SELECT f.name AS title, f.userid AS keyval, 'user' AS sourcetable, IFNULL(f.lastpost, f.joindate) AS lastactivity,\n\u0009\u0009\u0009\u0009\u0009f.posts as activity, 'Member' AS type,\n\u0009\u0009\u0009\u0009\u00090 AS noUnsubscribe\n\u0009\u0009\u0009\u0009\u0009FROM user AS f\n\u0009\u0009\u0009\u0009\u0009INNER JOIN userlist AS ul ON ul.relationid = f.userid AND ul.userid = 15965\n\u0009\u0009\u0009\u0009\u0009WHERE ul.type = 'f' AND ul.aq = 'yes'\n ORDER BY title ASC LIMIT 100",
			User:   "root",
			Host:   "localhost",
			Db:     "db1",
			TimeMetrics: map[string]float32{
				"InnoDB_IO_r_wait":     0,
				"InnoDB_queue_wait":    0,
				"InnoDB_rec_lock_wait": 0,
				"Lock_time":            0.000161,
				"Query_time":           0.000628,
			},
			NumberMetrics: map[string]uint64{
				"Bytes_sent":            323,
				"InnoDB_IO_r_bytes":     0,
				"InnoDB_IO_r_ops":       0,
				"InnoDB_pages_distinct": 3,
				"InnoDB_trx_id":         0,
				"Killed":                0,
				"Last_errno":            0,
				"Merge_passes":          0,
				"Rows_affected":         0,
				"Rows_examined":         0,
				"Rows_read":             0,
				"Rows_sent":             0,
				"Thread_id":             103375137,
				"Tmp_disk_tables":       0,
				"Tmp_table_sizes":       0,
				"Tmp_tables":            1,
			},
			BoolMetrics: map[string]bool{
				"Filesort":          true,
				"Filesort_on_disk":  false,
				"Full_join":         false,
				"Full_scan":         true,
				"QC_Hit":            false,
				"Tmp_table":         true,
				"Tmp_table_on_disk": false,
			},
		},
		{
			Offset: 2105,
			Query:  "SELECT COUNT(userfing.keyval) AS total\n\u0009\u0009\u0009FROM\n\u0009\u0009\u0009((### All Content ###\n\u0009\u0009\u0009\u0009\u0009SELECT f.nodeid AS keyval\n\u0009\u0009\u0009\u0009\u0009FROM node AS f\n\u0009\u0009\u0009\u0009\u0009INNER JOIN subscribed AS sd ON sd.did = f.nodeid AND sd.userid = 15965) UNION ALL (\n\u0009\u0009\u0009\u0009\u0009### Users ###\n\u0009\u0009\u0009\u0009\u0009SELECT f.userid AS keyval\n\u0009\u0009\u0009\u0009\u0009FROM user AS f\n\u0009\u0009\u0009\u0009\u0009INNER JOIN userlist AS ul ON ul.relationid = f.userid AND ul.userid = 15965\n\u0009\u0009\u0009\u0009\u0009WHERE ul.type = 'f' AND ul.aq = 'yes')\n) AS userfing",
			User:   "root",
			Host:   "localhost",
			Db:     "db1",
			TimeMetrics: map[string]float32{
				"InnoDB_IO_r_wait":     0,
				"InnoDB_queue_wait":    0,
				"InnoDB_rec_lock_wait": 0,
				"Lock_time":            0.000116,
				"Query_time":           0.00042,
			},
			NumberMetrics: map[string]uint64{
				"Bytes_sent":            60,
				"InnoDB_IO_r_bytes":     0,
				"InnoDB_IO_r_ops":       0,
				"InnoDB_pages_distinct": 3,
				"InnoDB_trx_id":         0,
				"Killed":                0,
				"Last_errno":            0,
				"Merge_passes":          0,
				"Rows_affected":         0,
				"Rows_examined":         0,
				"Rows_read":             0,
				"Rows_sent":             1,
				"Thread_id":             103375137,
				"Tmp_disk_tables":       0,
				"Tmp_table_sizes":       0,
				"Tmp_tables":            2,
			},
			BoolMetrics: map[string]bool{
				"Filesort":          false,
				"Filesort_on_disk":  false,
				"Full_join":         false,
				"Full_scan":         true,
				"QC_Hit":            false,
				"Tmp_table":         true,
				"Tmp_table_on_disk": false,
			},
		},
		{
			Offset: 3164,
			Query:  "SELECT u.userid, u.name AS name, u.usergroupid AS usergroupid, IFNULL(u.lastactivity, u.joindate) as lastactivity,\n\u0009\u0009\u0009\u0009IFNULL((SELECT userid FROM userlist AS ul2 WHERE ul2.userid = 15965 AND ul2.relationid = u.userid AND ul2.type = 'f' AND ul2.aq = 'yes'), 0) as isFollowing,\n\u0009\u0009\u0009\u0009IFNULL((SELECT userid FROM userlist AS ul2 WHERE ul2.userid = 15965 AND ul2.relationid = u.userid AND ul2.type = 'f' AND ul2.aq = 'pending'), 0) as isPending\nFROM user AS u\n\u0009\u0009\u0009\u0009INNER JOIN userlist AS ul ON (u.userid = ul.userid AND ul.relationid = 15965)\n\n\u0009\u0009\u0009WHERE ul.type = 'f' AND ul.aq = 'yes'\nORDER BY name ASC\nLIMIT 0, 100",
			User:   "root",
			Host:   "localhost",
			Db:     "db1",
			TimeMetrics: map[string]float32{
				"InnoDB_IO_r_wait":     0,
				"InnoDB_queue_wait":    0,
				"InnoDB_rec_lock_wait": 0,
				"Lock_time":            0.000144,
				"Query_time":           0.000457,
			},
			NumberMetrics: map[string]uint64{
				"Bytes_sent":            359,
				"InnoDB_IO_r_bytes":     0,
				"InnoDB_IO_r_ops":       0,
				"InnoDB_pages_distinct": 1,
				"InnoDB_trx_id":         0,
				"Killed":                0,
				"Last_errno":            0,
				"Merge_passes":          0,
				"Rows_affected":         0,
				"Rows_examined":         0,
				"Rows_read":             0,
				"Rows_sent":             0,
				"Thread_id":             103375137,
				"Tmp_disk_tables":       0,
				"Tmp_table_sizes":       0,
				"Tmp_tables":            1,
			},
			BoolMetrics: map[string]bool{
				"Filesort":          true,
				"Filesort_on_disk":  false,
				"Full_join":         false,
				"Full_scan":         false,
				"QC_Hit":            false,
				"Tmp_table":         true,
				"Tmp_table_on_disk": false,
			},
		},
	}
	if same, diff := IsDeeply(got, &expect); !same {
		Dump(got)
		t.Error(diff)
	}
}
