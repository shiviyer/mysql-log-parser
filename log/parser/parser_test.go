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
		FilterAdminCommands: map[string]bool{
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
