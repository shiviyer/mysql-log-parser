package log_test

import (
	"github.com/percona/percona-go-mysql/log"
	"github.com/percona/percona-go-mysql/log/parser"
	"github.com/percona/percona-go-mysql/test"
	. "github.com/percona/percona-go-mysql/test"
	. "launchpad.net/gocheck"
	"testing"
)

// Hook gocheck into the "go test" runner.
// http://labix.org/gocheck
func Test(t *testing.T) { TestingT(t) }

/////////////////////////////////////////////////////////////////////////////
// Fingerprint() test suite
// //////////////////////////////////////////////////////////////////////////

type FingerprintTestSuite struct {
}

var _ = Suite(&FingerprintTestSuite{})

func (s *FingerprintTestSuite) TestFingerprintBasic(t *C) {
	var q string

	// A most basic case
	q = "SELECT c FROM t WHERE id=1"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"select c from t where id=?",
	)

	// The values looks like one line -- comments, but they're not.
	q = `UPDATE groups_search SET  charter = '   -------3\'\' XXXXXXXXX.\n    \n    -----------------------------------------------------', show_in_list = 'Y' WHERE group_id='aaaaaaaa'`
	t.Check(
		log.Fingerprint(q),
		Equals,
		"update groups_search set charter = ?, show_in_list = ? where group_id=?",
	)

	// PT treats this as "mysqldump", but we don't do any special fingerprints.
	q = "SELECT /*!40001 SQL_NO_CACHE */ * FROM `film`"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"select /*!? sql_no_cache */ * from `film`",
	)

	// Fingerprints stored procedure calls specially
	q = "CALL foo(1, 2, 3)"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"call foo",
	)

	// Fingerprints admin commands as themselves
	q = "administrator command: Init DB"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"administrator command: Init DB",
	)

	// Removes identifier from USE
	q = "use `foo`"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"use ?",
	)

	// Handles bug from perlmonks thread 728718
	q = "select null, 5.001, 5001. from foo"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"select ?, ?, ? from foo",
	)

	// Handles quoted strings
	q = "select 'hello', '\nhello\n', \"hello\", '\\'' from foo"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"select ?, ?, ?, ? from foo",
	)

	// Handles trailing newline
	q = "select 'hello'\n"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"select ?",
	)

	// Does not handle all quoted strings
	// This is a known deficiency, fixes seem to be expensive though.
	q = "select '\\\\' from foo"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"select '\\ from foo",
	)

	// Collapses whitespace
	q = "select   foo"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"select foo",
	)

	// Lowercases, replaces integer
	q = "SELECT * from foo where a = 5"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"select * from foo where a = ?",
	)

	// Floats
	q = "select 0e0, +6e-30, -6.00 from foo where a = 5.5 or b=0.5 or c=.5"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"select ?, ?, ? from foo where a = ? or b=? or c=?",
	)

	// Hex/bit
	q = "select 0x0, x'123', 0b1010, b'10101' from foo"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"select ?, ?, ?, ? from foo",
	)

	// Collapses whitespace
	q = " select  * from\nfoo where a = 5"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"select * from foo where a = ?",
	)

	// IN lists
	q = "select * from foo where a in (5) and b in (5, 8,9 ,9 , 10)"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"select * from foo where a in(?+) and b in(?+)",
	)

	// Numeric table names.  By default, PT will return foo_?, etc. because
	// match_embedded_numbers is false by default for speed.
	q = "select foo_1 from foo_2_3"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"select foo_1 from foo_2_3",
	)

	// Numeric table name prefixes
	// 123f00 => ?oo because f "looks like it could be a number".
	q = "select 123foo from 123foo"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"select ?oo from ?oo",
	)

	// Numeric table name prefixes with underscores
	q = "select 123_foo from 123_foo"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"select ?_foo from ?_foo",
	)

	// A string that needs no changes
	q = "insert into abtemp.coxed select foo.bar from foo"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"insert into abtemp.coxed select foo.bar from foo",
	)

	// limit alone
	q = "select * from foo limit 5"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"select * from foo limit ?",
	)

	// limit with comma-offset
	q = "select * from foo limit 5, 10"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"select * from foo limit ?",
	)

	// limit with offset
	q = "select * from foo limit 5 offset 10"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"select * from foo limit ?",
	)

	// Fingerprint LOAD DATA INFILE
	q = "LOAD DATA INFILE '/tmp/foo.txt' INTO db.tbl"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"load data infile ? into db.tbl",
	)

	// Fingerprint db.tbl<number>name (preserve number)
	q = "SELECT * FROM prices.rt_5min where id=1"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"select * from prices.rt_5min where id=?",
	)

	// Fingerprint /* -- comment */ SELECT (bug 1174956)
	q = "/* -- S++ SU ABORTABLE -- spd_user: rspadim */SELECT SQL_SMALL_RESULT SQL_CACHE DISTINCT centro_atividade FROM est_dia WHERE unidade_id=1001 AND item_id=67 AND item_id_red=573"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"select sql_small_result sql_cache distinct centro_atividade from est_dia where unidade_id=? and item_id=? and item_id_red=?",
	)
}

func (s *FingerprintTestSuite) TestFingerprintValueList(t *C) {
	var q string

	// VALUES lists
	q = "insert into foo(a, b, c) values(2, 4, 5)"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"insert into foo(a, b, c) values(?+)",
	)

	// VALUES lists with multiple ()
	q = "insert into foo(a, b, c) values(2, 4, 5) , (2,4,5)"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"insert into foo(a, b, c) values(?+)",
	)

	// VALUES lists with VALUE()
	q = "insert into foo(a, b, c) value(2, 4, 5)"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"insert into foo(a, b, c) value(?+)",
	)
}

/////////////////////////////////////////////////////////////////////////////
// Skipped test cases for various reasons, mostly becuase Go re is very
// limited compared to Perl re.
/////////////////////////////////////////////////////////////////////////////

func (s *FingerprintTestSuite) TestFingerprintOrderBy(t *C) {
	t.Skip("Fingerprint ORDER BY doesn't work yet")

	var q string

	// Remove ASC from ORDER BY
	// Issue 1030: Fingerprint can remove ORDER BY ASC
	q = "select c from t where i=1 order by c asc"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"select c from t where i=? order by c",
	)

	// Remove only ASC from ORDER BY
	q = "select * from t where i=1 order by a, b ASC, d DESC, e asc"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"select * from t where i=? order by a, b, d desc, e",
	)

	// Remove ASC from spacey ORDER BY
	q = `select * from t where i=1      order            by 
		  a,  b          ASC, d    DESC,    
								 
								 e asc`
	t.Check(
		log.Fingerprint(q),
		Equals,
		"select * from t where i=? order by a, b, d desc, e",
	)
}

func (s *FingerprintTestSuite) TestFingerprintUnion(t *C) {
	t.Skip("Fingerprint UNION doesn't work yet")

	var q string

	// union fingerprints together
	q = "select 1 union select 2 union select 4"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"select ? /*repeat union*/",
	)

	// union all fingerprints together
	q = "select 1 union all select 2 union all select 4"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"select ? /*repeat union all*/",
	)

	// union all fingerprints together
	q = `select * from (select 1 union all select 2 union all select 4) as x 
		  join (select 2 union select 2 union select 3) as y`
	t.Check(
		log.Fingerprint(q),
		Equals,
		`select * from (select ? /*repeat union all*/) as x 
		  join (select ? /*repeat union*/) as y`,
	)
}

func (s *FingerprintTestSuite) TestFingerprintHugeQueries(t *C) {
	t.Skip("Not testing huge queries yet")
	/*
		var q string
		var f string

		// Issue 322: mk-query-digest segfault before report
		t.Check(
			log.Fingerprint(q),
			load_file("t/lib/samples/huge_replace_into_values.txt") ),
		   Equals,
		   `replace into `film_actor` values(?+)`
		   "huge replace into values() (issue 322)",
		)

		t.Check(
			log.Fingerprint(q),
			load_file("t/lib/samples/huge_insert_ignore_into_values.txt") ),
		   Equals,
		   `insert ignore into `film_actor` values(?+)`
		   "huge insert ignore into values() (issue 322)",
		)

		t.Check(
			log.Fingerprint(q),
			load_file("t/lib/samples/huge_explicit_cols_values.txt") ),
		   Equals,
		   `insert into foo (a,b,c,d,e,f,g,h) values(?+)`
		   "huge insert with explicit columns before values() (issue 322)",
		)
	*/
}

func (s *FingerprintTestSuite) TestFingerprintOneLineComments(t *C) {
	t.Skip("Stripping one-line comments doesn't work yet")

	var q string

	// Removes one-line comments in fingerprints
	q = "select \n--bar\n foo"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"select foo",
	)

	// Removes one-line comments in fingerprint without mushing things together
	q = "select foo--bar\nfoo"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"select foo foo",
	)

	// Removes one-line EOL comments in fingerprints
	q = "select foo -- bar\n"
	t.Check(
		log.Fingerprint(q),
		Equals,
		"select foo ",
	)
}

/////////////////////////////////////////////////////////////////////////////
// Checksum() test suite
// //////////////////////////////////////////////////////////////////////////

type ChecksumTestSuite struct {
}

var _ = Suite(&ChecksumTestSuite{})

func (s *ChecksumTestSuite) TestChecksum(t *C) {
	var f string

	// A most basic case
	f = "hello world"
	t.Check(
		log.Checksum(f),
		Equals,
		"93CB22BB8F5ACDC3",
	)
}

/////////////////////////////////////////////////////////////////////////////
// Stats test suite
// //////////////////////////////////////////////////////////////////////////

type EventStatsTestSuite struct {
}

var _ = Suite(&EventStatsTestSuite{})

func (s *EventStatsTestSuite) TestSlow001(t *C) {
	stats := log.NewEventStats()
	events := testlog.ParseSlowLog("slow001.log", parser.Options{})
	for _, e := range *events {
		stats.Add(&e)
	}
	stats.Current()
	expect := &log.EventStats{
		TimeMetrics: map[string]*log.TimeStats{
			"Lock_time": &log.TimeStats{
				Cnt:   2,
				Sum:   0,
				Min:   0,
				Avg:   0,
				Med:   0,
				Pct95: 0,
				Max:   0,
			},
			"Query_time": &log.TimeStats{
				Cnt:   2,
				Sum:   4,
				Min:   2,
				Avg:   2,
				Med:   2,
				Pct95: 2,
				Max:   2,
			},
		},
		NumberMetrics: map[string]*log.NumberStats{
			"Rows_examined": &log.NumberStats{
				Cnt:   2,
				Sum:   0,
				Min:   0,
				Avg:   0,
				Med:   0,
				Pct95: 0,
				Max:   0,
			},
			"Rows_sent": &log.NumberStats{
				Cnt:   2,
				Sum:   2,
				Min:   1,
				Avg:   1,
				Med:   1,
				Pct95: 1,
				Max:   1,
			},
		},
	}
	if same, diff := IsDeeply(stats, expect); !same {
		Dump(stats)
		t.Error(diff)
	}
}

func (s *EventStatsTestSuite) TestSlow010(t *C) {
	stats := log.NewEventStats()
	events := testlog.ParseSlowLog("slow010.log", parser.Options{})
	for _, e := range *events {
		stats.Add(&e)
	}
	stats.Current()
	expect := &log.EventStats{
		TimeMetrics: map[string]*log.TimeStats{
			"Query_time": &log.TimeStats{
				Cnt:   36,
				Sum:   22.703689,
				Min:   0.000002,
				Avg:   0.630658,
				Med:   0.192812, // pqd: 0.198537
				Pct95: 2.034012, // pqd: 1.964363
				Max:   3.034012,
			},
			"Lock_time": &log.TimeStats{
				Cnt:   36,
				Sum:   0,
				Min:   0,
				Avg:   0,
				Med:   0,
				Pct95: 0,
				Max:   0,
			},
		},
		NumberMetrics: map[string]*log.NumberStats{
			"Rows_sent": &log.NumberStats{
				Cnt:   36,
				Sum:   156,
				Min:   0,
				Avg:   4,
				Med:   1, // pqd: 0
				Pct95: 6, // pqd: 4
				Max:   99,
			},
		},
	}
	if same, diff := IsDeeply(stats, expect); !same {
		Dump(stats)
		t.Error(diff)
	}
}
