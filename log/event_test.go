package log_test

import (
	"github.com/percona/percona-go-mysql/log"
	//	. "github.com/percona/percona-go-mysql/test"
	. "launchpad.net/gocheck"
	"testing"
)

// Hook gocheck into the "go test" runner.
// http://labix.org/gocheck
func Test(t *testing.T) { TestingT(t) }

/////////////////////////////////////////////////////////////////////////////
// Event.Class() tests
// //////////////////////////////////////////////////////////////////////////

type QueryClassTestSuite struct {
}

var _ = Suite(&QueryClassTestSuite{})

func (s *QueryClassTestSuite) TestQueryClass(t *C) {
	var q string
	var f string

	q = "SELECT c FROM t WHERE id=1"
	f = "select c from t where id=?"
	t.Check(
		log.QueryClass(q),
		Equals,
		f,
	)

	// Complex comments
	q = `UPDATE groups_search SET  charter = '   -------3\'\' XXXXXXXXX.\n    \n    -----------------------------------------------------', show_in_list = 'Y' WHERE group_id='aaaaaaaa'`
	t.Check(
		log.QueryClass(q),
		Equals,
		"update groups_search set charter = ?, show_in_list = ? where group_id=?",
	)

	// Fingerprints all mysqldump SELECTs together
	q = "SELECT /*!40001 SQL_NO_CACHE */ * FROM `film`"
	t.Check(
		log.QueryClass(q),
		Equals,
		"mysqldump",
	)

	// Fingerprints stored procedure calls specially
	q = "CALL foo(1, 2, 3)"
	t.Check(
		log.QueryClass(q),
		Equals,
		"call foo",
	)

	// Fingerprints admin commands as themselves
	q = "administrator command: Init DB"
	t.Check(
		log.QueryClass(q),
		Equals,
		"administrator command: Init DB",
	)

	// Fingerprints mk-table-checksum queries together
	q = "REPLACE /*foo.bar:3/3*/ INTO checksum.checksum (db, tbl, " +
	    "chunk, boundaries, this_cnt, this_crc) SELECT 'foo', 'bar', " +
	    "2 AS chunk_num, '`id` >= 2166633', COUNT(*) AS cnt, " +
	    "LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#', `id`, `created_by`, " +
	    "`created_date`, `updated_by`, `updated_date`, `ppc_provider`, " +
	    "`account_name`, `provider_account_id`, `campaign_name`, " +
	    "`provider_campaign_id`, `adgroup_name`, `provider_adgroup_id`, " +
	    "`provider_keyword_id`, `provider_ad_id`, `foo`, `reason`, " +
	    "`foo_bar_bazz_id`, `foo_bar_baz`, CONCAT(ISNULL(`created_by`), " +
	    "ISNULL(`created_date`), ISNULL(`updated_by`), ISNULL(`updated_date`), " +
	    "ISNULL(`ppc_provider`), ISNULL(`account_name`), " +
	    "ISNULL(`provider_account_id`), ISNULL(`campaign_name`), " +
	    "ISNULL(`provider_campaign_id`), ISNULL(`adgroup_name`), " +
	    "ISNULL(`provider_adgroup_id`), ISNULL(`provider_keyword_id`), " +
	    "ISNULL(`provider_ad_id`), ISNULL(`foo`), ISNULL(`reason`), " +
	    "ISNULL(`foo_base_foo_id`), ISNULL(`fooe_foo_id`)))) AS UNSIGNED)), 10, 16)) AS crc " +
	    "FROM `foo`.`bar` USE INDEX (`PRIMARY`) WHERE  `id` >= 2166633)"
	t.Check(
		log.QueryClass(q),
		Equals,
		"percona-toolkit",
	)

	// Removes identifier from USE
	q = "use `foo`"
	t.Check(
		log.QueryClass(q),
		Equals,
		"use ?",
	)

	// Removes one-line comments in fingerprints
	q = "select \n--bar\n foo"
	t.Check(
		log.QueryClass(q),
		Equals,
		"select foo",
	)

	// Removes one-line comments in fingerprint without mushing things together
	q = "select foo--bar\nfoo"
	t.Check(
		log.QueryClass(q),
		Equals,
		"select foo foo",
	)

	// Removes one-line EOL comments in fingerprints
	q = "select foo -- bar\n"
	t.Check(
		log.QueryClass(q),
		Equals,
		"select foo ",
	)

	// Handles bug from perlmonks thread 728718
	q = "select null, 5.001, 5001. from foo"
	t.Check(
		log.QueryClass(q),
		Equals,
		"select ?, ?, ? from foo",
	)

	// Handles quoted strings
	q = "select 'hello', '\nhello\n', \"hello\", '\\'' from foo"
	t.Check(
		log.QueryClass(q),
		Equals,
		"select ?, ?, ?, ? from foo",
	)

	// Handles trailing newline
	q = "select 'hello'\n"
	t.Check(
		log.QueryClass(q),
		Equals,
		"select ?",
	)

	// Does not handle all quoted strings
	// This is a known deficiency, fixes seem to be expensive though.
	q = "select '\\\\' from foo"
	t.Check(
		log.QueryClass(q),
		Equals,
		"select '\\ from foo",
	)

	// Collapses whitespace
	q = "select   foo"
	t.Check(
		log.QueryClass(q),
		Equals,
		"select foo",
	)

	// Lowercases, replaces integer
	q = "SELECT * from foo where a = 5"
	t.Check(
		log.QueryClass(q),
		Equals,
		"select * from foo where a = ?",
	)

	// Floats
	q = "select 0e0, +6e-30, -6.00 from foo where a = 5.5 or b=0.5 or c=.5"
	t.Check(
		log.QueryClass(q),
		Equals,
		"select ?, ?, ? from foo where a = ? or b=? or c=?",
	)

	// Hex/bit
	q = "select 0x0, x'123', 0b1010, b'10101' from foo"
	t.Check(
		log.QueryClass(q),
		Equals,
		"select ?, ?, ?, ? from foo",
	)

	// Collapses whitespace
	q = " select  * from\nfoo where a = 5"
	t.Check(
		log.QueryClass(q),
		Equals,
		"select * from foo where a = ?",
	)

	// IN lists
	q = "select * from foo where a in (5) and b in (5, 8,9 ,9 , 10)"
	t.Check(
		log.QueryClass(q),
		Equals,
		"select * from foo where a in(?+) and b in(?+)",
	)

	// Numeric table names
	q = "select foo_1 from foo_2_3"
	t.Check(
		log.QueryClass(q),
		Equals,
		"select foo_? from foo_?_?",
	)

	// Numeric table name prefixes
	// 123f00 => ?oo because f "looks like it could be a number".
	q = "select 123foo from 123foo"
	t.Check(
		log.QueryClass(q),
		Equals,
		"select ?oo from ?oo",
	)

	// Numeric table name prefixes with underscores
	q = "select 123_foo from 123_foo"
	t.Check(
		log.QueryClass(q),
		Equals,
		"select ?_foo from ?_foo",
	)

	// A string that needs no changes
	q = "insert into abtemp.coxed select foo.bar from foo"
	t.Check(
		log.QueryClass(q),
		Equals,
		"insert into abtemp.coxed select foo.bar from foo",
	)

	// VALUES lists
	q = "insert into foo(a, b, c) values(2, 4, 5)"
	t.Check(
		log.QueryClass(q),
		Equals,
		"insert into foo(a, b, c) values(?+)",
	)

	// VALUES lists with multiple ()
	q = "insert into foo(a, b, c) values(2, 4, 5) , (2,4,5)"
	t.Check(
		log.QueryClass(q),
		Equals,
		"insert into foo(a, b, c) values(?+)",
	)

	// VALUES lists with VALUE()
	q = "insert into foo(a, b, c) value(2, 4, 5)"
	t.Check(
		log.QueryClass(q),
		Equals,
		"insert into foo(a, b, c) value(?+)",
	)

	// limit alone
	q = "select * from foo limit 5"
	t.Check(
		log.QueryClass(q),
		Equals,
		"select * from foo limit ?",
	)

	// limit with comma-offset
	q = "select * from foo limit 5, 10"
	t.Check(
		log.QueryClass(q),
		Equals,
		"select * from foo limit ?",
	)

	// limit with offset
	q = "select * from foo limit 5 offset 10"
	t.Check(
		log.QueryClass(q),
		Equals,
		"select * from foo limit ?",
	)

	// union fingerprints together
	q = "select 1 union select 2 union select 4"
	t.Check(
		log.QueryClass(q),
		Equals,
		"select ? /*repeat union*/",
	)

	// union all fingerprints together
	q = "select 1 union all select 2 union all select 4"
	t.Check(
		log.QueryClass(q),
		Equals,
		"select ? /*repeat union all*/",
	)

	// union all fingerprints together
	q = `select * from (select 1 union all select 2 union all select 4) as x 
		  join (select 2 union select 2 union select 3) as y`
	t.Check(
		log.QueryClass(q),
		Equals,
		`select * from (select ? /*repeat union all*/) as x 
		  join (select ? /*repeat union*/) as y`,
	)

	/*
		# Issue 322: mk-query-digest segfault before report
		t.Check(
			log.QueryClass(q),
			load_file("t/lib/samples/huge_replace_into_values.txt") ),
		   Equals,
		   `replace into `film_actor` values(?+)`
		   "huge replace into values() (issue 322)",
		)
		t.Check(
			log.QueryClass(q),
			load_file("t/lib/samples/huge_insert_ignore_into_values.txt") ),
		   Equals,
		   `insert ignore into `film_actor` values(?+)`
		   "huge insert ignore into values() (issue 322)",
		)

		t.Check(
			log.QueryClass(q),
			load_file("t/lib/samples/huge_explicit_cols_values.txt") ),
		   Equals,
		   `insert into foo (a,b,c,d,e,f,g,h) values(?+)`
		   "huge insert with explicit columns before values() (issue 322)",
		)
	*/

	// Remove ASC from ORDER BY
	// Issue 1030: Fingerprint can remove ORDER BY ASC
	q = "select c from t where i=1 order by c asc"
	t.Check(
		log.QueryClass(q),
		Equals,
		"select c from t where i=? order by c",
	)

	// Remove only ASC from ORDER BY
	q = "select * from t where i=1 order by a, b ASC, d DESC, e asc"
	t.Check(
		log.QueryClass(q),
		Equals,
		"select * from t where i=? order by a, b, d desc, e",
	)

	// Remove ASC from spacey ORDER BY
	q = `select * from t where i=1      order            by 
		  a,  b          ASC, d    DESC,    
								 
								 e asc`
	t.Check(
		log.QueryClass(q),
		Equals,
		"select * from t where i=? order by a, b, d desc, e",
	)

	// Fingerprint LOAD DATA INFILE
	q = "LOAD DATA INFILE '/tmp/foo.txt' INTO db.tbl"
	t.Check(
		log.QueryClass(q),
		Equals,
		"load data infile ? into db.tbl",
	)

	// Fingerprint db.tbl<number>name (preserve number)
	q = "SELECT * FROM prices.rt_5min where id=1"
	t.Check(
		log.QueryClass(q),
		Equals,
		"select * from prices.rt_5min where id=?",
	)

	// Fingerprint /* -- comment */ SELECT (bug 1174956)
	q = "/* -- S++ SU ABORTABLE -- spd_user: rspadim */SELECT SQL_SMALL_RESULT SQL_CACHE DISTINCT centro_atividade FROM est_dia WHERE unidade_id=1001 AND item_id=67 AND item_id_red=573"
	t.Check(
		log.QueryClass(q),
		Equals,
		"select sql_small_result sql_cache distinct centro_atividade from est_dia where unidade_id=? and item_id=? and item_id_red=?",
	)
}
