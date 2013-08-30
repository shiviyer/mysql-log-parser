package parser_test

import (
	"os"
	l "log"
	. "launchpad.net/gocheck"
	"testing"
	"github.com/percona/percona-go-mysql/log"
	"github.com/percona/percona-go-mysql/log/parser"
)

// Hook gocheck into the "go test" runner.
// http://labix.org/gocheck
func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
}
var _ = Suite(&TestSuite{})

var sample = os.Getenv("GOPATH") + "/src/github.com/percona/percona-go-mysql/test/logs/"

// Start a mock ws server that sends all client msgs back to us via fromClients.
func (s *TestSuite) SetUpSuite(t *C) {
}

/////////////////////////////////////////////////////////////////////////////
// Test cases
// //////////////////////////////////////////////////////////////////////////

/*
 * Test slow log parser
 */

func (s *TestSuite) TestSlowLogParser(t *C) {
	file, err := os.Open(sample + "slow001.log")
	if err != nil {
		l.Fatal(err)
	}
	var got []log.Event
	p := parser.NewSlowLogParser(file, false)
	go p.Run()
	for e := range p.EventChan {
		got = append(got, *e)
	}
	expect := []log.Event{
		{
			Offset: 0,
			Ts: "071015 21:43:52",
			Admin: false,
			Query: `select sleep(2) from n`,
			User: "root",
			Host: "localhost",
			Db: "test",
			TimeMetrics: map[string]float32{
				"Query_time": 2,
				"Lock_time": 0,
			},
			NumberMetrics: map[string]uint64{
				"Rows_sent": 1,
				"Rows_examined": 0,
			},
			BoolMetrics: map[string]bool{},
		},
		{
			Offset: 0,
			Ts: "071015 21:45:10",
			Admin: false,
			Query: `select sleep(2) from test.n`,
			User: "root",
			Host: "localhost",
			Db: "sakila",
			TimeMetrics: map[string]float32{
				"Query_time": 2,
				"Lock_time": 0,
			},
			NumberMetrics: map[string]uint64{
				"Rows_sent": 1,
				"Rows_examined": 0,
			},
			BoolMetrics: map[string]bool{},
		},
	}
	t.Check(got, DeepEquals, expect)
}
