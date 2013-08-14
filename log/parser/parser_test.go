package parser_test

import (
	"os"
	l "log"
	. "launchpad.net/gocheck"
	"testing"
	//"github.com/percona/percona-go-mysql/log"
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
	p := parser.NewSlowLogParser(file, true)
	go p.Run()
	for e := range p.EventChan {
		l.Println(e)
	}
}
