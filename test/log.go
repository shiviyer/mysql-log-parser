package testlog

import (
	"os"
	"fmt"
	l "log"
	"reflect"
	"github.com/percona/percona-go-mysql/log"
	"github.com/percona/percona-go-mysql/log/parser"
	"launchpad.net/gocheck"
)

var sample = os.Getenv("GOPATH") + "/src/github.com/percona/percona-go-mysql/test/logs/"

func ParseSlowLog(filename string) *[]log.Event {
	file, err := os.Open(sample + filename)
	p := parser.NewSlowLogParser(file, false)
	if err != nil {
		l.Fatal(err)
	}
	var got []log.Event
	go p.Run()
	for e := range p.EventChan {
		got = append(got, *e)
	}
	return &got
}

/////////////////////////////////////////////////////////////////////////////
// EventsEqual gocheck.Checker
/////////////////////////////////////////////////////////////////////////////

/*
 * EventsEqual gocheck.Checker, like DeepEquals for []log.Event but returns
 * the difference, like:
 *   ... event 0 field User:
 *        got:
 *   expected: [SQL_SLAVE]
 * Only the first diff is returned, just because that's easier than finding
 * and returning all diffs.
 */

type eventsChecker struct {
	*gocheck.CheckerInfo
}

var EventsEqual gocheck.Checker = &eventsChecker{
	&gocheck.CheckerInfo{Name: "EventsEqual", Params: []string{"got", "expected"}},
}

func (checker *eventsChecker) Check(params []interface{}, names []string) (result bool, error string) {
	// Get the []log.Event{}
	gotEvents := reflect.ValueOf(params[0]).Elem()
	expectEvents := reflect.ValueOf(params[1]).Elem()

	// For each expected event...
	for i := 0; i < expectEvents.Len(); i++ {

		// Get the current expected and got log.Event.
		expectEvent := expectEvents.Index(i)
		gotEvent := gotEvents.Index(i)

		// For each field (key) in the expected log.Event...
		for j := 0; j < expectEvent.NumField(); j++ {

			// Get the value expected and got for the field.
			expectVal := expectEvent.Field(j)
			gotVal := gotEvent.Field(j)

			// Compare the expected and got values based on their type,
			// return immediate when a difference is found.
			switch expectVal.Type().Kind() {
			case reflect.String:
				if gotVal.String() != expectVal.String() {
					err := fmt.Sprintf("event %d field %s:\n     got: %s\nexpected: %s\n",
						i,  gotEvent.Type().Field(j).Name, gotVal.String(), expectVal.String())
					return false, err
				}
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				if gotVal.Int() != expectVal.Int() {
					err := fmt.Sprintf("event %d field %s:\n     got: %d\nexpected: %d\n",
						i,  gotEvent.Type().Field(j).Name, gotVal.Int(), expectVal.Int())
					return false, err
				}
			case reflect.Bool:
				if gotVal.Bool() != expectVal.Bool() {
					err := fmt.Sprintf("event %d field %s:\n     got: %t\nexpected: %t\n",
						i,  gotEvent.Type().Field(j).Name, gotVal.Bool(), expectVal.Bool())
					return false, err
				}
			case reflect.Map:
				if equal, err := checkEventMaps(gotVal, expectVal); !equal {
					return false, err
				}
			default:
				// If this happens, we need to add a new case ^ to handle the data type.
				err := fmt.Sprintf("EventsEqual cannot handle event %d field %s type %s",
					i, gotEvent.Type().Field(j).Name, expectVal.Type().Kind())
				return false, err
			}
		}
	}

	// No differences; all the events are identical (or there's a bug in this func).
	return true, ""
}

func checkEventMaps(gotMap reflect.Value, expectMap reflect.Value) (bool, string) {
	// @fixme get type of value, not type of key
	keyType := expectMap.Type().Key().Kind()
	fmt.Println(keyType)
	keys := expectMap.MapKeys()
	for _, key := range keys {
		switch keyType {
		case reflect.Float32, reflect.Float64:
			if gotMap.MapIndex(key).Float() != expectMap.MapIndex(key).Float() {
				err := fmt.Sprintf("key %s:    got: %f\nexpected: %f\n",
					key, gotMap.MapIndex(key).Float(), expectMap.MapIndex(key).Float())
				return false, err
			}
		case reflect.Bool:
			if gotMap.MapIndex(key).Bool() != expectMap.MapIndex(key).Bool() {
				err := fmt.Sprintf("key %s:    got: %t\nexpected: %t\n",
					key, gotMap.MapIndex(key).Bool(), expectMap.MapIndex(key).Bool())
				return false, err
			}
		default:
			if gotMap.MapIndex(key).Int() != expectMap.MapIndex(key).Int() {
				err := fmt.Sprintf("key %s:    got: %d\nexpected: %d\n",
					key, gotMap.MapIndex(key).Int(), expectMap.MapIndex(key).Int())
				return false, err
			}
		}
	}
	return true, ""
}
