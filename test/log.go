package testlog

import (
	"fmt"
	"github.com/percona/percona-go-mysql/log"
	"github.com/percona/percona-go-mysql/log/parser"
	"launchpad.net/gocheck"
	l "log"
	"os"
	"os/exec"
	"reflect"
)

var Sample = os.Getenv("GOPATH") + "/src/github.com/percona/percona-go-mysql/test/logs/"

func ParseSlowLog(filename string, o parser.Options) *[]log.Event {
	file, err := os.Open(Sample + filename)
	p := parser.NewSlowLogParser(file, o)
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
						i, gotEvent.Type().Field(j).Name, gotVal.String(), expectVal.String())
					return false, err
				}
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				if gotVal.Int() != expectVal.Int() {
					err := fmt.Sprintf("event %d field %s:\n     got: %d\nexpected: %d\n",
						i, gotEvent.Type().Field(j).Name, gotVal.Int(), expectVal.Int())
					return false, err
				}
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				if gotVal.Uint() != expectVal.Uint() {
					err := fmt.Sprintf("event %d field %s:\n     got: %d\nexpected: %d\n",
						i, gotEvent.Type().Field(j).Name, gotVal.Uint(), expectVal.Uint())
					return false, err
				}
			case reflect.Bool:
				if gotVal.Bool() != expectVal.Bool() {
					err := fmt.Sprintf("event %d field %s:\n     got: %t\nexpected: %t\n",
						i, gotEvent.Type().Field(j).Name, gotVal.Bool(), expectVal.Bool())
					return false, err
				}
			case reflect.Map:
				if equal, mapErr := checkEventMaps(gotVal, expectVal); !equal {
					err := fmt.Sprintf("event %d field %s %s",
						i, gotEvent.Type().Field(j).Name, mapErr)
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
	// Get all keys in the expected map.  If there aren't any, check that
	// there also aren't any in the got map.
	keys := expectMap.MapKeys()
	if len(keys) == 0 {
		gotKeys := gotMap.MapKeys()
		if len(gotKeys) != 0 {
			err := fmt.Sprintf("     got: %s values\nexpected: no %s values\n", gotKeys[0], gotKeys[0])
			return false, err
		}
		return true, "" // no keys or values in either map
	}

	// Get the type of values in this map.
	valueType := expectMap.MapIndex(keys[0]).Type().Kind()

	// For key in the map, compare the got and expected values...
	for _, key := range keys {
		expectValue := expectMap.MapIndex(key)
		gotValue := gotMap.MapIndex(key)

		/*
		 * Check gotValue.IsValid() first: this returns true if the value is defined.
		 * We know the expect value is defined because it's in the map we're iterating,
		 * but the got value may not be defined (i.e. is not "valid"--IsValid() is
		 * poorly named; IsDefined() would be better imho).  This avoids a panic like
		 * "called gotValue.Float() on zero Value".
		 */

		switch valueType {
		case reflect.Float32, reflect.Float64:
			if gotValue.IsValid() {
				if gotValue.Float() != expectValue.Float() {
					err := fmt.Sprintf("key %s:\n     got: %f\nexpected: %f\n",
						key, gotValue.Float(), expectValue.Float())
					return false, err
				}
			} else {
				err := fmt.Sprintf("key %s:\n     got: undef\nexpected: %f\n",
					key, expectValue.Float())
				return false, err
			}
		case reflect.Bool:
			if gotValue.IsValid() {
				if gotValue.Bool() != expectValue.Bool() {
					err := fmt.Sprintf("key %s:\n     got: %t\nexpected: %t\n",
						key, gotValue.Bool(), expectValue.Bool())
					return false, err
				}
			} else {
				err := fmt.Sprintf("key %s:\n     got: undef\nexpected: %t\n",
					key, expectValue.Bool())
				return false, err
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if gotValue.IsValid() {
				if gotValue.Int() != expectValue.Int() {
					err := fmt.Sprintf("key %s:\n     got: %d\nexpected: %d\n",
						key, gotValue.Int(), expectValue.Int())
					return false, err
				}
			} else {
				err := fmt.Sprintf("key %s:\n     got: undef\nexpected: %d\n",
					key, expectValue.Int())
				return false, err
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if gotValue.IsValid() {
				if gotValue.Uint() != expectValue.Uint() {
					err := fmt.Sprintf("key %s:\n     got: %d\nexpected: %d\n",
						key, gotValue.Uint(), expectValue.Uint())
					return false, err
				}
			} else {
				err := fmt.Sprintf("key %s:\n     got: undef\nexpected: %d\n",
					key, expectValue.Uint())
				return false, err
			}
		default:
			// If this happens, we need to add a new case ^ to handle the data type.
			err := fmt.Sprintf("checkEventMaps cannot handle type %s", valueType)
			return false, err
		}
	}

	// No differenes; the maps are identical (or there's a bug in this func).
	return true, ""
}

/////////////////////////////////////////////////////////////////////////////
// StatsEqual
/////////////////////////////////////////////////////////////////////////////

type statsChecker struct {
	*gocheck.CheckerInfo
}

var StatsEqual gocheck.Checker = &statsChecker{
	&gocheck.CheckerInfo{Name: "StatsEqual", Params: []string{"got", "expected"}},
}

func (checker *statsChecker) Check(params []interface{}, names []string) (result bool, error string) {
	// Dereference *log.EventStats params
	got := reflect.ValueOf(params[0]).Elem()
	expect := reflect.ValueOf(params[1]).Elem()

	// For TimeMetrics, NumberMetrics, and BoolMetrics...
	for i := 0; i < expect.NumField(); i++ {
		gotMetrics := got.Field(i)
		expectMetrics := expect.Field(i)

		keys := expectMetrics.MapKeys()
		if len(keys) == 0 {
			gotKeys := gotMetrics.MapKeys()
			if len(gotKeys) != 0 {
				err := fmt.Sprintf("     got: %s values\nexpected: no %s values\n", gotKeys[0], gotKeys[0])
				return false, err
			}
			continue // no keys or values in either map
		}

		// Foreach metric in the XMetrics[metric]*TimeStats|*NumberStats|*BoolStats map...
		for _, metric := range keys {
			gotStat := gotMetrics.MapIndex(metric).Elem()
			expectStat := expectMetrics.MapIndex(metric).Elem()
			if equal, mapErr := checkStructs(gotStat, expectStat); !equal {
				err := fmt.Sprintf("%s.%s.%s",
					expect.Type().Field(i).Name, metric, mapErr)
				return false, err
			}
		}
	}

	// No differences; all the events are identical (or there's a bug in this func).
	return true, ""
}

func checkStructs(gotStruct reflect.Value, expectStruct reflect.Value) (bool, string) {
	// For each field (key) in the expected struct...
	for i := 0; i < expectStruct.NumField(); i++ {

		// Get the value expected and got for the field.
		expectVal := expectStruct.Field(i)
		gotVal := gotStruct.Field(i)

		// Compare the expected and got values based on their type,
		// return immediate when a difference is found.
		switch expectVal.Type().Kind() {
		case reflect.String:
			if gotVal.String() != expectVal.String() {
				err := fmt.Sprintf("%s:\n     got: %s\nexpected: %s\n",
					gotStruct.Type().Field(i).Name, gotVal.String(), expectVal.String())
				return false, err
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if gotVal.Int() != expectVal.Int() {
				err := fmt.Sprintf("%s:\n     got: %d\nexpected: %d\n",
					gotStruct.Type().Field(i).Name, gotVal.Int(), expectVal.Int())
				return false, err
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if gotVal.Uint() != expectVal.Uint() {
				err := fmt.Sprintf("%s:\n     got: %d\nexpected: %d\n",
					gotStruct.Type().Field(i).Name, gotVal.Uint(), expectVal.Uint())
				return false, err
			}
		case reflect.Float32, reflect.Float64:
			if gotVal.Float() != expectVal.Float() {
				err := fmt.Sprintf("%s:\n     got: %f\nexpected: %f\n",
					gotStruct.Type().Field(i).Name, gotVal.Float(), expectVal.Float())
				return false, err
			}
		case reflect.Bool:
			if gotVal.Bool() != expectVal.Bool() {
				err := fmt.Sprintf("%s:\n     got: %t\nexpected: %t\n",
					gotStruct.Type().Field(i).Name, gotVal.Bool(), expectVal.Bool())
				return false, err
			}
		case reflect.Map:
			if equal, mapErr := checkEventMaps(gotVal, expectVal); !equal {
				err := fmt.Sprintf("%s %s",
					gotStruct.Type().Field(i).Name, mapErr)
				return false, err
			}
		default:
			// If this happens, we need to add a new case ^ to handle the data type.
			err := fmt.Sprintf("checkStructs cannot handle field %s type %s",
				gotStruct.Type().Field(i).Name, expectVal.Type().Kind())
			return false, err
		}
	}

	return true, ""
}

/////////////////////////////////////////////////////////////////////////////
// FileEqual
/////////////////////////////////////////////////////////////////////////////

type fileChecker struct {
	*gocheck.CheckerInfo
}

var FileEquals gocheck.Checker = &fileChecker{
	&gocheck.CheckerInfo{Name: "FileEquals", Params: []string{"got", "expected"}},
}

func (checker *fileChecker) Check(params []interface{}, names []string) (result bool, error string) {
	gotFile := reflect.ValueOf(params[0])
	expectFile := reflect.ValueOf(params[1])
	cmd := exec.Command("diff", "-u", gotFile.String(), expectFile.String())
	diff, err := cmd.CombinedOutput()
	if err != nil {
		return false, string(diff)
	}
	return true, ""
}
