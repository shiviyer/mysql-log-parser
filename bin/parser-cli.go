package main

import (
	"fmt"
	mysqlLog "github.com/vadimtk/mysql-log-parser/log"
	"github.com/vadimtk/mysql-log-parser/log/parser"
	"github.com/davecgh/go-spew/spew"
//	"github.com/davecheney/profile"
	l "log"
	"os"
	"time"
	"flag"
)

var logFile = flag.String("log", "", "log file to parse")

type Result struct {
        Global     *mysqlLog.GlobalClass
        Classes    []*mysqlLog.QueryClass
}


func ParseSlowLog(filename string, o parser.Options) (*Result, error) {
	file, err := os.Open(filename)
	if err != nil {
		l.Fatal(err)
	}
	stopChan := make(<-chan bool, 1)
	p := parser.NewSlowLogParser(file, stopChan, o)
	if err != nil {
		l.Fatal(err)
	}

        global := mysqlLog.NewGlobalClass()
        queries := make(map[string]*mysqlLog.QueryClass)
	result := &Result{}


//	var got []mysqlLog.Event
	go p.Run()
	for event := range p.EventChan {
		//got = append(got, *e)
		global.AddEvent(event)
		// Get the query class to which the event belongs.
	//        _, err := sqlparser.Parse(event.Query)
	//	if err != nil {
	//		fmt.Println("Query error: ", err, event.Query)
	//	}
                fingerprint := mysqlLog.Fingerprint(event.Query)
                classId := mysqlLog.Checksum(fingerprint)
                class, haveClass := queries[classId]
                if !haveClass {
                        class = mysqlLog.NewQueryClass(classId, fingerprint, true)
                        queries[classId] = class
                }

                // Add the event to its query class.
                class.AddEvent(event)
	}
        for _, class := range queries {
                class.Finalize()
        }
        global.Finalize(uint64(len(queries)))


        nQueries := len(queries)
        classes := make([]*mysqlLog.QueryClass, nQueries)
        for _, class := range queries {
                // Decr before use; can't classes[--nQueries] in Go.
                nQueries--
                classes[nQueries] = class
        }

        result.Global = global
        result.Classes = classes

	return result, nil
}

func main() {
//  defer profile.Start(profile.CPUProfile).Stop()
// re := pcre.MustCompile("(",0)
 flag.Parse()
 startT := time.Now()
 gotG, _ := ParseSlowLog(*logFile, parser.Options{Debug:false})
 sinceT := time.Since(startT)
 fmt.Printf("Events: %d, time: %f sec, rate: %f\n", gotG.Global.TotalQueries,sinceT.Seconds(),float64(gotG.Global.TotalQueries)/sinceT.Seconds())
 fmt.Printf("95pct query time : %f\n", gotG.Global.Metrics.TimeMetrics["Query_time"].GKq.Query(0.95))
 spew.Dump(gotG)
}
