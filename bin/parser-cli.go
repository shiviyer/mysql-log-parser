package main

import (
	"fmt"
	mysqlLog "github.com/vadimtk/mysql-log-parser/log"
	"github.com/vadimtk/mysql-log-parser/log/parser"
	//"github.com/davecgh/go-spew/spew"
//	"github.com/davecheney/profile"
	l "log"
	"os"
	"time"
	"flag"
	"runtime"
	"sync"
)

var logFile = flag.String("log", "", "log file to parse")

type WorkRes struct {
	Event *mysqlLog.Event 
	Fingerprint string
}

type Result struct {
        Global     *mysqlLog.GlobalClass
        Classes    []*mysqlLog.QueryClass
}

func Worker(id int, queue chan *mysqlLog.Event, req chan *WorkRes) {
    var wp *mysqlLog.Event
    for {
        // get work item (pointer) from the queue
        wp = <-queue
        if wp == nil {
            break
        }

        fingerprint := mysqlLog.Fingerprint(wp.Query)
 	req <- &WorkRes{wp, fingerprint}	
    }
}

func ParseSlowLog(filename string, o parser.Options) (*Result, error) {
	file, err := os.Open(filename)
	if err != nil {
		l.Fatal(err)
	}
	stopChan := make(<-chan bool, 1)

	queue := make(chan *mysqlLog.Event)
	res := make(chan *WorkRes)

	// spawn workers
	for i := 0; i < runtime.NumCPU(); i++ {
		go Worker(i, queue, res)
	}

	p := parser.NewSlowLogParser(file, stopChan, o)
	if err != nil {
		l.Fatal(err)
	}

        global := mysqlLog.NewGlobalClass()
        queries := make(map[string]*mysqlLog.QueryClass)
	result := &Result{}

	var wg sync.WaitGroup


	go p.Run()

	go func(){
		for {
			// get work item (pointer) from the queue
wp := <-res
	    classId := mysqlLog.Checksum(wp.Fingerprint)
	    class, haveClass := queries[classId]
	    if !haveClass {
		    class = mysqlLog.NewQueryClass(classId, wp.Fingerprint, true)
			    queries[classId] = class
	    }

    // Add the event to its query class.
    class.AddEvent(wp.Event)
	wg.Done()
		}

	}()

	for event := range p.EventChan {
		//got = append(got, *e)
		global.AddEvent(event)
		queue <- event
		wg.Add(1)
	}

	wg.Wait()
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
 runtime.GOMAXPROCS(runtime.NumCPU())

 startT := time.Now()
 gotG, _ := ParseSlowLog(*logFile, parser.Options{Debug:false})
 sinceT := time.Since(startT)
 fmt.Printf("Events: %d, time: %f sec, rate: %f\n", gotG.Global.TotalQueries,sinceT.Seconds(),float64(gotG.Global.TotalQueries)/sinceT.Seconds())
 //i:=0.05

 for i:=0.00;i<=1.04;i+=0.05 {
	val,rmin,rmax := gotG.Global.Metrics.TimeMetrics["Query_time"].GKq.QueryRank(i)
	 fmt.Printf("%f pct query time : %f, (%d-%d)\n", i, val, rmin, rmax)
 }

 fmt.Printf("Real 95pct %f, med: %f\n",  gotG.Global.Metrics.TimeMetrics["Query_time"].Pct95,gotG.Global.Metrics.TimeMetrics["Query_time"].Med )
 fmt.Printf("GK length: %d\n",  len(gotG.Global.Metrics.TimeMetrics["Query_time"].GKq.Items))
 gotG.Global.Metrics.TimeMetrics["Query_time"].GKq.Print()
 for _,v := range gotG.Classes {
  if v.TotalQueries > gotG.Global.TotalQueries / 10 {
 fmt.Printf("Query ID %s, Events: %d\n", v.Id, v.TotalQueries)

 for i:=0.00;i<=1.04;i+=0.05 {
	 val,rmin,rmax := v.Metrics.TimeMetrics["Query_time"].GKq.QueryRank(i)
	 fmt.Printf("%f pct query time : %f, (%d-%d)\n", i, val, rmin, rmax)
 }
 fmt.Printf("Real 95pct %f, med: %f\n",  v.Metrics.TimeMetrics["Query_time"].Pct95,v.Metrics.TimeMetrics["Query_time"].Med )
 fmt.Printf("GK length: %d\n",  len(v.Metrics.TimeMetrics["Query_time"].GKq.Items))
 }
// fmt.Printf("%.7f\n",v)
 }


// spew.Dump(gotG)
}
