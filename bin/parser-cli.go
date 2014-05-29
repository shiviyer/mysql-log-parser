package main

import (
	"fmt"
	"github.com/percona/mysql-log-parser/log"
	"github.com/percona/mysql-log-parser/log/parser"
	l "log"
	"os"
	"time"
)

func ParseSlowLog(filename string, o parser.Options) *[]log.Event {
	file, err := os.Open(filename)
	if err != nil {
		l.Fatal(err)
	}
	stopChan := make(<-chan bool, 1)
	p := parser.NewSlowLogParser(file, stopChan, o)
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

func main() {
 startT := time.Now()
 got := ParseSlowLog("mysql-slow.log-1401339840", parser.Options{Debug:false})
 sinceT := time.Since(startT)
 fmt.Printf("Events: %d, time: %f sec, rate: %f\n",len(*got),sinceT.Seconds(),float64(len(*got))/sinceT.Seconds())
}
