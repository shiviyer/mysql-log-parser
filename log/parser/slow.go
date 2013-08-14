package parser

import (
	"os"
	l "log"
	"fmt"
	"bufio"
	"strings"
	"github.com/percona/percona-go-mysql/log"
)

type SlowLogParser struct {
	file *os.File
	debug bool
	scanner *bufio.Scanner
	EventChan chan *log.Event
	inHeader bool
	inQuery bool
	queryLines uint
	event *log.Event
}

func NewSlowLogParser(file *os.File, debug bool) *SlowLogParser { 
	scanner := bufio.NewScanner(file)
	if debug {
		l.SetFlags(l.Ltime | l.Lmicroseconds)
	}
	p := &SlowLogParser{
		file: file,
		debug: debug,
		scanner: scanner,
		EventChan: make(chan *log.Event),
		inHeader: false,
		inQuery: false,
		queryLines: 0,
		event: new(log.Event),
	}
	return p
}

func (p *SlowLogParser) Run() {
	for p.scanner.Scan() {
		line := p.scanner.Text()
		if p.debug { // @debug
			fmt.Println()
			l.Println("line: " + line)
		}
		if p.inHeader {
			p.parseHeader(line)
		} else if p.inQuery {
			p.parseQuery(line)
		} else if strings.HasPrefix(line, "#") {
			p.inHeader = true
			p.inQuery = false
			p.parseHeader(line)
		}
	}
	if p.queryLines > 0 {
		p.sendEvent(false, false)
	}
	close(p.EventChan)
}

func (p *SlowLogParser) IsMetaLine(line string) bool {
	if strings.HasPrefix(line, "/") || strings.HasPrefix(line, "Time") || strings.HasPrefix(line, "Tcp") || strings.HasPrefix(line, "TCP") {
		if p.debug { // @debug
			l.Println("meta")
		}
		return true
	}
	return false

}

func (p *SlowLogParser) parseHeader(line string) {
	if !strings.HasPrefix(line, "#") {
		p.inHeader = false
		p.inQuery = true
		p.parseQuery(line)
		return
	}

	if strings.HasPrefix(line, "# Time") {
		if p.debug { // @debug
			l.Println("time")
		}
	} else if strings.HasPrefix(line, "# User") {
		if p.debug { // @debug
			l.Println("user")
		}
	} else if strings.HasPrefix(line, "# admin") {
		if p.debug { // @debug
			l.Println("admin command")
		}
	} else {
		if p.debug { // @debug
			l.Println("metrics")
		}
	}
}

func (p *SlowLogParser) parseQuery(line string) {
	if strings.HasPrefix(line, "#") || p.IsMetaLine(line) {


		p.inHeader = true
		p.inQuery = false
		p.sendEvent(true, false)
		p.parseHeader(line)
		return
	}

	if p.queryLines == 0 && strings.HasPrefix(line, "use ") {
		if p.debug { // @debug
			l.Println("use db")
		}
		db := strings.TrimPrefix(line, "use ")
		db = strings.TrimRight(db, ";")
		p.event.Db = db
	} else if p.queryLines == 0 && strings.HasPrefix(line, "SET ") {
		if p.debug { // @debug
			l.Println("set var")
		}
		// @todo ignore or use these lines?
	} else {
		if p.debug { // @debug
			l.Println("query")
		}
		if p.queryLines > 0 {
			p.event.Query += "\n" + line
		} else {
			p.event.Query = line
		}
		p.queryLines++
	}
}

func (p *SlowLogParser) sendEvent(inHeader bool, inQuery bool) {
	if p.debug { // @debug
		l.Println("send event")
	}
	p.EventChan <- p.event
	p.event = new(log.Event)
	p.queryLines = 0
	p.inHeader = inHeader
	p.inQuery = inQuery
}
