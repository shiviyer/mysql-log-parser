package parser

import (
	"os"
	l "log"
	"fmt"
	"time"
	"bufio"
	"regexp"
	"strconv"
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
	timeRe *regexp.Regexp
	userRe *regexp.Regexp
	metricsRe *regexp.Regexp
	adminRe *regexp.Regexp
	setRe *regexp.Regexp
}

func NewSlowLogParser(file *os.File, debug bool) *SlowLogParser { 
	scanner := bufio.NewScanner(file)
	if debug { // @debug
		l.SetFlags(l.Ltime | l.Lmicroseconds)
		fmt.Println()
		l.Println("parsing " + file.Name())
	}
	p := &SlowLogParser{
		file: file,
		debug: debug,
		scanner: scanner,
		EventChan: make(chan *log.Event),
		inHeader: false,
		inQuery: false,
		queryLines: 0,
		event: log.NewEvent(),
		timeRe: regexp.MustCompile(`Time: (\S+\s{1,2}\S+)`),
		userRe: regexp.MustCompile(`User@Host: ([^\[]+|\[[^[]+\]).*?@ (\S*) \[(.*)\]`),
		metricsRe: regexp.MustCompile(`(\w+): (\S+|\z)`),
		adminRe: regexp.MustCompile(`command: (.+)`),
		setRe: regexp.MustCompile(`SET (?:last_insert_id|insert_id|timestamp)`),
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

func ConvertSlowLogTs(ts string) *time.Time {
	t, err := time.Parse("060102 15:04:05", ts)
	if err != nil {
		return nil
	}
	return &t
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
		m := p.timeRe.FindStringSubmatch(line)
		p.event.Ts = m[1]
		if p.userRe.MatchString(line) {
			if p.debug { // @debug
				l.Println("user (bad format)")
			}
			m := p.userRe.FindStringSubmatch(line)
			p.event.User = m[1]
			p.event.Host = m[2]
		}
	} else if strings.HasPrefix(line, "# User") {
		if p.debug { // @debug
			l.Println("user")
		}
		m := p.userRe.FindStringSubmatch(line)
		p.event.User = m[1]
		p.event.Host = m[2]
	} else if strings.HasPrefix(line, "# admin") {
		if p.debug { // @debug
			l.Println("admin command")
		}
		p.event.Admin = true
		m := p.adminRe.FindStringSubmatch(line)
		p.event.Query= m[1]

		// admin commands should be the last line of the event.
		p.sendEvent(false, false)
	} else {
		if p.debug { // @debug
			l.Println("metrics")
		}
		m := p.metricsRe.FindAllStringSubmatch(line, -1)
		for _, smv := range m {
			// [String, Metric, Value], e.g. ["Query_time: 2" "Query_time" "2"] 
			if strings.HasSuffix(smv[1], "_time") || strings.HasSuffix(smv[1], "_wait") {
				// microsecond value
				val, _ := strconv.ParseFloat(smv[2], 32)
				p.event.TimeMetrics[smv[1]] = float32(val)
			} else if smv[2] == "Yes" || smv[2] == "No" {
				// boolean value
				if smv[2] == "Yes" {
					p.event.BoolMetrics[smv[1]] = true
				} else {
					p.event.BoolMetrics[smv[1]] = false
				}
			} else if smv[1] == "Schema" {
				p.event.Db = smv[2]
			} else {
				// integer value
				val, _ := strconv.ParseUint(smv[2], 10, 64)
				p.event.NumberMetrics[smv[1]] = val
			}
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
	} else if p.setRe.MatchString(line) {
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

	// Clean up the event.
	p.event.Query = strings.TrimSuffix(p.event.Query, ";")

	// Send the event.  This will block.
	p.EventChan <- p.event

	// Make a new event and reset our metadata.
	p.event = log.NewEvent()
	p.queryLines = 0
	p.inHeader = inHeader
	p.inQuery = inQuery
}
