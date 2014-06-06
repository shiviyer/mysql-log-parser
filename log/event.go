package log

import (
	"crypto/md5"
	"fmt"
	"io"
	"regexp"
	"strings"
	"github.com/glenn-brown/golang-pkg-pcre/src/pkg/pcre"
)

var spaceRe *regexp.Regexp = regexp.MustCompile(`\s+`)
var nullRe *regexp.Regexp = regexp.MustCompile(`\bnull\b`)
var limitRe *regexp.Regexp = regexp.MustCompile(`\blimit \?(?:, ?\?| offset \?)?`)
var escapedQuoteRe *regexp.Regexp = regexp.MustCompile(`\\["']`)
//var doubleQuotedValRe *regexp.Regexp = regexp.MustCompile(`".*?"`)
var doubleQuotedValRe pcre.Regexp = pcre.MustCompile(`".*?"`,0)
var singleQuotedValRe *regexp.Regexp = regexp.MustCompile(`'.*?'`)
var number1Re *regexp.Regexp = regexp.MustCompile(`\b[0-9+-][0-9a-f.xb+-]*|[xb.+-]\?`)
var number2Re *regexp.Regexp = regexp.MustCompile(`[xb.+-]\?`)
var valueListRe *regexp.Regexp = regexp.MustCompile(`\b(in|values?)(?:[\s,]*\([\s?,]*\))+`)
var multiLineCommentRe *regexp.Regexp = regexp.MustCompile(`(?sm)/\*[^!].*?\*/`)
var orderByAscRe *regexp.Regexp = regexp.MustCompile(`(?i)order by (\S+) asc\b`)

// Go re doesn't support ?=, but I don't think slow logs can have -- comments,
// so we don't need this for now
//var oneLineCommentRe *regexp.Regexp = regexp.MustCompile(`(?:--|#)[^'"\r\n]*(?=[\r\n]|\z)`)
var oneLineHashCommentRe *regexp.Regexp = regexp.MustCompile(`#[^'"\r\n]*([\r\n]|\z)`)
var useDbRe *regexp.Regexp = regexp.MustCompile(`\Ause .+\z`)
var unionRe *regexp.Regexp = regexp.MustCompile(`\b(select\s.*?)(?:(\sunion(?:\sall)?)\s$1)+`)
var adminCmdRe *regexp.Regexp = regexp.MustCompile(`\Aadministrator command: `)
var storedProcRe *regexp.Regexp = regexp.MustCompile(`(?i)\A\s*(call\s+\S+)\(`)

type Event struct {
	Offset        uint64 // byte offset in log file, start of event
	Ts            string // if present in log file, often times not
	Admin         bool   // Query is admin command not SQL query
	Query         string // SQL query or admin command
	User          string
	Host          string
	Db            string
	RateType      string             // Percona Server rate limit type
	RateLimit     byte               // Percona Server rate limit
	TimeMetrics   map[string]float32 // *_time and *_wait metrics
	NumberMetrics map[string]uint64  // most metrics
	BoolMetrics   map[string]bool    // yes/no metrics
}

func NewEvent() *Event {
	event := new(Event)
	event.TimeMetrics = make(map[string]float32)
	event.NumberMetrics = make(map[string]uint64)
	event.BoolMetrics = make(map[string]bool)
	return event
}

func StripComments(q string) string {
	// @todo See comment above
	// q = oneLineCommentRe.ReplaceAllString(q, "")
	q = oneLineHashCommentRe.ReplaceAllString(q, "")
	q = multiLineCommentRe.ReplaceAllString(q, "")
	return q
}

func Fingerprint(q string) string {
	// First check for special case that shouldn't need any further processing.
	if useDbRe.MatchString(q) {
		return "use ?"
	} else if adminCmdRe.MatchString(q) {
		return q
	} else if storedProcRe.MatchString(q) {
		m := storedProcRe.FindStringSubmatch(q)
		return strings.ToLower(m[1])
	}

	// Strip the fluff.
	q = StripComments(q)
	q = strings.TrimSpace(q)

	// Do case-insensitive replacements
	q = spaceRe.ReplaceAllLiteralString(q, " ")
	q = escapedQuoteRe.ReplaceAllLiteralString(q, "")
	//q = doubleQuotedValRe.ReplaceAllLiteralString(q, "?")
	q = string(doubleQuotedValRe.ReplaceAll([]byte(q), []byte("?"),0))
	q = singleQuotedValRe.ReplaceAllLiteralString(q, "?")
	// @todo Are 2 passes really necessary?
	q = number1Re.ReplaceAllLiteralString(q, "?")
	//q = number2Re.ReplaceAllLiteralString(q, "?")

	// Lowercase the query then do case-sensitive replacements
	q = strings.ToLower(q)
	q = valueListRe.ReplaceAllString(q, "$1(?+)")       // in|value (...) -> in|value (?+)
	q = unionRe.ReplaceAllString(q, "$1 /*repeat$2*/")  // @todo
	q = nullRe.ReplaceAllString(q, "?")                 // null -> ?
	q = limitRe.ReplaceAllString(q, "limit ?")          // limit N -> limit ?
	q = orderByAscRe.ReplaceAllString(q, "order by $1") // order by col asc -> order by col

	return q
}

func Checksum(className string) string {
	id := md5.New()
	io.WriteString(id, className)
	h := fmt.Sprintf("%x", id.Sum(nil))
	return strings.ToUpper(h[16:32])
}
