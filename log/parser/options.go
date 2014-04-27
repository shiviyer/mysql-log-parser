package parser

type Options struct {
	StartOffset        uint64
	ExampleQueries     bool
	FilterAdminCommand map[string]bool
	Debug              bool
}
