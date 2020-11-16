package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/midbel/log"
)

var (
	input  = "[%t] [%h(%4:%p)]%b%u:%g:%n [%p:%l(INFO, WARNING)]:%b%m"
	output = "%t %n[%p]: %m"
)

func main() {
	var (
		in = flag.String("i", input, "input pattern")
		// out    = flag.String("o", output, "output pattern")
		filter = flag.String("f", "", "filter log entry")
	)
	flag.Parse()

	r, err := os.Open(flag.Arg(0))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	defer r.Close()

	rs, err := log.NewReader(r, *in, *filter)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for i := 1; ; i++ {
		e, err := rs.Read()
		if err != nil {
			break
		}
		fmt.Printf("%d: %+v\n", i, e)
	}
}
