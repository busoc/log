package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"

	"github.com/midbel/log"
	"github.com/midbel/toml"
)

const (
  qFilter = "filter"
  qLimit  = "limit"
)

type Log struct {
	File    string
	URL     string
	Pattern string `toml:"format"`
	Line    int64
}

func (g Log) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var (
		query = r.URL.Query()
		limit = retrLimit(query.Get(qLimit))
	)
	es, err := g.readEntries(limit, query.Get(qFilter))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if len(es) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	json.NewEncoder(w).Encode(es)
}

func (g Log) readEntries(limit int, filter string) ([]log.Entry, error) {
	r, err := os.Open(g.File)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	rs, err := log.NewReader(r, g.Pattern, "")
	if err != nil {
		return nil, err
	}
	es, err := rs.ReadAll()
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if limit > 0 && limit < len(es) {
		es = es[len(es)-limit:]
	}
	return es, nil
}

func main() {
	flag.Parse()
	config := struct {
		Addr string
		Logs []Log `toml:"log"`
	}{}
	if err := toml.DecodeFile(flag.Arg(0), &config); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	for _, g := range config.Logs {
		if i, err := os.Stat(g.File); err != nil || i.IsDir() {
			fmt.Fprintf(os.Stderr, "%s: file does not exist! (%v)\n", g.File, err)
			os.Exit(1)
		}
		http.Handle(g.URL, wrapHandler(g))
	}

	if err := http.ListenAndServe(config.Addr, nil); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

func wrapHandler(next http.Handler) http.Handler {
	return allowMethod(next)
}

func allowMethod(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		next(w, r)
	}
	return http.HandlerFunc(fn)
}

func retrLimit(str string) int {
	i, _ := strconv.Atoi(str)
	return i
}
