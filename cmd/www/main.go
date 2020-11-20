package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/midbel/log"
	"github.com/midbel/toml"
)

const (
	qFilter = "filter"
	qLimit  = "limit"
)

type Log struct {
	Label   string
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
	es := make([]log.Entry, 0, g.Line)
	for {
		e, err := rs.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		e.When = e.When.Truncate(time.Second)
		es = append(es, e)
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
	http.Handle("/sources", viewSources(config.Logs))

	if err := http.ListenAndServe(config.Addr, nil); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

func wrapHandler(next http.Handler) http.Handler {
	return allowMethod(allowOrigin(next))
}

func viewSources(logs []Log) http.Handler {
	sources := []struct {
		Label string `json:"label"`
		URL   string `json:"url"`
	}{}
	for _, g := range logs {
		if g.Label == "" {
			g.Label = strings.TrimSuffix(filepath.Base(g.File), filepath.Ext(g.File))
		}
		s := struct {
			Label string `json:"label"`
			URL   string `json:"url"`
		}{
			Label: g.Label,
			URL:   g.URL,
		}
		sources = append(sources, s)
	}
	fn := func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(sources)
	}
	return wrapHandler(http.HandlerFunc(fn))
}

func allowMethod(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet, http.MethodOptions:
			next.ServeHTTP(w, r)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
	return http.HandlerFunc(fn)
}

const (
	corsAllowOrigin = "Access-Control-Allow-Origin"
	corsAllowHeader = "Access-Control-Allow-Headers"
	corsAllowMethod = "Access-Control-Allow-Methods"
)

func allowOrigin(next http.Handler) http.Handler {
	ms := []string{http.MethodGet, http.MethodOptions}
	fn := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(corsAllowOrigin, "*")
		w.Header().Set(corsAllowMethod, strings.Join(ms, ", "))
		next.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

func retrLimit(str string) int {
	i, _ := strconv.Atoi(str)
	return i
}
