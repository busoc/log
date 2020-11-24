package main

import (
	"context"
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
	"github.com/midbel/tail"
	"github.com/midbel/toml"
	"golang.org/x/sync/semaphore"
)

const (
	qFilter = "filter"
	qLimit  = "limit"
)

const MaxQuery = 1024

type Log struct {
	Label   string
	File    string
	URL     string
	Pattern string `toml:"format"`
	Line    int64
}

func (g Log) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == g.URL {
		g.serveEntries(w, r)
	} else if r.URL.Path == fmt.Sprintf("%s/%s", g.URL, "detail") {
		g.serveInfo(w, r)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

func (g Log) serveInfo(w http.ResponseWriter, r *http.Request) {
	i, err := os.Stat(g.File)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	c := struct {
		File    string    `json:"file"`
		Size    int64     `json:"size"`
		ModTime time.Time `json:"modtime"`
	}{
		File:    filepath.Clean(g.File),
		Size:    i.Size(),
		ModTime: i.ModTime(),
	}
	json.NewEncoder(w).Encode(c)
}

func (g Log) serveEntries(w http.ResponseWriter, r *http.Request) {
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
	w.Header().Set("content-type", "application/json")
	json.NewEncoder(w).Encode(es)
}

func (g Log) readEntries(limit int, filter string) ([]log.Entry, error) {
	if limit <= 0 {
		limit = int(g.Line)
	}
	r, err := tail.Tail(g.File, limit)
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
	return es, nil
}

type Site struct {
	Base string `toml:"dir"`
	URL  string
}

func (s Site) Handle() (string, http.Handler) {
	if i, err := os.Stat(s.Base); s.Base == "" || err != nil || !i.IsDir() {
		return "", nil
	}
	if s.URL == "" {
		s.URL = "/"
	}
	return s.URL, http.FileServer(http.Dir(s.Base))
}

func main() {
	flag.Parse()
	config := struct {
		Addr string
		Site Site
		Logs []Log `toml:"log"`
	}{}
	if err := toml.DecodeFile(flag.Arg(0), &config); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	sema := semaphore.NewWeighted(MaxQuery)

	for _, g := range config.Logs {
		if i, err := os.Stat(g.File); err != nil || i.IsDir() {
			fmt.Fprintf(os.Stderr, "%s: file does not exist! (%v)\n", g.File, err)
			os.Exit(1)
		}
		http.Handle(g.URL, wrapHandler(sema, g))
		http.Handle(fmt.Sprintf("%s/detail", g.URL), wrapHandler(sema, g))
	}
	http.Handle("/sources", viewSources(config.Logs))
	if url, handler := config.Site.Handle(); url != "" && handler != nil {
		http.Handle(url, handler)
	}

	if err := http.ListenAndServe(config.Addr, nil); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

func wrapHandler(sema *semaphore.Weighted, next http.Handler) http.Handler {
	if sema != nil {
		next = limitRequest(sema, next)
	}
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
	return wrapHandler(nil, http.HandlerFunc(fn))
}

func limitRequest(sema *semaphore.Weighted, next http.Handler) http.Handler {
	ctx := context.TODO()
	fn := func(w http.ResponseWriter, r *http.Request) {
		if err := sema.Acquire(ctx, 1); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		defer sema.Release(1)
		next.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
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
