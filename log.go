package log

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"
)

func init() {
	sort.Strings(days)
	sort.Strings(months)
}

var (
	ErrPattern = errors.New("invalid pattern")
	ErrSyntax  = errors.New("syntax error")
)

type Entry struct {
	Line string

	Pid     int
	Process string
	User    string
	Group   string
	Level   string
	Message string
	Words   []string
	Host    string
	When    time.Time
}

type Reader struct {
	inner *bufio.Scanner
	err   error

	keep  filterfunc
	parse parsefunc
}

func NewReader(rs io.Reader, pattern, filter string) (*Reader, error) {
	var (
		r   Reader
		err error
	)
	r.inner = bufio.NewScanner(rs)

	if r.parse, err = parsePattern(pattern); err != nil {
		return nil, err
	}
	if r.keep, err = parseFilter(filter); err != nil {
		return nil, err
	}
	return &r, nil
}

func (r *Reader) ReadAll() ([]Entry, error) {
	var (
		es  []Entry
		e   Entry
		err error
	)
	for {
		e, err = r.Read()
		if err != nil {
			break
		}
		es = append(es, e)
	}
	return es, err
}

func (r *Reader) Read() (Entry, error) {
	var e Entry
	if r.err != nil {
		return e, r.err
	}
	for {
		if !r.inner.Scan() {
			r.err = r.inner.Err()
			if r.err == nil {
				r.err = io.EOF
			}
			return e, r.err
		}
		line := r.inner.Bytes()
		if len(line) == 0 {
			continue
		}
		err := r.parse(&e, bytes.NewReader(line))
		if err != nil {
			if errors.Is(err, ErrPattern) {
				continue
			}
			r.err = err
			return e, r.err
		}
		if r.keep == nil || r.keep(e) {
			e.Line = r.inner.Text()
			break
		}
	}
	return e, r.err
}

type (
	parsefunc  func(*Entry, io.RuneScanner) error
	whenfunc   func(*when, io.RuneScanner) error
	hostfunc   func(*host, io.RuneScanner) error
	filterfunc func(Entry) bool
)

func parseFilter(str string) (filterfunc, error) {
	if str == "" {
		return func(_ Entry) bool { return true }, nil
	}
	return nil, nil
}

func parsePattern(pattern string) (parsefunc, error) {
	if pattern == "" {
		return nil, fmt.Errorf("%w: empty pattern not allowed", ErrSyntax)
	}
	var (
		pfs []parsefunc
		str = bytes.NewReader([]byte(pattern))
		buf bytes.Buffer
	)
	for str.Len() > 0 {
		r, _, _ := str.ReadRune()
		if r == '%' {
			r, _, err := str.ReadRune()
			if err != nil {
				return nil, err
			}
			if r == '%' {
				buf.WriteRune(r)
				continue
			}
			if buf.Len() > 0 {
				pfs = append(pfs, parseLiteral(buf.String()))
				buf.Reset()
			}
			switch r {
			case 't':
				arg, err := parseArgument(str, rfcPattern, "time")
				if err != nil {
					return nil, err
				}
				fn, err := parseTime(arg)
				if err != nil {
					return nil, err
				}
				pfs = append(pfs, fn)
			case 'b':
				pfs = append(pfs, parseBlank())
			case 'n':
				pfs = append(pfs, parseProcess())
			case 'p':
				pfs = append(pfs, parsePID())
			case 'u':
				pfs = append(pfs, parseUser())
			case 'g':
				pfs = append(pfs, parseGroup())
			case 'h':
				arg, err := parseArgument(str, "%f", "host")
				if err != nil {
					return nil, err
				}
				fn, err := parseHost(arg)
				if err != nil {
					return nil, err
				}
				pfs = append(pfs, fn)
			case 'l':
				arg, err := parseArgument(str, "", "level")
				if err != nil {
					return nil, err
				}
				fn, err := parseLevel(arg)
				if err != nil {
					return nil, err
				}
				pfs = append(pfs, fn)
			case 'm':
				pfs = append(pfs, parseMessage())
			case 'w':
				pfs = append(pfs, parseWord(""))
			default:
				return nil, fmt.Errorf("%w: unsupported specifier %%%c", ErrSyntax, r)
			}
		} else {
			buf.WriteRune(r)
		}
	}
	if buf.Len() > 0 {
		pfs = append(pfs, parseLiteral(buf.String()))
	}
	return mergeParse(pfs), nil
}

func mergeParse(pfs []parsefunc) parsefunc {
	return func(e *Entry, r io.RuneScanner) error {
		for _, pf := range pfs {
			if err := pf(e, r); err != nil {
				return err
			}
		}
		return nil
	}
}

func parseArgument(str *bytes.Reader, option, what string) (string, error) {
	r, _, _ := str.ReadRune()
	if r != '(' {
		if option != "" {
			return option, str.UnreadRune()
		}
		return "", fmt.Errorf("%w(%s): missing (", ErrSyntax, what)
	}
	var buf bytes.Buffer
	for str.Len() > 0 {
		r, _, _ := str.ReadRune()
		if r == ')' {
			return buf.String(), nil
		}
		buf.WriteRune(r)
		if buf.Len() > 64 {
			return "", fmt.Errorf("%w(%s): argument too long (%s)", ErrSyntax, what, buf.String())
		}
	}
	return "", fmt.Errorf("%w(%s): missing )", ErrSyntax, what)
}

func parseLevel(level string) (parsefunc, error) {
	level = strings.Map(func(r rune) rune {
		if isBlank(r) {
			return -1
		}
		return r
	}, level)
	levels := strings.Split(level, ",")
	sort.Strings(levels)
	fn := func(e *Entry, r io.RuneScanner) error {
		e.Level, _ = parseString(r, 0, isLetter)
		x := sort.SearchStrings(levels, e.Level)
		if len(levels) > 0 && (x >= len(levels) || levels[x] != e.Level) {
			return ErrPattern
		}
		return nil
	}
	return fn, nil
}

func parseTime(str string) (parsefunc, error) {
	parse, err := parseTimePattern(str)
	if err != nil {
		return nil, err
	}
	fn := func(e *Entry, r io.RuneScanner) error {
		var (
			w   when
			err = parse(&w, r)
		)
		if err == nil {
			e.When = w.Time()
		}
		return err
	}
	return fn, nil
}

func parseHost(str string) (parsefunc, error) {
	parse, err := parseHostPattern(str)
	if err != nil {
		return nil, err
	}
	fn := func(e *Entry, r io.RuneScanner) error {
		var h host
		if err := parse(&h, r); err != nil {
			return err
		}
		e.Host = h.String()
		return nil
	}
	return fn, nil
}

func parsePID() parsefunc {
	return func(e *Entry, r io.RuneScanner) error {
		str, _ := parseString(r, 0, isDigit)
		p, err := strconv.Atoi(str)
		if err == nil {
			e.Pid = p
		}
		return err
	}
}

func parseLiteral(str string) parsefunc {
	return func(e *Entry, r io.RuneScanner) error {
		pat := strings.NewReader(str)
		for pat.Len() > 0 {
			w, _, _ := pat.ReadRune()
			g, _, _ := r.ReadRune()
			if w != g {
				return ErrPattern
			}
		}
		return nil
	}
}

func parseMessage() parsefunc {
	return func(e *Entry, r io.RuneScanner) error {
		e.Message, _ = parseString(r, 0, func(r rune) bool { return !isEOL(r) })
		return nil
	}
}

func parseWord(str string) parsefunc {
	return func(e *Entry, r io.RuneScanner) error {
		var (
			buf     bytes.Buffer
			quote   = peek(r)
			isDelim = func(r rune) (bool, error) { return isBlank(r) || isEOL(r), nil }
		)
		if isQuote(quote) {
			r.ReadRune()
			isDelim = func(r rune) (bool, error) {
				if isEOL(r) {
					return false, ErrPattern
				}
				return r == quote, nil
			}
		}
		for {
			z, _, _ := r.ReadRune()
			ok, err := isDelim(z)
			if err != nil {
				return err
			}
			if ok {
				break
			}
			buf.WriteRune(z)
		}
		if !isQuote(quote) {
			r.UnreadRune()
		}
		if str := strings.TrimSpace(buf.String()); str != "" {
			e.Words = append(e.Words, str)
		}
		return nil
	}
}

func parseUser() parsefunc {
	return func(e *Entry, r io.RuneScanner) error {
		e.User, _ = parseString(r, 0, isAlpha)
		return nil
	}
}

func parseGroup() parsefunc {
	return func(e *Entry, r io.RuneScanner) error {
		e.Group, _ = parseString(r, 0, isAlpha)
		return nil
	}
}

func parseProcess() parsefunc {
	return func(e *Entry, r io.RuneScanner) error {
		e.Process, _ = parseString(r, 0, isAlpha)
		return nil
	}
}

func parseBlank() parsefunc {
	return func(_ *Entry, r io.RuneScanner) error {
		parseString(r, 0, isBlank)
		return nil
	}
}

var days = []string{
	"mon",
	"tue",
	"wed",
	"thu",
	"fri",
	"sat",
	"sun",
}

var months = []string{
	"jan",
	"feb",
	"mar",
	"apr",
	"may",
	"jun",
	"jul",
	"aug",
	"sep",
	"oct",
	"nov",
	"dec",
}

const (
	isoPattern = "%y-%m-%d %H:%M:%S%Z"
	rfcPattern = "%y-%m-%dT%H:%M:%S%Z"
)

type when struct {
	Year int
	Mon  int
	Day  int
	Hour int
	Min  int
	Sec  int
	Frac int

	Zone    int
	YearDay int
	Unix    int
}

func (w when) Time() time.Time {
	if w.Unix != 0 {
		return time.Unix(int64(w.Unix), 0)
	}
	if w.Year == 0 {
		w.Year++
	}
	if w.Mon == 0 {
		w.Mon++
	}
	if w.Day == 0 {
		w.Day++
	}
	zone := time.UTC
	if w.Zone != 0 {
		zone = time.FixedZone("", w.Zone)
	}
	t := time.Date(w.Year, time.Month(w.Mon), w.Day, w.Hour, w.Min, w.Sec, w.Frac, zone)
	if w.YearDay > 0 {
		t = t.AddDate(0, 0, w.YearDay-t.YearDay()+1)
	}
	return t
}

func parseTimePattern(pattern string) (whenfunc, error) {
	if pattern == "" {
		pattern = isoPattern
	}
	var (
		str = bytes.NewReader([]byte(pattern))
		buf bytes.Buffer
		wfs []whenfunc
	)
	for str.Len() > 0 {
		r, _, _ := str.ReadRune()
		if r == '%' {
			r, _, _ := str.ReadRune()
			if r == '%' {
				buf.WriteRune(r)
				continue
			}
			if buf.Len() > 0 {
				wfs = append(wfs, parseWhenLiteral(buf.String()))
				buf.Reset()
			}
			switch r {
			case 'I':
				fn, err := parseTimePattern(isoPattern)
				if err != nil {
					return nil, err
				}
				wfs = append(wfs, fn)
			case 'R':
				fn, err := parseTimePattern(isoPattern)
				if err != nil {
					return nil, err
				}
				wfs = append(wfs, fn)
			case 'y':
				wfs = append(wfs, parseYear)
			case 'm':
				wfs = append(wfs, parseMonth)
			case 'd':
				wfs = append(wfs, parseDay)
			case 'j':
				wfs = append(wfs, parseDOY)
			case 'a':
				wfs = append(wfs, parseDayStr)
			case 'b':
				wfs = append(wfs, parseMonthStr)
			case 's':
				wfs = append(wfs, parseTimestamp)
			case 'H':
				wfs = append(wfs, parseHour)
			case 'M':
				wfs = append(wfs, parseMinute)
			case 'S':
				wfs = append(wfs, parseSecond)
			case 'f':
				wfs = append(wfs, parseFraction)
			case 'Z':
				wfs = append(wfs, parseZone)
			default:
				return nil, fmt.Errorf("%w(time): unknown specifier %c", ErrSyntax, r)
			}
		} else {
			buf.WriteRune(r)
		}
	}
	if buf.Len() > 0 {
		wfs = append(wfs, parseWhenLiteral(buf.String()))
	}
	return mergeWhen(wfs), nil
}

func mergeWhen(wfs []whenfunc) whenfunc {
	return func(w *when, r io.RuneScanner) error {
		for _, fn := range wfs {
			if err := fn(w, r); err != nil {
				return err
			}
		}
		return nil
	}
}

func parseYear(w *when, r io.RuneScanner) error {
	return parseInt(&w.Year, 4, r, isDigit)
}

func parseDOY(w *when, r io.RuneScanner) error {
	return parseInt(&w.YearDay, 3, r, isDigit)
}

func parseDay(w *when, r io.RuneScanner) error {
	return parseInt(&w.Day, 2, r, isDigit)
}

func parseDayStr(w *when, r io.RuneScanner) error {
	day, err := parseString(r, 3, isLetter)
	if err != nil {
		return err
	}
	x := sort.SearchStrings(days, day)
	if x >= len(days) || days[x] != day {
		return ErrPattern
	}
	return nil
}

func parseMonth(w *when, r io.RuneScanner) error {
	return parseInt(&w.Mon, 2, r, isDigit)
}

func parseMonthStr(w *when, r io.RuneScanner) error {
	month, err := parseString(r, 3, isLetter)
	if err != nil {
		return err
	}
	x := sort.SearchStrings(months, month)
	if x >= len(months) || months[x] != month {
		return ErrPattern
	}
	w.Mon = x + 1
	return nil
}

func parseHour(w *when, r io.RuneScanner) error {
	return parseInt(&w.Hour, 2, r, isDigit)
}

func parseMinute(w *when, r io.RuneScanner) error {
	return parseInt(&w.Min, 2, r, isDigit)
}

func parseSecond(w *when, r io.RuneScanner) error {
	return parseInt(&w.Sec, 2, r, isDigit)
}

func parseTimestamp(w *when, r io.RuneScanner) error {
	return parseInt(&w.Unix, 0, r, isDigit)
}

func parseZone(w *when, r io.RuneScanner) error {
	switch z, _, _ := r.ReadRune(); z {
	case 'Z':
	case '+', '-':
		w.Zone++
		if z == '-' {
			w.Zone *= -1
		}
		var i int
		if err := parseInt(&i, 2, r, isDigit); err != nil {
			return err
		}
		w.Zone *= i * 60 * 60
		if z := peek(r); z == ':' {
			r.ReadRune()
		}
		if z := peek(r); isDigit(z) {
			err := parseInt(&i, 2, r, isDigit)
			if err == nil {
				w.Zone += i * 60
			}
			return err
		}
	default:
		return ErrPattern
	}
	return nil
}

func parseFraction(w *when, r io.RuneScanner) error {
	if err := parseInt(&w.Frac, 0, r, isDigit); err != nil {
		return err
	}
	switch {
	case w.Frac < 1000:
		w.Frac *= 1000 * 1000
	case w.Frac < 1000*1000:
		w.Frac *= 1000
	default:
	}
	return nil
}

func parseWhenLiteral(str string) whenfunc {
	return func(_ *when, r io.RuneScanner) error {
		pat := strings.NewReader(str)
		for pat.Len() > 0 {
			w, _, _ := pat.ReadRune()
			g, _, _ := r.ReadRune()
			if w != g {
				return ErrPattern
			}
		}
		return nil
	}
}

const (
	ip4len = 4
	ip6len = 8
)

const (
	ip4long  = "%4:%p"
	ip6long  = "%6:%p"
	fqdnlong = "%f:%p"
)

type host struct {
	Name string
	Addr string
	Mask int
	Port int
}

func (h host) String() string {
	if h.Name != "" {
		return h.Name
	}
	return fmt.Sprintf("%s:%d", h.Addr, h.Port)
}

func parseHostPattern(pattern string) (hostfunc, error) {
	var (
		str = bytes.NewReader([]byte(pattern))
		buf bytes.Buffer
		hfs []hostfunc
	)
	for str.Len() > 0 {
		r, _, _ := str.ReadRune()
		if r == '%' {
			r, _, _ = str.ReadRune()
			if r == '%' {
				buf.WriteRune(r)
				continue
			}
			if buf.Len() > 0 {
				hfs = append(hfs, parseHostLiteral(buf.String()))
				buf.Reset()
			}
			switch r {
			case '4':
				hfs = append(hfs, parseIPv4)
			case '6':
				hfs = append(hfs, parseIPv6)
			case 'p':
				hfs = append(hfs, parsePort)
			case 'f':
				hfs = append(hfs, parseFQDN)
			case 'h':
				hfs = append(hfs, parseHostname)
			case 'm':
				hfs = append(hfs, parseMask)
			case 'F':
				fn, err := parseHostPattern(ip4long)
				if err != nil {
					return nil, err
				}
				hfs = append(hfs, fn)
			case 'S':
				fn, err := parseHostPattern(ip6long)
				if err != nil {
					return nil, err
				}
				hfs = append(hfs, fn)
			case 'Q':
				fn, err := parseHostPattern(fqdnlong)
				if err != nil {
					return nil, err
				}
				hfs = append(hfs, fn)
			default:
				return nil, fmt.Errorf("%w(host): unknown specifier %c", ErrSyntax, r)
			}
		} else {
			buf.WriteRune(r)
		}
	}
	if buf.Len() > 0 {
		hfs = append(hfs, parseHostLiteral(buf.String()))
	}
	return mergeHost(hfs), nil
}

func mergeHost(hfs []hostfunc) hostfunc {
	return func(h *host, r io.RuneScanner) error {
		for _, fn := range hfs {
			if err := fn(h, r); err != nil {
				return err
			}
		}
		return nil
	}
}

func parseHostLiteral(str string) hostfunc {
	return func(_ *host, r io.RuneScanner) error {
		pat := strings.NewReader(str)
		for pat.Len() > 0 {
			w, _, _ := pat.ReadRune()
			g, _, _ := r.ReadRune()
			if w != g {
				return ErrPattern
			}
		}
		return nil
	}
}

func parseIPv4(h *host, r io.RuneScanner) error {
	var buf bytes.Buffer
	for i := 0; i < ip4len; i++ {
		var j int
		if err := parseInt(&j, 0, r, isDigit); err != nil {
			return err
		}
		if j < 0 || j > 0xFF {
			return ErrPattern
		}
		buf.WriteString(strconv.Itoa(j))
		if i < ip4len-1 {
			if k := peek(r); k != '.' {
				return ErrPattern
			}
			r.ReadRune()
			buf.WriteRune('.')
		}
	}
	h.Addr = buf.String()
	return nil
}

func parseIPv6(h *host, r io.RuneScanner) error {
	var (
		buf   bytes.Buffer
		quote = peek(r)
	)
	if quote == '[' {
		r.ReadRune()
	}
	for i := 0; i < ip6len; i++ {
		var j int
		if err := parseInt(&j, 0, r, isHexa); err != nil {
			return err
		}
		if j < 0 || j > 0xFFFF {
			return ErrPattern
		}
		buf.WriteString(strconv.FormatInt(int64(j), 16))
		if i < ip6len-1 {
			if k := peek(r); k != ':' {
				break
			}
			buf.WriteRune(':')
			r.ReadRune()
			if k := peek(r); k == ':' {
				buf.WriteRune(':')
				r.ReadRune()
			}
		}
	}
	if k := peek(r); quote == '[' {
		if k != ']' {
			return ErrPattern
		}
		r.ReadRune()
	}
	h.Addr = buf.String()
	return nil
}

func parseMask(h *host, r io.RuneScanner) error {
	if err := parseInt(&h.Mask, 0, r, isDigit); err != nil {
		return err
	}
	if h.Mask < 0 || h.Mask > 32 {
		return ErrPattern
	}
	return nil
}

func parsePort(h *host, r io.RuneScanner) error {
	if err := parseInt(&h.Port, 0, r, isDigit); err != nil {
		return err
	}
	if h.Port < 0 || h.Port > 0xFFFF {
		return ErrPattern
	}
	return nil
}

func parseHostname(h *host, r io.RuneScanner) error {
	h.Name, _ = parseString(r, 0, isAlpha)
	return nil
}

func parseFQDN(h *host, r io.RuneScanner) error {
	var buf bytes.Buffer
	for {
		part, _ := parseString(r, 0, isAlpha)
		buf.WriteString(part)
		if k := peek(r); k != '.' {
			break
		}
		buf.WriteRune('.')
		r.ReadRune()
	}
	h.Name = buf.String()
	return nil
}

func parseInt(i *int, n int, str io.RuneScanner, accept func(rune) bool) error {
	var buf bytes.Buffer
	for i := 0; n <= 0 || i < n; i++ {
		r, _, err := str.ReadRune()
		if err != nil {
			return err
		}
		if !accept(r) {
			if n == 0 {
				str.UnreadRune()
				break
			}
			return ErrPattern
		}
		buf.WriteRune(r)
	}
	x, err := strconv.ParseInt(buf.String(), 0, 64)
	if err == nil {
		*i = int(x)
	}
	return err
}

func parseString(r io.RuneScanner, length int, accept func(rune) bool) (string, error) {
	if accept == nil {
		accept = func(_ rune) bool { return true }
	}
	defer r.UnreadRune()
	var buf bytes.Buffer
	for i := 0; length <= 0 || i < length; i++ {
		c, _, _ := r.ReadRune()
		if !accept(c) {
			break
		}
		buf.WriteRune(c)
	}
	if length > 0 && buf.Len() != length {
		return "", ErrPattern
	}
	return buf.String(), nil
}

func peek(r io.RuneScanner) rune {
	defer r.UnreadRune()
	c, _, _ := r.ReadRune()
	return c
}

func isDigit(r rune) bool {
	return r >= '0' && r <= '9'
}

func isHexa(r rune) bool {
	return isDigit(r) || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')
}

func isLetter(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}

func isAlpha(r rune) bool {
	return isDigit(r) || isLetter(r) || r == '-' || r == '_'
}

func isBlank(r rune) bool {
	return r == ' ' || r == '\t'
}

func isEOL(r rune) bool {
	return r == 0
}

func isQuote(r rune) bool {
	return r == '\'' || r == '"'
}
