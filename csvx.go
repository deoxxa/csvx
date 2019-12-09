package csvx

import (
	"compress/gzip"
	"encoding"
	"encoding/csv"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/mitchellh/ioprogress"
	"github.com/pkg/errors"

	"fknsrs.biz/p/civil"
	"fknsrs.biz/p/timex"
)

type Scanner interface {
	ScanString(s string) error
}

type Reader struct {
	fd  io.Reader
	rd  *csv.Reader
	hdr []string
	row []string
	err error
	cl  func() error
	tz  *time.Location
}

type Option func(rd *Reader) error

func FromFile(filename string) Option {
	return func(rd *Reader) error {
		fd, err := os.Open(filename)
		if err != nil {
			return errors.Wrap(err, "csvx.FromPath")
		}

		rd.fd = fd
		rd.cl = fd.Close

		if strings.HasSuffix(filename, ".gz") {
			gz, err := gzip.NewReader(rd.fd)
			if err != nil {
				return errors.Wrap(err, "csvx.FromPath")
			}

			rd.fd = gz
		}

		return nil
	}
}

func FromReader(fd io.Reader) Option {
	return func(rd *Reader) error {
		rd.fd = fd

		if cl, ok := fd.(io.Closer); ok {
			rd.cl = cl.Close
		}

		return nil
	}
}

func WithTZ(tz *time.Location) Option {
	return func(rd *Reader) error {
		rd.tz = tz

		return nil
	}
}

type canStat interface {
	Stat() (os.FileInfo, error)
}

func WithProgress() Option {
	return WithProgressWindow(30)
}

func WithProgressWindow(window int) Option {
	return func(rd *Reader) error {
		fd, ok := rd.fd.(canStat)
		if !ok {
			return nil
		}

		st, err := fd.Stat()
		if err != nil {
			return errors.Wrap(err, "csvx.WithProgressWindow")
		}

		rd.fd = &ioprogress.Reader{
			Reader:   rd.fd,
			Size:     st.Size(),
			DrawFunc: ioprogress.DrawTerminalf(os.Stderr, timeRemainingFormatter(window)),
		}

		return nil
	}
}

func NewReader(opts ...Option) (*Reader, error) {
	r := &Reader{}

	for _, fn := range opts {
		if err := fn(r); err != nil {
			return nil, errors.Wrap(err, "csvx.NewReader")
		}
	}

	if r.fd == nil {
		return nil, errors.Errorf("csvx.NewReader: fd is nil after option processing")
	}

	r.rd = csv.NewReader(r.fd)

	hdr, err := r.rd.Read()
	if err != nil {
		return r, errors.Wrap(err, "csvx.NewReader: couldn't read header")
	}

	r.hdr = hdr

	return r, nil
}

func (r *Reader) Next() bool {
	r.row, r.err = r.rd.Read()
	if r.err == io.EOF {
		r.err = nil
		return false
	}

	return true
}

func (r *Reader) Scan(out ...interface{}) error {
	if r.err != nil {
		return r.err
	}

	for i, e := range out {
		c := ""

		if len(r.row) > i {
			c = strings.TrimSpace(r.row[i])
		}

		switch e := e.(type) {
		case nil:
			// nothing
		case *string:
			*e = c
		case *int:
			n, err := strconv.ParseInt(c, 10, 64)
			if err != nil {
				return errors.Wrapf(err, "csvx.Reader.Scan(%T) (index %d)", e, i)
			}
			*e = int(n)
		case **int:
			if c == "" {
				*e = nil
			} else {
				n, err := strconv.ParseInt(c, 10, 64)
				if err != nil {
					return errors.Wrapf(err, "csvx.Reader.Scan(%T) (index %d)", e, i)
				}
				v := int(n)
				*e = &v
			}
		case *float64:
			n, err := strconv.ParseFloat(c, 64)
			if err != nil {
				return errors.Wrapf(err, "csvx.Reader.Scan(%T) (index %d)", e, i)
			}
			*e = n
		case **float64:
			if c == "" {
				*e = nil
			} else {
				n, err := strconv.ParseFloat(c, 64)
				if err != nil {
					return errors.Wrapf(err, "csvx.Reader.Scan(%T) (index %d)", e, i)
				}
				*e = &n
			}
		case *time.Time:
			t, err := timex.ParseDefaultsInLocation(c, r.tz)
			if err != nil {
				return errors.Wrapf(err, "csvx.Reader.Scan(%T) (index %d)", e, i)
			}
			*e = t
		case **time.Time:
			if c == "" {
				*e = nil
			} else {
				t, err := timex.ParseDefaultsInLocation(c, r.tz)
				if err != nil {
					return errors.Wrapf(err, "csvx.Reader.Scan(%T) (index %d)", e, i)
				}
				*e = &t
			}
		case *civil.Date:
			t, err := civil.ParseDate(c)
			if err != nil {
				return errors.Wrapf(err, "csvx.Reader.Scan(%T) (index %d)", e, i)
			}
			*e = t
		case **civil.Date:
			if c == "" {
				*e = nil
			} else {
				t, err := civil.ParseDate(c)
				if err != nil {
					return errors.Wrapf(err, "csvx.Reader.Scan(%T) (index %d)", e, i)
				}
				*e = &t
			}
		case *bool:
			if c == "1" || c == "yes" || c == "true" || c == "t" {
				*e = true
			} else if c == "" || c == "0" || c == "no" || c == "false" || c == "f" {
				*e = false
			} else {
				return errors.Errorf("csvx.Reader.Scan(%T) (index %d): couldn't convert %q to boolean", e, i, c)
			}
		default:
			p := reflect.ValueOf(e)

			if p.Type().Kind() != reflect.Ptr {
				return errors.Errorf("csvx.Reader.Scan(%T) (index %d): can't scan into %T; must be a pointer", e, i, e)
			}

			if t := p.Type().Elem(); t.Kind() == reflect.Ptr && c == "" {
				p.Elem().Set(reflect.Zero(t))
				continue
			}

			if p.Type().Elem().Kind() == reflect.Ptr && p.Elem().IsNil() {
				p.Elem().Set(reflect.New(p.Type().Elem().Elem()))
				p = p.Elem()
			}

			v := p.Interface()

			if s, ok := v.(Scanner); ok {
				if err := s.ScanString(c); err != nil {
					return errors.Wrapf(err, "csvx.Reader.Scan(%T) (index %d)", e, i)
				}

				continue
			}

			if s, ok := v.(encoding.TextUnmarshaler); ok {
				if err := s.UnmarshalText([]byte(c)); err != nil {
					return errors.Wrapf(err, "csvx.Reader.Scan(%T) (index %d)", e, i)
				}

				continue
			}

			return errors.Errorf("csvx.Reader.Scan(%T) (index %d): can't scan into %T", e, i, e)
		}
	}

	return nil
}

func (r *Reader) ScanStruct(out interface{}) error {
	if r.err != nil {
		return r.err
	}

	ptr := reflect.ValueOf(out)
	if ptr.Kind() != reflect.Ptr {
		return errors.Errorf("csvx.Reader.ScanStruct: expected out to be pointer; was instead %s", ptr.Kind())
	}

	str := reflect.Indirect(ptr)
	if str.Kind() != reflect.Struct {
		return errors.Errorf("csvx.Reader.ScanStruct: expected out to be pointer to struct; was instead pointer to %s", str.Kind())
	}

	typ := str.Type()

	vars := make([]interface{}, len(r.hdr))

outer:
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)

		a := strings.Split(f.Tag.Get("csv"), ",")
		if a[0] == "-" {
			continue
		}

		name := f.Name
		if a[0] != "" {
			name = a[0]
		}

		for j, c := range r.hdr {
			if c == name {
				vars[j] = str.Field(i).Addr().Interface()
				continue outer
			}
		}

		for j, c := range r.hdr {
			if strings.ToLower(c) == strings.ToLower(name) {
				vars[j] = str.Field(i).Addr().Interface()
				continue outer
			}
		}

		for j, c := range r.hdr {
			if strings.ToLower(strings.Replace(c, "_", " ", -1)) == strings.ToLower(strings.Replace(name, "_", " ", -1)) {
				vars[j] = str.Field(i).Addr().Interface()
				continue outer
			}
		}

		return errors.Errorf("csvx.Reader.ScanStruct: couldn't find column in %v for field %s", r.hdr, f.Name)
	}

	if err := r.Scan(vars...); err != nil {
		return errors.Wrap(err, "csvx.Reader.ScanStruct")
	}

	return nil
}

func (r *Reader) Close() error {
	if r.cl == nil {
		return nil
	}

	return errors.Wrap(r.cl(), "csvx.Reader.Close")
}

func FindColumns(row []string, names ...string) (map[string]int, error) {
	m := make(map[string]int)
	var missing []string

outer:
	for _, n := range names {
		for i, e := range row {
			e = strings.TrimSpace(e)

			if strings.Replace(strings.ToLower(e), " ", "_", -1) == strings.Replace(strings.ToLower(n), " ", "_", -1) {
				m[n] = i
				continue outer
			}
		}

		missing = append(missing, n)
	}

	if len(missing) > 0 {
		return nil, errors.Errorf("findColumns: couldn't find columns %s", strings.Join(missing, ", "))
	}

	return m, nil
}
