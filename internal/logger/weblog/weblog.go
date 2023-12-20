package weblog

import (
	"bytes"
	"encoding/json"
	"errors"
	"http-collector/internal/logger"
	"io"
	"net/http"
	"strings"
)

func WebLog(l *logger.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lrw := NewLogResponseWriter(w)
		next.ServeHTTP(lrw, r)
		l.Debug("request", "method", r.Method, "path", r.URL.Path, "addr", r.RemoteAddr,
			"ua", r.UserAgent(), "code", lrw.statusCode)
	})
}

func WebLogBodyResponse(l *logger.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lrw := NewLogBodyResponseWriter(w)

		body, save, err := drainBody(r.Body)
		if err != nil {
			errReadBody(lrw, err)
			l.Error("request", "error", err, "method", r.Method, "path", r.URL.Path, "addr", r.RemoteAddr,
				"ua", r.UserAgent(), "code", lrw.statusCode)
			return
		}

		r.Body = save
		next.ServeHTTP(lrw, r)

		l.Debug("request", "method", r.Method, "path", r.URL.Path, "addr", r.RemoteAddr, "ua", r.UserAgent(),
			"code", lrw.statusCode, "body", RawJSON(body), "response", RawJSON(lrw.response.String()))
	})
}

func drainBody(b io.ReadCloser) (body []byte, r io.ReadCloser, err error) {
	if b == nil || b == http.NoBody {
		return body, http.NoBody, nil
	}

	if body, err = io.ReadAll(b); err != nil {
		return nil, b, err
	}
	if err = b.Close(); err != nil {
		return nil, b, err
	}
	return body, io.NopCloser(bytes.NewReader(body)), nil
}

func errReadBody(w http.ResponseWriter, err error) {
	var maxBytesError *http.MaxBytesError
	if errors.As(err, &maxBytesError) {
		http.Error(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
		return
	}

	http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
}

type RawJSON string

func (r RawJSON) MarshalJSON() ([]byte, error) {
	s := strings.Trim(string(r), " ")
	if strings.HasPrefix(s, "{") {
		return []byte(r), nil
	}
	return json.Marshal(s)
}

// logBodyResponseWriter implements http.ResponseWriter, capture response status code and body
type logBodyResponseWriter struct {
	http.ResponseWriter
	statusCode int
	response   strings.Builder
}

func NewLogBodyResponseWriter(w http.ResponseWriter) *logBodyResponseWriter {
	return &logBodyResponseWriter{ResponseWriter: w}
}

func (lrw *logBodyResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

func (lrw *logBodyResponseWriter) Write(data []byte) (int, error) {
	lrw.response.Write(data)
	return lrw.ResponseWriter.Write(data)
}

// logResponseWriter implments http.ResponseWriter, capture respoonse status code
type logResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func NewLogResponseWriter(w http.ResponseWriter) *logResponseWriter {
	return &logResponseWriter{ResponseWriter: w}
}

func (lrw *logResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}
