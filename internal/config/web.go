package config

import (
	"errors"
	"flag"
	"net/http"
	"os"
	"time"
)

var webAddr = flag.String("addr", os.Getenv("WEB_ADDR"), "The address web server binds to, ex: :8080, env: WEB_ADDR")

type Web struct {
	Server      *http.Server
	TLS         TLS
	MaxBodySize int64
}

func NewWeb() *Web {
	return &Web{
		Server: &http.Server{
			Addr:         ":8080",
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 10 * time.Second,
			IdleTimeout:  10 * time.Second,
		},
		MaxBodySize: 524288,
	}
}

func (w *Web) Validate() error {
	if webAddr != nil && *webAddr != "" {
		w.Server.Addr = *webAddr
	}
	if w.Server.Addr == "" {
		return errors.New("missing Web addr")
	}
	return nil
}
