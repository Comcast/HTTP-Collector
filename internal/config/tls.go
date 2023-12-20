package config

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
)

// TLS holds config for TLS
type TLS struct {
	CertFile string // Client or Server cert
	KeyFile  string
	CaFile   string // Root cert
	Insecure bool
	cert     []tls.Certificate
	ca       *x509.CertPool
}

// GetClientTLS return tls.config for use by client. Invoke Validate before calling this.
func (t *TLS) GetClientTLS() *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: t.Insecure,
		Certificates:       t.cert,
		RootCAs:            t.ca,
	}
}

// GetServerTLS return tls.config for use by server. Invoke Validate before calling this.
func (t *TLS) GetServerTLS() *tls.Config {
	if t.cert == nil {
		return nil
	}

	c := &tls.Config{Certificates: t.cert}
	if t.ca != nil {
		c.ClientAuth = tls.RequireAndVerifyClientCert
		c.ClientCAs = t.ca
	}
	return c
}

// Set tls.config based on config. Invoke Validate before calling this.
func (t *TLS) Set(orig *tls.Config) {
	orig.Certificates = t.cert
	orig.RootCAs = t.ca
	orig.InsecureSkipVerify = t.Insecure
}

func (t *TLS) Validate() error {
	if t.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return err
		}
		t.cert = []tls.Certificate{cert}
	}
	if t.CaFile != "" {
		caCert, err := os.ReadFile(t.CaFile)
		if err != nil {
			return err
		}
		caCertPool := x509.NewCertPool()
		if caCertPool.AppendCertsFromPEM(caCert) {
			t.ca = caCertPool
		} else {
			return errors.New("unable to load CA")
		}
	}
	return nil
}
