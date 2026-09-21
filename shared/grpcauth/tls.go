package grpcauth

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
)

// ServerTLS requires certificates signed by the configured internal client CA.
func ServerTLS(certFile, keyFile, caFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, errors.New("collector TLS key pair unavailable")
	}
	ca, err := os.ReadFile(caFile)
	if err != nil {
		return nil, errors.New("collector client CA unavailable")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("collector client CA invalid")
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert}, nil
}
