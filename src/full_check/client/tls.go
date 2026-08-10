package client

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

func LoadTLSConfig(enabled bool, caFile, certFile, keyFile, serverName string, skipVerify bool) (*tls.Config, error) {
	if !enabled {
		if caFile != "" || certFile != "" || keyFile != "" || serverName != "" || skipVerify {
			return nil, fmt.Errorf("TLS options require TLS to be enabled")
		}
		return nil, nil
	}

	if (certFile == "") != (keyFile == "") {
		return nil, fmt.Errorf("TLS client certificate and key must be specified together")
	}

	config := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         serverName,
		InsecureSkipVerify: skipVerify,
	}

	if caFile != "" {
		caPEM, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read TLS CA certificate %q: %w", caFile, err)
		}

		rootCAs, err := x509.SystemCertPool()
		if err != nil || rootCAs == nil {
			rootCAs = x509.NewCertPool()
		}
		if !rootCAs.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("TLS CA certificate %q does not contain a valid PEM certificate", caFile)
		}
		config.RootCAs = rootCAs
	}

	if certFile != "" {
		certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("load TLS client certificate %q and key %q: %w", certFile, keyFile, err)
		}
		config.Certificates = []tls.Certificate{certificate}
	}

	return config, nil
}
