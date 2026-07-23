package client

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadTLSConfigValidation(t *testing.T) {
	if config, err := LoadTLSConfig(false, "", "", "", "", false); err != nil || config != nil {
		t.Fatalf("disabled TLS returned config=%v err=%v", config, err)
	}
	if _, err := LoadTLSConfig(false, "ca.pem", "", "", "", false); err == nil {
		t.Fatal("expected TLS options without TLS enabled to fail")
	}
	if _, err := LoadTLSConfig(true, "", "client.pem", "", "", false); err == nil {
		t.Fatal("expected an incomplete client certificate pair to fail")
	}
}

func TestLoadTLSConfigCA(t *testing.T) {
	_, certificatePEM, _ := newTestTLSCertificate(t)
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, certificatePEM, 0600); err != nil {
		t.Fatal(err)
	}

	config, err := LoadTLSConfig(true, caFile, "", "", "redis.internal", false)
	if err != nil {
		t.Fatal(err)
	}
	if config.RootCAs == nil {
		t.Fatal("expected custom root CA pool")
	}
	if config.ServerName != "redis.internal" {
		t.Fatalf("unexpected server name %q", config.ServerName)
	}
}
