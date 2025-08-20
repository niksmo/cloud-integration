package dialer

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
)

type Dialer interface {
	DialContext(ctx context.Context, network, host string) (net.Conn, error)
}

func TLS(caRootPEMPath string) (Dialer, error) {
	caRootPEM, err := os.ReadFile(caRootPEMPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read CARootPEMFile: %w", err)
	}

	rootCAs := x509.NewCertPool()
	if ok := rootCAs.AppendCertsFromPEM(caRootPEM); !ok {
		return nil, fmt.Errorf("failed to parse CARootPEM: %q", caRootPEMPath)
	}

	tlsConfig := &tls.Config{
		RootCAs:    rootCAs,
		ClientAuth: tls.NoClientCert,
	}
	d := &tls.Dialer{Config: tlsConfig}
	return d, nil
}
