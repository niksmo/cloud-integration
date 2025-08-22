package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

func CreateTLSConfig(CARootFilepath string) (*tls.Config, error) {
	caRootPEM, err := os.ReadFile(CARootFilepath)
	if err != nil {
		return nil, fmt.Errorf("failed to read CARootPEMFile: %w", err)
	}

	rootCAs := x509.NewCertPool()
	if ok := rootCAs.AppendCertsFromPEM(caRootPEM); !ok {
		return nil, fmt.Errorf("failed to parse CARootPEM: %q", CARootFilepath)
	}

	c := &tls.Config{
		RootCAs:    rootCAs,
		ClientAuth: tls.NoClientCert,
	}
	return c, nil
}
