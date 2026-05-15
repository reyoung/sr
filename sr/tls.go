package sr

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

func LoadServerTLSConfig(path string) (*tls.Config, error) {
	files, err := readBundleFile(path)
	if err != nil {
		return nil, err
	}
	cert, err := tls.X509KeyPair(files[bundleServerCert], files[bundleServerKey])
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(files[bundleCACert]) {
		return nil, fmt.Errorf("server key bundle has invalid CA cert")
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

func LoadClientTLSConfig(path string) (*tls.Config, error) {
	files, err := readBundleFile(path)
	if err != nil {
		return nil, err
	}
	cert, err := tls.X509KeyPair(files[bundleClientCert], files[bundleClientKey])
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(files[bundleCACert]) {
		return nil, fmt.Errorf("client key bundle has invalid CA cert")
	}
	return &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            pool,
		InsecureSkipVerify: true,
		VerifyPeerCertificate: func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
			certs := make([]*x509.Certificate, 0, len(rawCerts))
			for _, raw := range rawCerts {
				cert, err := x509.ParseCertificate(raw)
				if err != nil {
					return err
				}
				certs = append(certs, cert)
			}
			opts := x509.VerifyOptions{Roots: pool, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
			_, err := certs[0].Verify(opts)
			return err
		},
		MinVersion: tls.VersionTLS13,
	}, nil
}

func readBundleFile(path string) (map[string][]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return readTar(data)
}
