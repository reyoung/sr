package sr

import (
	"archive/tar"
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"fmt"
	"io"
	"math/big"
	"os"
	"time"
)

const (
	bundleCACert     = "ca.crt"
	bundleCAKey      = "ca.key"
	bundleServerCert = "server.crt"
	bundleServerKey  = "server.key"
	bundleClientCert = "client.crt"
	bundleClientKey  = "client.key"
)

func runGenKey(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("gen-key", flag.ContinueOnError)
	server := fs.Bool("server", false, "generate server key bundle")
	clientServerKey := fs.String("client", "", "server key bundle used to sign a client cert")
	label := fs.String("label", "", "client label")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *server == (*clientServerKey != "") {
		return fmt.Errorf("gen-key requires exactly one of --server or --client server.key")
	}
	if *server {
		b, err := GenerateServerBundle()
		if err != nil {
			return err
		}
		_, err = stdout.Write(b)
		return err
	}
	if *label == "" {
		return fmt.Errorf("gen-key --client requires --label")
	}
	serverBundle, err := os.ReadFile(*clientServerKey)
	if err != nil {
		return err
	}
	b, err := GenerateClientBundle(serverBundle, *label)
	if err != nil {
		return err
	}
	_, err = stdout.Write(b)
	return err
}

func GenerateServerBundle() ([]byte, error) {
	caKey, err := rsa.GenerateKey(rand.Reader, 3072)
	if err != nil {
		return nil, err
	}
	caCertTmpl := certTemplate("sr-ca", true, nil)
	caDER, err := x509.CreateCertificate(rand.Reader, caCertTmpl, caCertTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, err
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, err
	}

	serverKey, err := rsa.GenerateKey(rand.Reader, 3072)
	if err != nil {
		return nil, err
	}
	serverTmpl := certTemplate("sr-server", false, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTmpl, caCert, &serverKey.PublicKey, caKey)
	if err != nil {
		return nil, err
	}
	return writeTar(map[string][]byte{
		bundleCACert:     pemBlock("CERTIFICATE", caDER),
		bundleCAKey:      pemBlock("RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(caKey)),
		bundleServerCert: pemBlock("CERTIFICATE", serverDER),
		bundleServerKey:  pemBlock("RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(serverKey)),
	})
}

func GenerateClientBundle(serverBundle []byte, label string) ([]byte, error) {
	files, err := readTar(serverBundle)
	if err != nil {
		return nil, err
	}
	caCert, err := parseCert(files[bundleCACert])
	if err != nil {
		return nil, err
	}
	caKey, err := parseRSAKey(files[bundleCAKey])
	if err != nil {
		return nil, err
	}
	clientKey, err := rsa.GenerateKey(rand.Reader, 3072)
	if err != nil {
		return nil, err
	}
	clientTmpl := certTemplate(label, false, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTmpl, caCert, &clientKey.PublicKey, caKey)
	if err != nil {
		return nil, err
	}
	return writeTar(map[string][]byte{
		bundleCACert:     files[bundleCACert],
		bundleClientCert: pemBlock("CERTIFICATE", clientDER),
		bundleClientKey:  pemBlock("RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(clientKey)),
	})
}

func certTemplate(cn string, isCA bool, eku []x509.ExtKeyUsage) *x509.Certificate {
	keyUsage := x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment
	if isCA {
		keyUsage |= x509.KeyUsageCertSign | x509.KeyUsageCRLSign
	}
	return &x509.Certificate{
		SerialNumber:          mustSerial(),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		KeyUsage:              keyUsage,
		ExtKeyUsage:           eku,
		BasicConstraintsValid: true,
		IsCA:                  isCA,
	}
}

func mustSerial() *big.Int {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		panic(err)
	}
	return serial
}

func pemBlock(typ string, der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der})
}

func writeTar(files map[string][]byte) ([]byte, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for name, data := range files {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0600, Size: int64(len(data))}); err != nil {
			return nil, err
		}
		if _, err := tw.Write(data); err != nil {
			return nil, err
		}
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func readTar(data []byte) (map[string][]byte, error) {
	tr := tar.NewReader(bytes.NewReader(data))
	files := map[string][]byte{}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		body, err := io.ReadAll(tr)
		if err != nil {
			return nil, err
		}
		files[hdr.Name] = body
	}
	return files, nil
}

func parseCert(pemData []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(pemData)
	if block == nil {
		return nil, fmt.Errorf("missing certificate PEM")
	}
	return x509.ParseCertificate(block.Bytes)
}

func parseRSAKey(pemData []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(pemData)
	if block == nil {
		return nil, fmt.Errorf("missing RSA private key PEM")
	}
	return x509.ParsePKCS1PrivateKey(block.Bytes)
}
