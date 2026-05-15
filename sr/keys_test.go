package sr

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGenerateKeyBundlesLoadTLS(t *testing.T) {
	serverBundle, err := GenerateServerBundle()
	if err != nil {
		t.Fatal(err)
	}
	clientBundle, err := GenerateClientBundle(serverBundle, "alice")
	if err != nil {
		t.Fatal(err)
	}

	serverFiles, err := readTar(serverBundle)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{bundleCACert, bundleCAKey, bundleServerCert, bundleServerKey} {
		if len(serverFiles[name]) == 0 {
			t.Fatalf("server bundle missing %s", name)
		}
	}
	clientFiles, err := readTar(clientBundle)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{bundleCACert, bundleClientCert, bundleClientKey} {
		if len(clientFiles[name]) == 0 {
			t.Fatalf("client bundle missing %s", name)
		}
	}

	dir := t.TempDir()
	serverPath := filepath.Join(dir, "server.key")
	clientPath := filepath.Join(dir, "client.key")
	if err := os.WriteFile(serverPath, serverBundle, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(clientPath, clientBundle, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadServerTLSConfig(serverPath); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadClientTLSConfig(clientPath); err != nil {
		t.Fatal(err)
	}
}
