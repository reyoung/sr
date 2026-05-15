package sr

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestCLIGenKeyBundles(t *testing.T) {
	var serverOut bytes.Buffer
	if err := Run(t.Context(), []string{"gen-key", "--server"}, &serverOut, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	serverPath := filepath.Join(dir, "server.key")
	if err := os.WriteFile(serverPath, serverOut.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}

	var clientOut bytes.Buffer
	err := Run(t.Context(), []string{"gen-key", "--client", serverPath, "--label", "alice"}, &clientOut, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := readTar(clientOut.Bytes()); err != nil {
		t.Fatal(err)
	}
}
