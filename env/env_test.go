package env

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEnvironmentWithoutDotenvIsNotAnError(t *testing.T) {
	t.Chdir(t.TempDir()) // no .env here, like in a container
	if err := Environment(); err != nil {
		t.Fatalf("Environment() = %v, want nil when .env is missing", err)
	}
}

func TestEnvironmentLoadsDotenvButRealEnvironmentWins(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ".env"), []byte("FAST_TEST_FROM_FILE=file\nFAST_TEST_BOTH=file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)
	t.Setenv("FAST_TEST_BOTH", "env")
	os.Unsetenv("FAST_TEST_FROM_FILE")
	t.Cleanup(func() { os.Unsetenv("FAST_TEST_FROM_FILE") })

	if err := Environment(); err != nil {
		t.Fatalf("Environment() = %v", err)
	}
	if got := os.Getenv("FAST_TEST_FROM_FILE"); got != "file" {
		t.Errorf("FAST_TEST_FROM_FILE = %q, want file", got)
	}
	if got := os.Getenv("FAST_TEST_BOTH"); got != "env" {
		t.Errorf("FAST_TEST_BOTH = %q, want env (the real environment wins)", got)
	}
}

func TestEnvironmentMalformedDotenvIsStillAnError(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ".env"), []byte("BROKEN LINE WITHOUT EQUALS\n=\n\"unterminated\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)
	if err := Environment(); err == nil {
		t.Fatal("Environment() = nil, want an error for a malformed .env")
	}
}

func TestEnvironmentNamedFileMissingIsNotAnError(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("FAST_TEST_ENV_FILE", filepath.Join(t.TempDir(), "nope.env"))
	if err := Environment("FAST_TEST_ENV_FILE"); err != nil {
		t.Fatalf("Environment(named) = %v, want nil when the file does not exist", err)
	}
}
