package runtimeenv

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMountedRoleEnvironmentIsData(t *testing.T) {
	file := filepath.Join(t.TempDir(), "role.env")
	if err := os.WriteFile(file, []byte("ROGI_FIXTURE_VALUE=$(not-a-command)\nROGI_FIXTURE_OVERRIDE=file\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ROLE_ENV_FILE", file)
	t.Setenv("ROGI_FIXTURE_OVERRIDE", "explicit")
	t.Cleanup(func() { os.Unsetenv("ROGI_FIXTURE_VALUE") })
	if err := Load(); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("ROGI_FIXTURE_VALUE") != "$(not-a-command)" || os.Getenv("ROGI_FIXTURE_OVERRIDE") != "explicit" {
		t.Fatal("file executed or explicit setting overridden")
	}
}
func TestInvalidRoleEnvironmentDoesNotExposeValues(t *testing.T) {
	file := filepath.Join(t.TempDir(), "role.env")
	os.WriteFile(file, []byte("invalid-key=synthetic-sensitive-value"), 0600)
	t.Setenv("ROLE_ENV_FILE", file)
	if err := Load(); err == nil || err.Error() != "invalid role environment assignment" {
		t.Fatal("unsafe parser result")
	}
}
