package archive

import "testing"

func TestR2RequiresAccountAndScopedCredentials(t *testing.T) {
	account := "0123456789abcdef0123456789abcdef"
	for _, entry := range []struct {
		account, bucket, key, secret string
	}{
		{"", "archive", "key", "secret"},
		{"not-an-account", "archive", "key", "secret"},
		{account, "", "key", "secret"},
		{account, "archive", "", "secret"},
		{account, "archive", "key", ""},
	} {
		if _, err := NewR2ObjectStore(entry.account, entry.bucket, entry.key, entry.secret); err == nil {
			t.Fatalf("accepted incomplete R2 configuration: %#v", entry)
		}
	}
	if _, err := NewR2ObjectStore(account, "archive", "key", "secret"); err != nil {
		t.Fatal(err)
	}
}
