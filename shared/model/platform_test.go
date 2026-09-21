package model

import "testing"

func TestPlatformIsValid(t *testing.T) {
	valid := []Platform{PlatformChzzk, PlatformSoop, PlatformCime}
	for _, p := range valid {
		if !p.IsValid() {
			t.Fatalf("expected %s to be valid", p)
		}
	}

	if Platform("unknown").IsValid() {
		t.Fatal("expected unknown platform to be invalid")
	}
}
