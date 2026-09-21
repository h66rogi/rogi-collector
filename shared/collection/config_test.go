package collection

import "testing"

func TestSingleChannelAndExplicitAnonymousTest(t *testing.T) {
	for _, s := range []string{"soop:fixture,soop:other", "chzzk:fixture", "soop:"} {
		if _, e := Channel(s); e == nil {
			t.Fatal("scope widened", s)
		}
	}
	if got, e := Channel(""); e != nil || got != "" {
		t.Fatal("missing scope enabled collection")
	}
	for _, mode := range []string{"", "cookie"} {
		if anonymous, e := AnonymousTest(mode); e != nil || anonymous {
			t.Fatal("cookie mode weakened")
		}
	}
	if anonymous, e := AnonymousTest("anonymous-test"); e != nil || !anonymous {
		t.Fatal("explicit public test unavailable")
	}
	if _, e := AnonymousTest("anonymous"); e == nil {
		t.Fatal("unknown auth mode accepted")
	}
}
