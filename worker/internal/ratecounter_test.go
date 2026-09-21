package internal

import (
	"testing"
	"time"
)

func TestRateCounter_InitiallyZero(t *testing.T) {
	rc := NewRateCounter()
	if got := rc.Rate(); got != 0 {
		t.Errorf("expected 0, got %f", got)
	}
}

func TestRateCounter_CountsMessages(t *testing.T) {
	rc := NewRateCounter()
	rc.Add(100)
	time.Sleep(100 * time.Millisecond)
	rc.snapshot()
	rate := rc.Rate()
	if rate <= 0 {
		t.Errorf("expected positive rate, got %f", rate)
	}
}

func TestRateCounter_PerKeyTracking(t *testing.T) {
	rc := NewRateCounter()
	rc.AddForKey("soop", 50)
	rc.AddForKey("chzzk", 30)

	time.Sleep(100 * time.Millisecond)
	rc.snapshot()

	soop := rc.RateForKey("soop")
	chzzk := rc.RateForKey("chzzk")
	if soop <= 0 || chzzk <= 0 {
		t.Errorf("expected positive rates, soop=%f chzzk=%f", soop, chzzk)
	}
	if soop <= chzzk {
		t.Errorf("expected soop > chzzk, soop=%f chzzk=%f", soop, chzzk)
	}
}
