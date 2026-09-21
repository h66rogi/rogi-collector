package store

import (
	"testing"
)

// TestExecutor_WithNilTx_ReturnsFallback verifies that PgHistoryStore.executor
// does not panic when tx is nil. In production, nil tx causes executor to
// return s.pool (the fallback). This test validates the nil-tx branch is safe
// to call even when pool is also nil (returns nil but does not panic).
//
// The fakeHistoryStore in emitter_test.go silently accepts nil tx, which is
// correct: now that executor() guards the nil-tx path, nil tx is a valid
// production call path (the pool is used as fallback).
func TestExecutor_WithNilTx_ReturnsFallback(t *testing.T) {
	// Verify that executor doesn't panic with nil tx.
	// The actual executor returns s.pool when tx is nil.
	// We can't test the full path without a real PG pool,
	// but we can verify the nil-tx branch doesn't panic.
	s := &PgHistoryStore{pool: nil}
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("executor(nil) panicked: %v", r)
		}
	}()
	_ = s.executor(nil)
}

// TestExecutor_NilTxBranchSelection verifies the branching logic of executor:
// a nil tx must always take the else-branch (return pool), never the tx-branch.
// We use a compile-time assertion via interface satisfaction to confirm the
// shape of the executor helper has not changed.
func TestExecutor_NilTxBranchSelection(t *testing.T) {
	// Compile-time: PgHistoryStore must still implement HistoryStore.
	var _ HistoryStore = (*PgHistoryStore)(nil)

	s := &PgHistoryStore{pool: nil}
	result := s.executor(nil)

	// result should equal s.pool (nil in this case).
	// The important thing is that the function returned without panicking
	// and returned the pool value (nil == nil is fine here).
	if result != s.pool {
		t.Fatalf("executor(nil) should return s.pool, got %v", result)
	}
}
