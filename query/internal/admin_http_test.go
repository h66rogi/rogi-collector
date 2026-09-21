package internal

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAdminAuthorize(t *testing.T) {
	t.Parallel()

	handler := &collectionOptOutAdminHandler{apiKey: "test-key"}
	request := httptest.NewRequest(http.MethodGet, "/admin/collection-opt-outs", nil)
	request.Header.Set("X-Internal-Api-Key", "test-key")
	if !handler.authorize(httptest.NewRecorder(), request) {
		t.Fatal("expected matching key to authorize")
	}

	request.Header.Set("X-Internal-Api-Key", "wrong")
	recorder := httptest.NewRecorder()
	if handler.authorize(recorder, request) {
		t.Fatal("expected wrong key to be rejected")
	}
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusUnauthorized)
	}
}

func TestDecodeAdminJSONLimitsAndStrictness(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantOK     bool
		wantStatus int
	}{
		{name: "valid", body: `{"title":"example","items":[]}`, wantOK: true, wantStatus: http.StatusOK},
		{name: "unknown field", body: `{"unknown":true}`, wantStatus: http.StatusBadRequest},
		{name: "trailing object", body: `{} {}`, wantStatus: http.StatusBadRequest},
		{name: "too large", body: `"` + strings.Repeat("x", maxAdminRequestBytes) + `"`, wantStatus: http.StatusRequestEntityTooLarge},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			recorder := httptest.NewRecorder()
			var payload createCollectionOptOutBatchRequest
			if got := decodeAdminJSON(recorder, request, &payload); got != tt.wantOK {
				t.Fatalf("decodeAdminJSON() = %v, want %v", got, tt.wantOK)
			}
			if recorder.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", recorder.Code, tt.wantStatus)
			}
		})
	}
}

func TestIntQueryBounds(t *testing.T) {
	t.Parallel()

	request := httptest.NewRequest(http.MethodGet, "/?limit=501", nil)
	if got := intQuery(request, "limit", 100, 1, 500); got != 100 {
		t.Fatalf("intQuery() = %d, want fallback 100", got)
	}
}
