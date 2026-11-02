package http

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/claudioed/network-inventory-planning/internal/application/contract"
	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
)

// fakeIdempotency is a lock-serialised in-memory ports.IdempotencyStore with
// the same contract as the Postgres adapter (global key, fingerprint compare,
// 5xx never stored).
type fakeIdempotency struct {
	mu      sync.Mutex
	records map[string]fakeRecord
}

type fakeRecord struct {
	fingerprint string
	resp        contract.StoredResponse
}

func newFakeIdempotency() *fakeIdempotency {
	return &fakeIdempotency{records: map[string]fakeRecord{}}
}

func (s *fakeIdempotency) Do(ctx context.Context, key, fingerprint string, fn func(ctx context.Context) (contract.StoredResponse, error)) (contract.StoredResponse, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec, ok := s.records[key]; ok {
		if rec.fingerprint != fingerprint {
			return contract.StoredResponse{}, false, contract.ErrIdempotencyKeyReused
		}
		return rec.resp, true, nil
	}
	resp, err := fn(ctx)
	if err != nil {
		return contract.StoredResponse{}, false, err
	}
	if resp.Status < 500 {
		s.records[key] = fakeRecord{fingerprint: fingerprint, resp: resp}
	}
	return resp, false, nil
}

const validRunBody = `{"asOf":"2026-10-05T10:00:00Z","positions":[],"policies":[],"lanes":[]}`

func keyedHandler() (Handler, *int32) {
	var executions int32
	h := Handler{Generate: usecases.GenerateTransferProposals{}, Idempotency: newFakeIdempotency()}
	return h, &executions
}

func postRun(h Handler, path, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body))
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	rec := httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, req)
	return rec
}

func TestKeyedRouteRejectsMissingAndInvalidKeys(t *testing.T) {
	h, _ := keyedHandler()
	for _, tc := range []struct {
		name, key, want string
	}{
		{"missing", "", "idempotency-key-required"},
		{"too long", strings.Repeat("k", 129), "idempotency-key-invalid"},
		{"space is not printable ASCII graphic", "has space", "idempotency-key-invalid"},
		{"non ASCII", "clé", "idempotency-key-invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := postRun(h, "/v1/transfer-proposal-runs", tc.key, validRunBody)
			if rec.Code != http.StatusBadRequest || problemType(t, rec) != tc.want {
				t.Fatalf("status = %d problem = %s, want 400 %s", rec.Code, rec.Body.String(), tc.want)
			}
		})
	}
	if rec := postRun(h, "/v1/transfer-proposal-runs", strings.Repeat("k", 128), validRunBody); rec.Code != http.StatusOK {
		t.Fatalf("a 128-char key must be accepted, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestKeyedRouteReplaysByteIdenticalWithHeaderAndRejectsReuse(t *testing.T) {
	h, _ := keyedHandler()
	first := postRun(h, "/v1/transfer-proposal-runs", "k-1", validRunBody)
	if first.Code != http.StatusOK || first.Header().Get("Idempotency-Replayed") != "" {
		t.Fatalf("first = %d replayed=%q", first.Code, first.Header().Get("Idempotency-Replayed"))
	}
	replay := postRun(h, "/v1/transfer-proposal-runs", "k-1", validRunBody)
	if replay.Code != first.Code || replay.Body.String() != first.Body.String() ||
		replay.Header().Get("Content-Type") != first.Header().Get("Content-Type") {
		t.Fatalf("replay differs: %d %q vs %d %q", replay.Code, replay.Body.String(), first.Code, first.Body.String())
	}
	if replay.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("Idempotency-Replayed = %q, want true", replay.Header().Get("Idempotency-Replayed"))
	}
	if got := replay.Header().Get("Idempotent-Replayed"); got != "" {
		t.Fatalf("retired header Idempotent-Replayed present: %q", got)
	}

	diffBody := postRun(h, "/v1/transfer-proposal-runs", "k-1", `{"asOf":"2026-10-05T11:00:00Z","positions":[],"policies":[],"lanes":[]}`)
	if diffBody.Code != http.StatusConflict || problemType(t, diffBody) != "idempotency-key-reused" {
		t.Fatalf("different body = %d %s, want 409 idempotency-key-reused", diffBody.Code, diffBody.Body.String())
	}
	// The key is GLOBAL: the same key on another route is a reuse, not a new key.
	otherRoute := postRun(h, "/v1/transfer-approvals", "k-1", validRunBody)
	if otherRoute.Code != http.StatusConflict || problemType(t, otherRoute) != "idempotency-key-reused" {
		t.Fatalf("other route = %d %s, want 409 idempotency-key-reused", otherRoute.Code, otherRoute.Body.String())
	}
}

func TestKeyedRouteCachesA4xxAndNeverStoresA5xx(t *testing.T) {
	h, _ := keyedHandler()
	bad := postRun(h, "/v1/transfer-proposal-runs", "k-4xx", `{"positions":[]}`)
	again := postRun(h, "/v1/transfer-proposal-runs", "k-4xx", `{"positions":[]}`)
	if bad.Code != http.StatusUnprocessableEntity || again.Code != http.StatusUnprocessableEntity ||
		again.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("4xx replay: %d then %d replayed=%q", bad.Code, again.Code, again.Header().Get("Idempotency-Replayed"))
	}

	// Unconfigured approval answers 503: it must re-execute on retry (never a replay).
	first := postRun(h, "/v1/transfer-approvals", "k-5xx", `{}`)
	second := postRun(h, "/v1/transfer-approvals", "k-5xx", `{}`)
	if first.Code != http.StatusServiceUnavailable || second.Code != http.StatusServiceUnavailable ||
		second.Header().Get("Idempotency-Replayed") != "" {
		t.Fatalf("5xx: %d then %d replayed=%q", first.Code, second.Code, second.Header().Get("Idempotency-Replayed"))
	}
}

func TestKeyedRouteConcurrentSameKeyExecutesOnce(t *testing.T) {
	var executions int32
	store := newFakeIdempotency()
	counting := countingStore{inner: store, executions: &executions}
	h := Handler{Generate: usecases.GenerateTransferProposals{}, Idempotency: counting}
	var wg sync.WaitGroup
	codes := make([]int, 12)
	for i := range codes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			codes[i] = postRun(h, "/v1/transfer-proposal-runs", "k-race", validRunBody).Code
		}()
	}
	wg.Wait()
	for _, c := range codes {
		if c != http.StatusOK {
			t.Fatalf("codes = %v, want all 200", codes)
		}
	}
	if got := atomic.LoadInt32(&executions); got != 1 {
		t.Fatalf("handler executed %d times for one key, want 1", got)
	}
}

// countingStore counts how often the wrapped handler actually ran.
type countingStore struct {
	inner      *fakeIdempotency
	executions *int32
}

func (c countingStore) Do(ctx context.Context, key, fp string, fn func(ctx context.Context) (contract.StoredResponse, error)) (contract.StoredResponse, bool, error) {
	return c.inner.Do(ctx, key, fp, func(ctx context.Context) (contract.StoredResponse, error) {
		atomic.AddInt32(c.executions, 1)
		return fn(ctx)
	})
}

func TestKeyedRoutesFailClosedWithoutAStore(t *testing.T) {
	rec := postRun(Handler{Generate: usecases.GenerateTransferProposals{}}, "/v1/transfer-proposal-runs", "k", validRunBody)
	if rec.Code != http.StatusServiceUnavailable || problemType(t, rec) != "idempotency-unavailable" {
		t.Fatalf("no store = %d %s", rec.Code, rec.Body.String())
	}
}

func TestRetiredRoutesAreGone(t *testing.T) {
	h, _ := keyedHandler()
	for _, tc := range []struct{ method, path string }{
		{http.MethodPost, "/v1/transfer-proposals:generate"},
		{http.MethodPost, "/v1/transfers:approve"},
	} {
		req := httptest.NewRequest(tc.method, tc.path, bytes.NewBufferString(validRunBody))
		req.Header.Set("Idempotency-Key", "k-retired")
		rec := httptest.NewRecorder()
		h.Routes().ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound && rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s %s = %d, want 404 or 405", tc.method, tc.path, rec.Code)
		}
	}
}
