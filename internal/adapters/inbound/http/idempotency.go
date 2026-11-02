package http

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"

	"github.com/claudioed/network-inventory-planning/internal/application/contract"
)

// Idempotency (fleet REST guideline section 2): every POST route sits behind
// Handler.idempotent. The key alone identifies the record; the fingerprint
// is method + path + sha256(body). Same key and fingerprint replays the
// stored answer with Idempotency-Replayed: true; a different fingerprint is
// 409 idempotency-key-reused; 2xx and 4xx answers are stored, a 5xx or a
// panic rolls back and is never stored. The store runs the handler inside
// the transaction that holds the key row, so the key, the aggregate change
// and the outbox rows commit together.
const (
	idempotencyKeyHeader      = "Idempotency-Key"
	idempotencyReplayedHeader = "Idempotency-Replayed"
	maxIdempotencyKeyLen      = 128
	maxIdempotentBody         = 1 << 20
)

// idempotent returns the route-scoped Idempotency-Key middleware.
func (h Handler) idempotent() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if h.Idempotency == nil {
				writeProblem(w, http.StatusServiceUnavailable, "idempotency-unavailable",
					"Idempotency store is not configured", "this instance cannot accept keyed writes without its idempotency store")
				return
			}
			key, ok := checkIdempotencyKey(w, r)
			if !ok {
				return
			}
			body, err := io.ReadAll(io.LimitReader(r.Body, maxIdempotentBody+1))
			if err != nil || len(body) > maxIdempotentBody {
				writeProblem(w, http.StatusBadRequest, "invalid-request", "Invalid request", "the request body could not be read or is too large")
				return
			}
			sum := sha256.Sum256(body)
			fingerprint := r.Method + " " + r.URL.EscapedPath() + " " + hex.EncodeToString(sum[:])

			var rec *bufferedResponse
			resp, replayed, err := h.Idempotency.Do(r.Context(), key, fingerprint, func(ctx context.Context) (contract.StoredResponse, error) {
				rec = &bufferedResponse{header: http.Header{}, status: http.StatusOK}
				req := r.Clone(ctx)
				req.Body = io.NopCloser(bytes.NewReader(body))
				next.ServeHTTP(rec, req)
				return contract.StoredResponse{
					Status:      rec.status,
					ContentType: rec.header.Get("Content-Type"),
					Location:    rec.header.Get("Location"),
					Body:        rec.body.Bytes(),
				}, nil
			})
			switch {
			case errors.Is(err, contract.ErrIdempotencyKeyReused):
				writeProblem(w, http.StatusConflict, "idempotency-key-reused",
					"Idempotency-Key was already used with a different request", "send a new key for a new logical request")
			case err != nil:
				writeProblem(w, http.StatusServiceUnavailable, "idempotency-unavailable",
					"Idempotency store could not complete the request", err.Error())
			case replayed:
				w.Header().Set(idempotencyReplayedHeader, "true")
				writeStored(w, resp)
			default:
				for k, v := range rec.header {
					w.Header()[k] = v
				}
				w.WriteHeader(resp.Status)
				_, _ = w.Write(resp.Body)
			}
		})
	}
}

func checkIdempotencyKey(w http.ResponseWriter, r *http.Request) (string, bool) {
	vals := r.Header.Values(idempotencyKeyHeader)
	if len(vals) == 0 || vals[0] == "" {
		writeProblem(w, http.StatusBadRequest, "idempotency-key-required",
			"Idempotency-Key header is required", "send a client-generated Idempotency-Key (a UUID per logical submit) on this operation")
		return "", false
	}
	if len(vals) > 1 || !validIdempotencyKey(vals[0]) {
		writeProblem(w, http.StatusBadRequest, "idempotency-key-invalid",
			"Idempotency-Key must be 1-128 printable ASCII characters", "send exactly one Idempotency-Key of 1-128 printable ASCII characters")
		return "", false
	}
	return vals[0], true
}

func validIdempotencyKey(key string) bool {
	if len(key) > maxIdempotencyKeyLen {
		return false
	}
	for i := 0; i < len(key); i++ {
		if key[i] < 0x21 || key[i] > 0x7e {
			return false
		}
	}
	return true
}

func writeStored(w http.ResponseWriter, s contract.StoredResponse) {
	if s.ContentType != "" {
		w.Header().Set("Content-Type", s.ContentType)
	}
	if s.Location != "" {
		w.Header().Set("Location", s.Location)
	}
	w.WriteHeader(s.Status)
	_, _ = w.Write(s.Body)
}

// bufferedResponse captures the handler's answer so it can be stored before
// it is sent.
type bufferedResponse struct {
	header      http.Header
	status      int
	wroteHeader bool
	body        bytes.Buffer
}

func (b *bufferedResponse) Header() http.Header { return b.header }

func (b *bufferedResponse) WriteHeader(status int) {
	if !b.wroteHeader {
		b.status, b.wroteHeader = status, true
	}
}

func (b *bufferedResponse) Write(p []byte) (int, error) {
	b.WriteHeader(http.StatusOK)
	return b.body.Write(p)
}
