package http

import (
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5/middleware"
)

// sanitizeForLog strips CR/LF from an attacker-controlled value (here, the
// raw request path) before it is written to a log line. Without this, a
// crafted path segment containing an encoded newline could forge a fake log
// entry that appears to be a separate, legitimate line (CWE-117 log
// injection) once the log record reaches a downstream viewer/aggregator
// that doesn't preserve slog's JSON string escaping.
func sanitizeForLog(s string) string {
	return strings.NewReplacer("\n", "", "\r", "").Replace(s)
}

// RequestLogger returns chi middleware that logs each request at Info level
// (Error for 5xx responses) with method, path, status, duration, response
// size, and the chi request ID. It runs AFTER otelchi (ADR-0013 Tier 1): by
// the time a line is logged, the request context already carries a span, so
// a telemetry-aware slog handler can stamp trace_id/span_id onto it.
func RequestLogger(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			start := time.Now()

			next.ServeHTTP(ww, r)

			duration := time.Since(start)
			attrs := []any{
				"method", r.Method,
				"path", sanitizeForLog(r.URL.Path),
				"status", ww.Status(),
				"duration_ms", duration.Milliseconds(),
				"bytes", ww.BytesWritten(),
				"request_id", middleware.GetReqID(r.Context()),
			}
			if ww.Status() >= http.StatusInternalServerError {
				logger.Error("http request", attrs...)
				return
			}
			logger.Info("http request", attrs...)
		})
	}
}
