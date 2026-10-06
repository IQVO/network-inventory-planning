package cloudevents

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// These tests are generic over the per-repo constants, so they pass
// unchanged in every instantiated repo. Each repo ALSO needs a golden
// exact-JSON test per published `type` next to its publisher (fleet
// Event Standard §6) — this file only proves the helper itself.

type samplePayload struct {
	Ref string `json:"ref"`
	N   int    `json:"n"`
}

func sampleSpec() Spec {
	return Spec{
		ID:        "6f1c2b7e-4c3a-4a1d-9f0b-6c2b8a7d1e33",
		Entity:    "sample",
		EventName: "SampleHappened",
		Subject:   "agg-1",
		Time:      time.Date(2026, 9, 30, 12, 0, 0, 0, time.FixedZone("BRT", -3*3600)),
		Stream:    StreamEvents,
		Version:   1,
		Data:      samplePayload{Ref: "r-1", N: 3},
	}
}

func TestNewProducesStructuredCloudEvent(t *testing.T) {
	b, err := New(sampleSpec())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	want := map[string]any{
		"specversion":     "1.0",
		"id":              "6f1c2b7e-4c3a-4a1d-9f0b-6c2b8a7d1e33",
		"source":          Source,
		"type":            "com.warehouse." + Subdomain + "." + Context + ".sample.SampleHappened",
		"subject":         "agg-1",
		"time":            "2026-09-30T15:00:00Z",
		"datacontenttype": "application/json",
		"dataschema":      "urn:warehouse:" + Repo + ":events:SampleHappened:v1",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %v, want %v", k, got[k], v)
		}
	}
	data, ok := got["data"].(map[string]any)
	if !ok || data["ref"] != "r-1" || data["n"] != float64(3) {
		t.Errorf("data = %v, want {ref:r-1 n:3}", got["data"])
	}
	for _, legacy := range []string{"event_id", "event_type", "occurred_at", "schema_version"} {
		if _, present := got[legacy]; present {
			t.Errorf("legacy flat-envelope field %q present", legacy)
		}
	}
}

func TestNewRejectsEmptySubject(t *testing.T) {
	s := sampleSpec()
	s.Subject = ""
	if _, err := New(s); err == nil {
		t.Fatal("New with empty subject: want error, got nil")
	}
}

func TestContentTypeHeader(t *testing.T) {
	h := ContentTypeHeader()
	if h.Key != "content-type" || string(h.Value) != "application/cloudevents+json; charset=UTF-8" {
		t.Errorf("header = %s: %s", h.Key, h.Value)
	}
}

func TestDecodeRoundTrip(t *testing.T) {
	b, err := New(sampleSpec())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	e, err := Decode(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if e.ID() != "6f1c2b7e-4c3a-4a1d-9f0b-6c2b8a7d1e33" || e.Subject() != "agg-1" {
		t.Errorf("id/subject = %s/%s", e.ID(), e.Subject())
	}
	var p samplePayload
	if err := e.DataAs(&p); err != nil || p.Ref != "r-1" || p.N != 3 {
		t.Errorf("DataAs = %+v, %v", p, err)
	}
}

func TestDecodeRejectsLegacyFlatEnvelope(t *testing.T) {
	legacy := []byte(`{"event_id":"x","event_type":"SampleHappened","occurred_at":"2026-09-30T15:00:00Z","source":"svc","data":{}}`)
	if _, err := Decode(legacy); !errors.Is(err, ErrNotCloudEvent) {
		t.Fatalf("Decode(legacy) err = %v, want ErrNotCloudEvent", err)
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	if _, err := Decode([]byte("not json")); !errors.Is(err, ErrNotCloudEvent) {
		t.Fatalf("Decode(garbage) err = %v, want ErrNotCloudEvent", err)
	}
}
