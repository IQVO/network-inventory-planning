package kafka

import (
	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/propagation"
)

// headerCarrier adapts a kafka-go header slice to
// propagation.TextMapCarrier, so the W3C traceparent/tracestate the
// propagator emits rides along with the message — the same shape as
// inventory-storage's outbound carrier (ADR 0007): a downstream consumer
// that Extracts from the same headers gets a span parented to the publish
// span in this process. kafka-go has no built-in OTel instrumentation;
// this small adapter is what makes a trace span the broker.
//
// It holds a *pointer* to the slice because Set has to append when a key
// is not already present.
type headerCarrier struct {
	headers *[]kafkago.Header
}

var _ propagation.TextMapCarrier = headerCarrier{}

// Get returns the value of the first header with the given key, or "".
func (c headerCarrier) Get(key string) string {
	for _, h := range *c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// Set replaces the first header with the given key, or appends one.
// Replacing rather than appending keeps a re-published message from
// carrying two conflicting traceparents.
func (c headerCarrier) Set(key, value string) {
	for i := range *c.headers {
		if (*c.headers)[i].Key == key {
			(*c.headers)[i].Value = []byte(value)
			return
		}
	}
	*c.headers = append(*c.headers, kafkago.Header{Key: key, Value: []byte(value)})
}

// Keys lists every header key present.
func (c headerCarrier) Keys() []string {
	keys := make([]string, 0, len(*c.headers))
	for _, h := range *c.headers {
		keys = append(keys, h.Key)
	}
	return keys
}
