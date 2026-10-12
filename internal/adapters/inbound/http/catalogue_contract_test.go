package http

import (
	"encoding/json"
	"net/http"
	"testing"
)

// TestCatalogueResponsesMatchTheOpenAPISchema keeps the D6 catalogue
// endpoints honest against apis/openapi.yaml: the PUT entry, the list
// envelope and the embedded Policy / Lane (the EXISTING schemas the
// generate endpoint uses) must carry exactly the spec's properties.
func TestCatalogueResponsesMatchTheOpenAPISchema(t *testing.T) {
	h := catalogueHandler().Routes()
	schemas := openAPISchemas(t)

	check := func(name string, body map[string]any) {
		t.Helper()
		props, required, _ := schemaKeys(t, schemas, name)
		if got := jsonKeys(body); !equalStrings(got, props) {
			t.Fatalf("%s: response keys %v != schema properties %v", name, got, props)
		}
		for _, r := range required {
			if _, ok := body[r]; !ok {
				t.Errorf("%s: required property %q missing", name, r)
			}
		}
	}
	decode := func(raw []byte) map[string]any {
		t.Helper()
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		return m
	}

	put := do(t, h, http.MethodPut, "/v1/rebalance-policies", "", policyBody)
	entry := decode(put.Body.Bytes())
	check("RebalancePolicyEntry", entry)
	check("Policy", entry["policy"].(map[string]any))

	list := decode(do(t, h, http.MethodGet, "/v1/rebalance-policies", "", "").Body.Bytes())
	check("RebalancePolicyList", list)
	listed := list["policies"].([]any)[0].(map[string]any)
	delete(entry, "outcome")
	if got, want := jsonKeys(listed), jsonKeys(entry); !equalStrings(got, want) {
		t.Fatalf("listed entry keys %v != PUT entry keys without outcome %v", got, want)
	}

	lanePut := do(t, h, http.MethodPut, "/v1/transfer-lanes", "",
		`{"Origin":"WH1","Destination":"WH2","LeadTime":3600000000000,"UnitHandlingCost":1,"Enabled":true}`)
	laneEntry := decode(lanePut.Body.Bytes())
	check("TransferLaneEntry", laneEntry)
	check("Lane", laneEntry["lane"].(map[string]any))
	check("TransferLaneList", decode(do(t, h, http.MethodGet, "/v1/transfer-lanes", "", "").Body.Bytes()))
}
