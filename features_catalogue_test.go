package main_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/cucumber/godog"
)

// ------------------------------------- rebalance policy / lane catalogues --
//
// The catalogue Givens go through the PUBLIC REST API (PUT
// /v1/rebalance-policies, PUT /v1/transfer-lanes, ADR 0015), so a scenario
// that plans from the catalogue also proves the operator's write path.

func (w *world) putCatalogue(ctx context.Context, path, body string) error {
	status, out, _, err := w.call(ctx, request{server: w.oltp, method: http.MethodPut, path: path, body: []byte(body)})
	if err != nil {
		return err
	}
	if status != http.StatusCreated && status != http.StatusOK {
		return fmt.Errorf("PUT %s: status %d: %s", path, status, string(out))
	}
	return nil
}

func (w *world) theCatalogueHoldsPolicy(ctx context.Context, site, version, sku string, safety, target, priority int) error {
	return w.putCatalogue(ctx, "/v1/rebalance-policies", fmt.Sprintf(
		`{"Version":%q,"Site":%q,"SKU":%q,"SafetyStock":%d,"TargetStock":%d,"UnitPriority":%d}`,
		version, site, sku, safety, target, priority))
}

func (w *world) theCatalogueHoldsLane(ctx context.Context, state, origin, destination string, hours, cost int) error {
	return w.putCatalogue(ctx, "/v1/transfer-lanes", fmt.Sprintf(
		`{"Origin":%q,"Destination":%q,"LeadTime":%d,"UnitHandlingCost":%d,"Enabled":%t}`,
		origin, destination, int64(time.Duration(hours)*time.Hour), cost, state == "enabled"))
}

func (w *world) iPutBody(ctx context.Context, path string, body *godog.DocString) error {
	return w.record(ctx, request{server: w.oltp, method: http.MethodPut, path: path, body: []byte(body.Content)})
}

func (w *world) iPutBodyIfMatch(ctx context.Context, path, ifMatch string, body *godog.DocString) error {
	return w.record(ctx, request{
		server: w.oltp, method: http.MethodPut, path: path,
		headers: map[string]string{"If-Match": ifMatch},
		body:    []byte(body.Content),
	})
}

func (w *world) theResponseHeaderIs(name, expected string) error {
	if got := w.headers.Get(name); got != expected {
		return fmt.Errorf("expected header %s = %q, got %q", name, expected, got)
	}
	return nil
}

func (w *world) registerCatalogueSteps(sc *godog.ScenarioContext) {
	sc.Step(`^the policy catalogue holds, for site "([^"]*)", policy "([^"]*)" for SKU "([^"]*)" with safety stock (\d+), target stock (\d+) and unit priority (\d+)$`, w.theCatalogueHoldsPolicy)
	sc.Step(`^the lane catalogue holds an? (enabled|disabled) lane from "([^"]*)" to "([^"]*)" with lead time (\d+) hours and unit handling cost (\d+)$`, w.theCatalogueHoldsLane)
	sc.Step(`^I send PUT "([^"]*)" with this body:$`, w.iPutBody)
	sc.Step(`^I send PUT "([^"]*)" with If-Match (.+) and this body:$`, func(ctx context.Context, path, ifMatch string, body *godog.DocString) error {
		return w.iPutBodyIfMatch(ctx, path, strings.TrimSpace(ifMatch), body)
	})
	sc.Step(`^the response header "([^"]*)" is (.+)$`, func(name, expected string) error {
		return w.theResponseHeaderIs(name, strings.TrimSpace(expected))
	})
}
