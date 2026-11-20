// The catalogue-level replay-class checks that need no store (ADR 0024): a
// problem type without a class fails here, the ADR's class table equals the
// catalogue in both directions, and the catalogue is the only place a
// problem type is declared. The per-class behaviour over a real Postgres
// lives in the integration-tagged replay-class tests.
package http_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	httpadapter "github.com/claudioed/network-inventory-planning/internal/adapters/inbound/http"
)

// declaresAReplayClass reports whether c is one of the three replay classes.
func declaresAReplayClass(c string) bool {
	return c == "FINAL" || c == "OUTCOME" || c == "RETRYABLE"
}

// Every problem type the adapter can answer declares exactly one replay
// class; there is no runtime default. A 5xx is never stored, so its class is
// RETRYABLE by definition. Every (slug, status) pair appears once.
func TestTheCatalogueDeclaresAReplayClassForEveryProblemType(t *testing.T) {
	entries := httpadapter.ReplayCatalogue()
	if len(entries) == 0 {
		t.Fatal("the catalogue accessor returned no problem types: the test would pass on nothing")
	}
	seen := map[string]bool{}
	for _, e := range entries {
		t.Run(e.Slug+"/"+strconv.Itoa(e.Status), func(t *testing.T) {
			if !declaresAReplayClass(e.Class) {
				t.Errorf("problem type %q (%d) declares no replay class (%q): set FINAL, OUTCOME or RETRYABLE on its catalogue entry (.claude/rules/fleet/idempotency-and-outbox.md)", e.Slug, e.Status, e.Class)
			}
			if e.Status >= 500 && e.Class != "RETRYABLE" {
				t.Errorf("problem type %q answers %d: a 5xx is never stored, so its class is RETRYABLE, got %q", e.Slug, e.Status, e.Class)
			}
			id := e.Slug + "/" + strconv.Itoa(e.Status)
			if seen[id] {
				t.Errorf("problem type %q (%d) appears twice in the catalogue", e.Slug, e.Status)
			}
			seen[id] = true
		})
	}
}

var adrClassRow = regexp.MustCompile(`^\| ` + "`([a-z0-9-]+)`" + ` \| (\d{3}) \| (FINAL|OUTCOME|RETRYABLE) \|`)

// ADR 0024 carries the class of every problem type with its status and
// reason; the table and the catalogue must say the same, in both directions.
func TestTheADRDeclaresTheClassOfEveryProblem(t *testing.T) {
	paths, err := filepath.Glob("../../../../docs/docs/adr/0024-*.md")
	if err != nil || len(paths) != 1 {
		t.Fatalf("ADR 0024 not found (%v, %v)", paths, err)
	}
	raw, err := os.ReadFile(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	inADR := map[string]string{}
	for _, line := range regexp.MustCompile(`\r?\n`).Split(string(raw), -1) {
		if m := adrClassRow.FindStringSubmatch(line); m != nil {
			inADR[m[1]+"/"+m[2]] = m[3]
		}
	}
	if len(inADR) == 0 {
		t.Fatal("ADR 0024 carries no class table rows")
	}
	seen := map[string]bool{}
	for _, row := range httpadapter.ReplayCatalogue() {
		id := row.Slug + "/" + strconv.Itoa(row.Status)
		if got := inADR[id]; got != row.Class {
			t.Errorf("problem type %q (%d): the ADR says %q, the catalogue %q", row.Slug, row.Status, got, row.Class)
		}
		seen[id] = true
	}
	for id, class := range inADR {
		if !seen[id] {
			t.Errorf("the ADR classifies %q as %s but the catalogue has no such problem type", id, class)
		}
	}
}

// The catalogue file is the only place a problem type is declared: a literal
// elsewhere would carry no class and be silently released as unclassified.
func TestProblemTypesAreDeclaredOnlyInTheCatalogue(t *testing.T) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, lit := range compositeLiteralsOf(fset, file, "problemType") {
			if name != "problems.go" {
				t.Errorf("%s declares a problemType literal (%s): declare it on the catalogue in problems.go instead", name, lit)
			}
		}
	}
}

// compositeLiteralsOf returns the position strings of the composite literals
// of the named local type in file.
func compositeLiteralsOf(fset *token.FileSet, file *ast.File, typeName string) []string {
	var out []string
	ast.Inspect(file, func(n ast.Node) bool {
		lit, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		if ident, ok := lit.Type.(*ast.Ident); ok && ident.Name == typeName {
			out = append(out, fset.Position(lit.Pos()).String())
		}
		return true
	})
	return out
}
