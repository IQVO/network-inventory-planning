package architecture

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	archgo "github.com/arch-go/arch-go/api"
	"github.com/arch-go/arch-go/api/configuration"
)

// TestA2AAdapterDependencyRule encodes ADR-0020: the A2A inbound adapter is
// additive, never load-bearing for the OLTP composition root. Like the MCP
// adapter it may depend only on the application and domain layers (never on
// outbound adapters, the REST adapter or cmd), and nothing else in this
// codebase may depend on it: cmd/a2a wires it up and nothing else touches it.
func TestA2AAdapterDependencyRule(t *testing.T) {
	moduleInfo := configuration.Load(modulePath)

	t.Run("a2a adapter depends only on application and domain", func(t *testing.T) {
		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{
				{
					Package: "**.internal.adapters.inbound.a2a.**",
					ShouldOnlyDependsOn: &configuration.Dependencies{
						Internal: []string{
							"**.internal.adapters.inbound.a2a.**",
							"**.internal.application.**",
							"**.internal.domain.**",
						},
					},
				},
			},
		})

		assertArchGoPasses(t, result)
	})

	t.Run("nothing else depends on the a2a adapter", func(t *testing.T) {
		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{
				{
					Package: "**.internal.**",
					ShouldNotDependsOn: &configuration.Dependencies{
						Internal: []string{"**.internal.adapters.inbound.a2a.**"},
					},
				},
			},
		})

		assertArchGoPasses(t, result)
	})
}

// importsOf returns the import paths of one Go source file.
func importsOf(t *testing.T, path string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	var imports []string
	for _, spec := range file.Imports {
		p, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			t.Fatalf("%s: bad import %s", path, spec.Path.Value)
		}
		imports = append(imports, p)
	}
	return imports
}

// TestNoOutboundA2AClient: this context only SERVES A2A. Delegating onward to
// a sibling context is a separate decision (an outbound client in a
// consuming context, not here), so no non-test source may import the SDK's
// client package. Tests drive the wire through it, which is the only
// legitimate use.
func TestNoOutboundA2AClient(t *testing.T) {
	const clientPkg = "github.com/a2aproject/a2a-go/v2/a2aclient"
	for _, root := range []string{"../../internal", "../../cmd"} {
		for _, path := range goFilesUnder(t, root, false) {
			for _, imp := range importsOf(t, path) {
				if imp == clientPkg || strings.HasPrefix(imp, clientPkg+"/") {
					t.Errorf("%s imports %s: this context is an A2A server only, no outbound A2A client", path, imp)
				}
			}
		}
	}
}

// TestA2ADepsCarryNoWriteUseCase backs the write gate: the MCP surface is
// read-only (ADR-0008), so the A2A agent is read-only too. Its Deps may carry
// only the two read use cases the skills need; adding the approve use case (or
// any other) fails here until the ADR and the MCP charter change first.
func TestA2ADepsCarryNoWriteUseCase(t *testing.T) {
	want := []string{"*usecases.FindStuckTransfers", "*usecases.GetTransfer"}

	file, err := parser.ParseFile(token.NewFileSet(), "../adapters/inbound/a2a/server.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	found := false
	ast.Inspect(file, func(n ast.Node) bool {
		spec, ok := n.(*ast.TypeSpec)
		if !ok || spec.Name.Name != "Deps" {
			return true
		}
		found = true
		for _, field := range spec.Type.(*ast.StructType).Fields.List {
			got = append(got, typeString(field.Type))
		}
		return false
	})
	if !found {
		t.Fatal("type Deps not found in internal/adapters/inbound/a2a/server.go")
	}
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("a2a Deps field types = %v, want exactly %v (read-only agent: no write use case)", got, want)
	}
}

// typeString renders a field type expression like *usecases.GetTransfer.
func typeString(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.StarExpr:
		return "*" + typeString(e.X)
	case *ast.SelectorExpr:
		return typeString(e.X) + "." + e.Sel.Name
	case *ast.Ident:
		return e.Name
	default:
		return "?"
	}
}

// TestA2ABinaryIsReadOnlyWiring: cmd/a2a never dials Kafka and never starts an
// outbox relay or a consumer (only the api binary drains outbox_events).
func TestA2ABinaryIsReadOnlyWiring(t *testing.T) {
	forbidden := []string{
		"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/kafka",
		"github.com/claudioed/network-inventory-planning/internal/adapters/inbound/kafka",
		"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/memory",
	}
	files, err := filepath.Glob("../../cmd/a2a/*.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("no cmd/a2a sources found: %v", err)
	}
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		for _, imp := range importsOf(t, path) {
			for _, bad := range forbidden {
				if imp == bad {
					t.Errorf("%s imports %s: cmd/a2a is read-only (no Kafka, relay, consumer or in-memory runtime path)", path, imp)
				}
			}
		}
	}
}
