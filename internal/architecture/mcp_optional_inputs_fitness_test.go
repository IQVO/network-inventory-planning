package architecture

import (
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// optionalDescriptionRE recognises a jsonschema description that tells the
// agent it may leave the field out: "optional", "omit ...", "defaults to
// ...", or a filter phrased "only ...".
var optionalDescriptionRE = regexp.MustCompile(`(?i)\boptional\b|\bomit\b|\bdefaults? to\b|^\s*only\b`)

// mcpOptionalFieldViolation is one MCP tool input field described as
// optional whose json tag lacks omitempty/omitzero.
type mcpOptionalFieldViolation struct {
	pos   string
	field string
}

// optionalFieldsWithoutOmitempty scans Go source for struct fields that
// carry a jsonschema description marking them optional but whose json tag
// has neither omitempty nor omitzero. The MCP Go SDK infers a tool's input
// schema from these structs and lists every field WITHOUT omitempty/omitzero
// as `required`, so such a field turns an optional filter into a mandatory
// argument and a call that omits it fails validation.
func optionalFieldsWithoutOmitempty(t *testing.T, filename string, src any) []mcpOptionalFieldViolation {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filename, src, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", filename, err)
	}
	var out []mcpOptionalFieldViolation
	ast.Inspect(file, func(n ast.Node) bool {
		if st, ok := n.(*ast.StructType); ok {
			for _, f := range st.Fields.List {
				if optionalWithoutOmitempty(f) {
					out = append(out, mcpOptionalFieldViolation{pos: fset.Position(f.Pos()).String(), field: fieldName(f)})
				}
			}
		}
		return true
	})
	return out
}

// optionalWithoutOmitempty reports whether f carries a jsonschema
// description marking it optional while its json tag has neither
// omitempty nor omitzero.
func optionalWithoutOmitempty(f *ast.Field) bool {
	if f.Tag == nil {
		return false
	}
	raw, err := strconv.Unquote(f.Tag.Value)
	if err != nil {
		return false
	}
	tag := reflect.StructTag(raw)
	description, hasSchema := tag.Lookup("jsonschema")
	if !hasSchema || !optionalDescriptionRE.MatchString(description) {
		return false
	}
	return !containsAny(strings.Split(tag.Get("json"), ",")[1:], "omitempty", "omitzero")
}

func fieldName(f *ast.Field) string {
	if len(f.Names) > 0 {
		return f.Names[0].Name
	}
	return "<embedded>"
}

func containsAny(options []string, want ...string) bool {
	for _, o := range options {
		for _, w := range want {
			if strings.TrimSpace(o) == w {
				return true
			}
		}
	}
	return false
}

// TestMCPOptionalToolInputsCarryOmitempty (gap-fix D16): every MCP tool
// input field described as optional must be optional in the inferred input
// schema, i.e. carry omitempty (or omitzero) in its json tag. Without it
// the SDK marks the field required and an agent call that leaves the
// filter out is rejected.
func TestMCPOptionalToolInputsCarryOmitempty(t *testing.T) {
	files := goFilesUnder(t, "../adapters/inbound/mcp", false)
	if len(files) == 0 {
		t.Fatal("no MCP adapter source found under internal/adapters/inbound/mcp")
	}
	for _, path := range files {
		for _, v := range optionalFieldsWithoutOmitempty(t, path, nil) {
			t.Errorf("%s: MCP input field %s is described as optional but its json tag lacks omitempty", v.pos, v.field)
		}
	}
}

// TestMCPOptionalFieldDetector proves the detector catches the violation
// and leaves required fields and correct optional fields alone.
func TestMCPOptionalFieldDetector(t *testing.T) {
	src := "package x\n" +
		"type in struct {\n" +
		"\tID    string `json:\"id\" jsonschema:\"the id\"`\n" +
		"\tState string `json:\"state\" jsonschema:\"only transfers in this state\"`\n" +
		"\tLimit int    `json:\"limit\" jsonschema:\"page size; defaults to 50\"`\n" +
		"\tSite  string `json:\"site\" jsonschema:\"optional site filter\"`\n" +
		"\tAge   int    `json:\"age\" jsonschema:\"omit for every age\"`\n" +
		"\tOK1   string `json:\"ok1,omitempty\" jsonschema:\"only this one\"`\n" +
		"\tOK2   int    `json:\"ok2,omitzero\" jsonschema:\"defaults to 1\"`\n" +
		"}\n"
	got := map[string]bool{}
	for _, v := range optionalFieldsWithoutOmitempty(t, "detector.go", src) {
		got[v.field] = true
	}
	for _, want := range []string{"State", "Limit", "Site", "Age"} {
		if !got[want] {
			t.Errorf("detector missed optional field %s without omitempty", want)
		}
	}
	for _, clean := range []string{"ID", "OK1", "OK2"} {
		if got[clean] {
			t.Errorf("detector flagged %s, which is required or already carries omitempty/omitzero", clean)
		}
	}
}
