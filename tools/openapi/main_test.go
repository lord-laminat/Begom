package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"
)

// Check the server's registered routes, including actions registered in loops,
// against the generated artifact rather than a second hand-maintained list.
func TestEveryRouteAndReferenceDocumented(t *testing.T) {
	file, e := parser.ParseFile(token.NewFileSet(), "../../internal/coordinator/server.go", nil, 0)
	if e != nil {
		t.Fatal(e)
	}
	b, e := os.ReadFile("../../docs/openapi.json")
	if e != nil {
		t.Fatal(e)
	}
	var doc map[string]any
	if e = json.Unmarshal(b, &doc); e != nil {
		t.Fatal(e)
	}
	bindings := map[string][]string{}
	ast.Inspect(file, func(n ast.Node) bool {
		if loop, ok := n.(*ast.RangeStmt); ok {
			name, named := loop.Value.(*ast.Ident)
			values, literal := loop.X.(*ast.CompositeLit)
			if named && literal {
				for _, value := range values.Elts {
					if s, ok := value.(*ast.BasicLit); ok && s.Kind == token.STRING {
						v, _ := strconv.Unquote(s.Value)
						bindings[name.Name] = append(bindings[name.Name], v)
					}
				}
			}
		}
		return true
	})
	var values func(ast.Expr) []string
	values = func(expr ast.Expr) []string {
		switch v := expr.(type) {
		case *ast.BasicLit:
			s, e := strconv.Unquote(v.Value)
			if e == nil {
				return []string{s}
			}
		case *ast.Ident:
			return bindings[v.Name]
		case *ast.BinaryExpr:
			if v.Op == token.ADD {
				var result []string
				for _, left := range values(v.X) {
					for _, right := range values(v.Y) {
						result = append(result, left+right)
					}
				}
				return result
			}
		}
		return nil
	}
	paths := doc["paths"].(map[string]any)
	count := 0
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) == 0 {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || (selector.Sel.Name != "HandleFunc" && selector.Sel.Name != "Handle") {
			return true
		}
		for _, route := range values(call.Args[0]) {
			method, path, ok := strings.Cut(route, " ")
			if !ok || path == "/" {
				continue
			}
			methods, ok := paths[path].(map[string]any)
			if !ok || methods[strings.ToLower(method)] == nil {
				t.Error("undocumented route:", route)
			}
			count++
		}
		return true
	})
	if count < 50 {
		t.Fatal("route extraction did not inspect the application", count)
	}
	var walk func(any)
	walk = func(v any) {
		switch v := v.(type) {
		case map[string]any:
			for k, child := range v {
				if k == "$ref" {
					ref := child.(string)
					var node any = doc
					for _, part := range strings.Split(strings.TrimPrefix(ref, "#/"), "/") {
						m, ok := node.(map[string]any)
						if !ok || m[part] == nil {
							t.Errorf("unresolved reference: %s", ref)
							break
						}
						node = m[part]
					}
				} else {
					walk(child)
				}
			}
		case []any:
			for _, child := range v {
				walk(child)
			}
		}
	}
	walk(doc)
}
