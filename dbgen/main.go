// Copyright 2014 The Go Authors. All rights reserved.
// Copyright 2015 Paul Stuart. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Adapted from golang.org/x/tools/cmd/stringer/stringer.go

// dbgen is a tool to automate the creation of create/update/delete methods that
// satisfy the github.com/paulstuart/dbobj.DBObject interface.
//
// For example, given this snippet,
//
//	package dbobjs
//
// type User struct {
// 	ID       int64		`sql:"id" key:"true" table:"users"`
// 	Username string		`sql:"username"`
// 	First    string		`sql:"firstname"`
// 	Last     string		`sql:"lastname"`
// 	Email    string		`sql:"email"`
// 	Role     int		`sql:"role"`
// 	UserID   int64		`sql:"userid"    audit:"user"`
// 	Modified time.Time  `sql:"modified"  audit:"time"`
// 	Created  time.Time  `sql:"created"  update="false"
// }
//
// running this command
//
//	dbgen
//
// in the same directory will create the file db_generated.go, in package dbobjs,
// containing the definition:
//
//
// Typically this process would be run using go generate, like this:
//
//	//go:generate dbgen
//
// The -type flag accepts a comma-separated list of types so a single run can
// generate methods for multiple types. The default output file is db_generated.go,
// where t is the lower-cased name of the first type listed. It can be overridden
// with the -output flag.
//
package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/build"
	"go/format"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
)

// For testing
//go:generate ./dbgen -output generated_test.go -type testStruct struct_test.go
var (
	typeNames = flag.String("type", "", "comma-separated list of type names; leave blank for all")
	output    = flag.String("output", "", "output file name; default srcdir/db_wrapper.go")
)

const (
	ignore = "github.com/paulstuart/dbobj.DBObject"
)

// Usage is a replacement usage function for the flags package.
func Usage() {
	const msg = `
Usage of %s:

%s [flags] [-type T] files... # Must be a single package

For more information, see: http://github.com/paulstuart/dbgen

Flags:
`

	fmt.Fprintf(os.Stderr, msg, os.Args[0], os.Args[0])
	/*
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\tdbgen [flags] [-type T] [directory]\n")
		fmt.Fprintf(os.Stderr, "\tdbgen [flags[ [-type T] files... # Must be a single package\n")
		fmt.Fprintf(os.Stderr, "For more information, see:\n")
		fmt.Fprintf(os.Stderr, "\thttp://github.com/paulstuart/dbgen\n")
		fmt.Fprintf(os.Stderr, "Flags:\n")
		flag.PrintDefaults()
	*/
	os.Exit(2)
}

type SQLInfo struct {
	Name      string            // type name
	Table     string            // sql table
	KeyName   string            // member name for key
	KeyField  string            // sql field for key
	UserField string            // sql field for user id
	TimeField string            // sql field for timestamp
	Order     []string          // sql fields in order
	Fields    map[string]string //
	NoUpdate  map[string]struct{}
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("dbgen: ")
	flag.Usage = Usage
	flag.Parse()
	names := strings.Split(*typeNames, ",")

	// We accept either one directory or a list of files. Which do we have?
	args := flag.Args()
	if len(args) == 0 {
		// Default: process whole package in current directory.
		args = []string{"."}
	}

	// Parse the package once.
	var (
		dir string
		g   Generator
	)
	if len(args) == 1 && isDirectory(args[0]) {
		g.parsePackageDir(args[0])
	} else {
		dir = filepath.Dir(args[0])
		g.parsePackageFiles(args)
	}

	// Print the header and package clause.
	g.Printf("// generated by 'dbgen %s'; DO NOT EDIT\n", strings.Join(os.Args[1:], " "))
	g.Printf("\npackage %s\n", g.pkg.name)
	g.Printf(`

import (
	"time"
)

`)
	if len(names) == 0 {
		g.generate("")
	} else {
		for _, typeName := range names {
			g.generate(typeName)
		}
	}

	// Format the output.
	src := g.format()

	// Write to file.
	outputName := *output
	if outputName == "" {
		baseName := "db_generated.go"
		outputName = filepath.Join(dir, strings.ToLower(baseName))
	}
	err := ioutil.WriteFile(outputName, src, 0644)
	if err != nil {
		log.Fatalf("writing output: %s", err)
	}
}

// helper to generate sql values placeholders
func Placeholders(n int) string {
	a := make([]string, n)
	for i := range a {
		a[i] = "?"
	}
	return strings.Join(a, ",")
}

// isDirectory reports whether the named file is a directory.
func isDirectory(name string) bool {
	info, err := os.Stat(name)
	if err != nil {
		log.Fatal(err)
	}
	return info.IsDir()
}

// Generator holds the state of the analysis. Primarily used to buffer
// the output for format.Source.
// sql tag added for testing
type Generator struct {
	buf bytes.Buffer `sql:"buf" table:generator` // Accumulated output.
	pkg *Package     // Package we are scanning.
}

func (g *Generator) Printf(format string, args ...interface{}) {
	fmt.Fprintf(&g.buf, format, args...)
}

// File holds a single parsed file and associated data.
type File struct {
	pkg  *Package  // Package to which this file belongs.
	file *ast.File // Parsed AST.
	// These fields are reset for each type being generated.
	TypeName string     // Name of the current type.
	findName string     // Type name to match (if set)
	values   []*SQLInfo // Accumulator for sql annotated objects
}

// sql tags for testing
type Package struct {
	dir      string `sql:"dir" table:"pkg" key="true"`
	name     string `sql:"name" audit:"name"`
	defs     map[*ast.Ident]types.Object
	files    []*File
	typesPkg *types.Package
}

// parsePackageDir parses the package residing in the directory.
func (g *Generator) parsePackageDir(directory string) {
	pkg, err := build.Default.ImportDir(directory, 0)
	if err != nil {
		log.Fatalf("cannot process directory %s: %s", directory, err)
	}
	var names []string
	names = append(names, pkg.GoFiles...)
	//fmt.Println("NAMES", names)
	names = append(names, pkg.CgoFiles...)
	// names = append(names, pkg.TestGoFiles...) // These are also in the "foo" package.
	names = append(names, pkg.SFiles...)
	names = prefixDirectory(directory, names)
	g.parsePackage(directory, names, nil)
}

// parsePackageFiles parses the package occupying the named files.
func (g *Generator) parsePackageFiles(names []string) {
	//fmt.Println("PARSE", names)
	g.parsePackage(".", names, nil)
}

// prefixDirectory places the directory name on the beginning of each name in the list.
func prefixDirectory(directory string, names []string) []string {
	if directory == "." {
		return names
	}
	ret := make([]string, len(names))
	for i, name := range names {
		ret[i] = filepath.Join(directory, name)
	}
	return ret
}

// parsePackage analyzes the single package constructed from the named files.
// If text is non-nil, it is a string to be used instead of the content of the file,
// to be used for testing. parsePackage exits if there is an error.
func (g *Generator) parsePackage(directory string, names []string, text interface{}) {
	var files []*File
	var astFiles []*ast.File
	g.pkg = new(Package)
	fs := token.NewFileSet()
	for _, name := range names {
		if !strings.HasSuffix(name, ".go") {
			continue
		}
		parsedFile, err := parser.ParseFile(fs, name, text, 0)
		if err != nil && name != "db_generated.go" {
			log.Fatalf("parsing package: %s: %s", name, err)
		}
		astFiles = append(astFiles, parsedFile)
		files = append(files, &File{
			file: parsedFile,
			pkg:  g.pkg,
		})
	}
	if len(astFiles) == 0 {
		log.Fatalf("%s: no buildable Go files", directory)
	}
	g.pkg.name = astFiles[0].Name.Name
	g.pkg.files = files
	g.pkg.dir = directory
	// Type check the package.
	g.pkg.check(fs, astFiles)
}

// check type-checks the package. The package must be OK to proceed.
func (pkg *Package) check(fs *token.FileSet, astFiles []*ast.File) {
	pkg.defs = make(map[*ast.Ident]types.Object)
	config := types.Config{
		Importer:    importer.Default(),
		FakeImportC: true,
		Error: func(e error) {
			//fmt.Println("PKG ERR:", e)
			err := e.(types.Error)
			//if strings.HasSuffix(err.Msg, ignore) || strings.Index(err.Msg, "DBObject") > 0 {
			i := strings.Index(err.Msg, "DBObject")
			if strings.HasSuffix(err.Msg, ignore) || i > 0 {
				err.Msg = ""
				e = nil
				return
			}
			if strings.Index(err.Msg, "has no field or method") > 0 {
				switch {
				case strings.Index(err.Msg, "TableName") > 0:
				default:
					file := err.Fset.File(err.Pos)
					//log.Println("POS:", err.Pos, "MSG:", err.Msg, "INDEX:", i, "SOFT:", err.Soft, "FSET:", err.Fset)
					log.Println("POS:", err.Pos, "MSG:", err.Msg, "INDEX:", i, "SOFT:", err.Soft, "FILE:", file.Name())
					return
				}
				err.Msg = ""
				e = nil
			}
		},
	}
	info := &types.Info{
		Defs: pkg.defs,
	}
	typesPkg, err := config.Check(pkg.dir, fs, astFiles, info)
	if err != nil {
		log.Println("failed checking package:", err)
	}
	pkg.typesPkg = typesPkg
}

// generate produces the DBObject methods for the named type.
func (g *Generator) generate(typeName string) {
	for _, file := range g.pkg.files {
		file.findName = typeName
		if file.file != nil {
			ast.Inspect(file.file, file.genDecl)
			for _, v := range file.values {
				g.buildWrappers(v)
			}
		}
	}
}

// format returns the gofmt-ed contents of the Generator's buffer.
func (g *Generator) format() []byte {
	src, err := format.Source(g.buf.Bytes())
	if err != nil {
		// Should never happen, but can arise when developing this code.
		// The user can compile the output to see the error.
		log.Printf("warning: internal error: invalid Go generated: %s", err)
		log.Printf("warning: compile the package to analyze the error")
		return g.buf.Bytes()
	}
	return src
}

//
//
// Parse the tags
//
//
func sqlTags(typeName string, fields *ast.FieldList) *SQLInfo {
	info := SQLInfo{}
	info.Fields = make(map[string]string) // [memberName]sqlName
	info.Order = make([]string, 0, len(fields.List))
	info.NoUpdate = make(map[string]struct{})
	good := false
	for _, field := range fields.List {
		if t := field.Tag; t != nil {
			s := string(t.Value)
			// the code uses backticks to metaquote, need to strip them whilst evaluating
			tag := reflect.StructTag(s[1 : len(s)-1])
			if sql := tag.Get("sql"); len(sql) > 0 {
				//fmt.Println("SQL:", sql)
				if table := tag.Get("table"); len(table) > 0 {
					info.Table = table
				}
				if key := tag.Get("key"); len(key) > 0 {
					info.KeyName = string(field.Names[0].Name)
					info.KeyField = sql
				} else {
					info.Fields[field.Names[0].Name] = sql
					info.Order = append(info.Order, field.Names[0].Name)
				}
				good = true
			}
			if audit := tag.Get("audit"); len(audit) > 0 {
				//fmt.Println("AUDIT:", audit, "N:", string(field.Names[0].Name))
				switch {
				case audit == "user":
					info.UserField = string(field.Names[0].Name)
				case audit == "time":
					info.TimeField = string(field.Names[0].Name)
				}
			}
			if update := tag.Get("update"); len(update) > 0 {
				if up, err := strconv.ParseBool(update); err == nil && up == false {
					//if _, err := strconv.ParseBool(update); err == nil {
					//fmt.Println("NO UPDATE:", field.Names[0].Name)
					info.NoUpdate[field.Names[0].Name] = struct{}{}
				}
			}
		}
	}
	if good {
		return &info
	}
	return nil
}

// genDecl processes one declaration clause.
func (f *File) genDecl(node ast.Node) bool {
	switch x := node.(type) {
	case *ast.TypeSpec:
		f.TypeName = x.Name.Name
	case *ast.StructType:
		if len(f.findName) == 0 || f.findName == f.TypeName {
			if tags := sqlTags(f.TypeName, x.Fields); tags != nil {
				tags.Name = f.TypeName
				f.values = append(f.values, tags)
			}
			return false
		}
	}
	return true
}

// buildWrappers generates the variables and String method for a single run of contiguous values.
func (g *Generator) buildWrappers(s *SQLInfo) {
	names := []string{}
	elem := []string{}
	ptr := []string{}
	set := []string{}
	sql := []string{}
	insert_fields := []string{}
	if len(s.KeyField) > 0 {
		sql = append(sql, s.KeyField)
	}
	if len(s.KeyName) > 0 {
		ptr = append(ptr, "&o."+s.KeyName)
	}
	for _, k := range s.Order {
		if len(k) > 0 {
			v := s.Fields[k]
			sql = append(sql, v)
			names = append(names, `"`+k+`"`)
			elem = append(elem, "o."+k)
			ptr = append(ptr, "&o."+k)
			set = append(set, v+"=?")
			if _, ok := s.NoUpdate[v]; !ok {
				insert_fields = append(insert_fields, v)
			}
		}
	}
	g.Printf("\n\n//\n// %s DBObject generator\n//\n", s.Name)
	g.Printf(stringNewObj, s.Name)
	g.Printf("\n//\n// %s DBObject interface functions\n//\n", s.Name)
	g.Printf(stringInsertValues, s.Name, strings.Join(elem, ","))
	if len(s.KeyName) > 0 {
		elem = append(elem, "o."+s.KeyName)
	}
	g.Printf(stringUpdateValues, s.Name, strings.Join(elem, ","))
	g.Printf(stringMemberPointers, s.Name, strings.Join(ptr, ","))
	if len(s.KeyField) > 0 {
		g.Printf(stringKey, s.Name, s.KeyName)
		g.Printf(stringSetID, s.Name, s.KeyName)
	} else {
		g.Printf(stringNoKey, s.Name)
		g.Printf(stringNoSetID, s.Name)
	}

	g.Printf(stringSQLGet, s.Name, s.Table, strings.Join(sql, ","), "")
	g.Printf(stringTableName, s.Name, s.Table)
	g.Printf(stringSelectFields, s.Name, strings.Join(sql, ","))
	g.Printf(stringInsertFields, s.Name, strings.Join(sql, ","))
	g.Printf(stringKeyField, s.Name, s.KeyField)
	g.Printf(stringKeyName, s.Name, s.KeyName)
	g.Printf(stringNames, s.Name, strings.Join(names, ","))
	g.Printf(auditString(s.Name, s.UserField, s.TimeField))
}

// Arguments to format are:
//	[1]: type name
//	[2]: sql table
//	[3]: comma separated list of fields
//	[4]: comma separated list of parameter placeholders, e.g., (?,?,?)
const stringReplace = `func (o *%[1]s) ReplaceQuery() string {
	return "replace into %[2]s (%[3]s) values(%[4]s)"
}

`

// Arguments to format are:
//	[1]: type name
//	[2]: sql table
//	[3]: comma separated list of fields
//	[4]: comma separated list of parameter placeholders, e.g., (?,?,?)
const stringInsert = `func (o *%[1]s) InsertQuery() string {
	return "insert into %[2]s (%[3]s) values(%[4]s)"
}

`

// Arguments to format are:
//	[1]: type name
//	[2]: sql table
//	[3]: update set pairs
//	[4]: where criteria
const stringUpdate = `func (o *%[1]s) UpdateQuery() string {
	return "update %[2]s set %[3]s where %[4]s"
}

`

// Arguments to format are:
//	[1]: type name
//	[2]: sql table
//	[3]: insert fields (excluding key)
const stringInsertValues = `func (o *%[1]s) InsertValues() []interface{} {
	return []interface{}{%s}
}

`

// stringUpdateValues arguments
//	[1]: type name
//	[2]: sql table
//	[3]: update fields (including key)
const stringUpdateValues = `func (o *%[1]s) UpdateValues() []interface{} {
	return []interface{}{%s}
}

`

/*
// Arguments to format are:
//	[1]: type name
const stringUpdateInvalid = `func (o *%[1]s) UpdateValues() []interface{} {
	return []interface{}{%s}
}
`
*/

// Arguments to format are:
//	[1]: type name
//	[2]: sql table
//	[3]: update fields (including key)
const stringMemberPointers = `func (o *%[1]s) MemberPointers() []interface{} {
	return []interface{}{%s}
}

`

// Arguments to format are:
//	[1]: type name
//	[2]: key field
const stringKey = `func (o *%[1]s) Key() int64 {
	return o.%[2]s
}

`

// Arguments to format are:
//	[1]: type name
//	[2]: key field
const stringNoKey = `func (o *%[1]s) Key() int64 {
	return 0
}

`

// Arguments to format are:
//	[1]: type name
//	[2]: key field
const stringSetID = `func (o *%[1]s) SetID(id int64) {
	o.%[2]s = id
}

`

// Arguments to format are:
//	[1]: type name
//	[2]: key field
const stringNoSetID = `func (o *%[1]s) SetID(id int64) {
}

`

// Arguments to format are:
//	[1]: type name
//	[2]: table name
const stringTableName = `func (o *%[1]s) TableName() string {
	return "%[2]s"
}

`

// Arguments to format are:
//	[1]: type name
//	[2]: key field
const stringKeyField = `func (o *%[1]s) KeyField() string {
	return "%[2]s"
}

`

// Arguments to format are:
//	[1]: type name
//	[2]: key name
const stringKeyName = `func (o *%[1]s) KeyName() string {
	return "%[2]s"
}

`

// Arguments to format are:
//	[1]: type name
//	[2]: sql table
//	[3]: where criteria
const stringDelete = `func (o *%[1]s) DeleteQuery() string {
	return "delete from %[2]s where %[3]s"
}

`

// Arguments to format are:
//	[1]: type name
//	[2]: select fields
const stringSelectFields = `func (o *%[1]s) SelectFields() string {
	return "%[2]s"
}

`

// Arguments to format are:
//	[1]: type name
//	[2]: insert fields
const stringInsertFields = `func (o *%[1]s) InsertFields() string {
	return "%[2]s"
}

`

// Arguments to format are:
//	[1]: type name
const stringNewObj = `func (o %[1]s) NewObj() interface{} {
	return new(%[1]s)
}

`

// Arguments to format are:
//	[1]: type name
//	[2]: member names
const stringNames = `func (o *%[1]s) Names() []string {
	return []string{%[2]s}
}

`

func auditString(name, u, t string) string {
	args := []interface{}{name}
	stringAudit := "func (o *%s) ModifiedBy(user int64, t time.Time) {\n"
	if len(u) > 0 {
		stringAudit += "o.%s = &user\n"
		args = append(args, u)
	}
	if len(t) > 0 {
		stringAudit += "o.%s = t\n"
		args = append(args, t)
	}
	stringAudit += "}\n\n\n"
	return fmt.Sprintf(stringAudit, args...)
}

// Arguments to format are:
//	[1]: type name
//	[2]: table name
//	[3]: select fields
//	[4]: where fields
const stringSQLGet = `func (o *%[1]s) SQLGet(keys interface{}...) string {
	return "select %[3]s from %[2]s where %[4]s;"
}

`
