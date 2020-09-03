// +build why

package dbobj

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

var (
	// ErrNoKeyField is returned for tables without primary key identified
	ErrNoKeyField = fmt.Errorf("table has no key field")

	// ErrKeyMissing is returned when key value is not set
	ErrKeyMissing = fmt.Errorf("key is not set")

	numeric = regexp.MustCompile("^[0-9]+(\\.[0-9])?$")
)

// helper to generate sql values placeholders
func placeholders(n int) string {
	a := make([]string, n)
	for i := range a {
		a[i] = "?"
	}
	return strings.Join(a, ",")
}

func keyIsSet(obj interface{}) bool {
	val := reflect.ValueOf(obj)
	t := reflect.TypeOf(obj)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.Tag.Get("key") == "true" {
			v := val.Field(i).Interface()
			switch v.(type) {
			case int:
				return v.(int) > 0
			case int64:
				return v.(int64) > 0
			default:
				return false
			}
		}
	}
	return false
}

// generate list of sql fields for members.
// if skipKey is true, do not include the key field in the list
func dbFields(obj interface{}, skipKey bool) (table, key, fields string) {
	t := reflect.TypeOf(obj)
	list := make([]string, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if isTable := f.Tag.Get("table"); len(isTable) > 0 {
			table = isTable
		}
		k := f.Tag.Get("sql")
		if f.Tag.Get("key") == "true" {
			key = k
			if skipKey {
				continue
			}
		}
		if len(k) > 0 {
			list = append(list, k)
		}
	}
	fields = strings.Join(list, ",")
	return
}

// objFields marshals the object fields into an array
func objFields(obj interface{}, skipKey bool) (interface{}, []interface{}) {
	val := reflect.ValueOf(obj)
	t := reflect.TypeOf(obj)
	a := make([]interface{}, 0, t.NumField())
	var key interface{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if len(f.Tag.Get("sql")) == 0 {
			continue
		}
		if f.Tag.Get("key") == "true" {
			key = val.Field(i).Interface()
			if skipKey {
				continue
			}
		}
		a = append(a, val.Field(i).Interface())
	}
	return key, a
}

// ObjectInsert inserts an object
func ObjectInsert(db *sql.DB, obj interface{}) (int64, error) {
	skip := !keyIsSet(obj) // if we have a key, we should probably use it
	_, a := objFields(obj, skip)
	table, _, fields := dbFields(obj, skip)
	if len(table) == 0 {
		return -1, fmt.Errorf("no table defined for object: %v (fields: %s)", reflect.TypeOf(obj), fields)
	}
	query := fmt.Sprintf("insert into %s (%s) values (%s)", table, fields, placeholders(len(a)))
	result, err := db.Exec(query, a...)
	if result != nil {
		id, _ := result.LastInsertId()
		return id, err
	}
	return -1, err
}

// ObjectUpdate updates an object
func ObjectUpdate(db *sql.DB, obj interface{}) error {
	var table, key string
	var id interface{}
	val := reflect.ValueOf(obj)
	t := reflect.TypeOf(obj)
	list := make([]string, 0, t.NumField())
	args := make([]interface{}, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if isTable := f.Tag.Get("table"); len(isTable) > 0 {
			table = isTable
		}
		if len(f.Tag.Get("sql")) == 0 {
			continue
		}
		if f.Tag.Get("update") == "false" {
			continue
		}
		k := f.Tag.Get("sql")
		v := val.Field(i).Interface()
		isKey := f.Tag.Get("key")
		if isKey == "true" {
			key = k
			id = v
			continue
		}
		args = append(args, val.Field(i).Interface())
		list = append(list, fmt.Sprintf("%s=?", k))
	}
	if len(key) == 0 {
		return ErrNoKeyField
	}
	args = append(args, id)
	query := fmt.Sprintf("update %s set %s where %s=?", table, strings.Join(list, ","), key)

	_, err := db.Exec(query, args...)
	return err
}

func deleteInfo(obj interface{}) (table, key string, id interface{}) {
	val := reflect.ValueOf(obj)
	t := reflect.TypeOf(obj)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if isTable := f.Tag.Get("table"); len(isTable) > 0 {
			table = isTable
		}
		if len(f.Tag.Get("sql")) == 0 {
			continue
		}
		if f.Tag.Get("update") == "false" {
			continue
		}
		k := f.Tag.Get("sql")
		v := val.Field(i).Interface()
		isKey := f.Tag.Get("key")
		if isKey == "true" {
			key = k
			id = v
			break
		}
	}
	return
}

// ObjectDelete deletes the object
func ObjectDelete(db *sql.DB, obj interface{}) error {
	table, key, id := deleteInfo(obj)
	if len(key) == 0 {
		return ErrNoKeyField
	}
	query := fmt.Sprintf("delete from %s where %s=?", table, key)
	rec, err := db.Exec(query, id)
	if err != nil {
		return fmt.Errorf("BAD QUERY:%s ID:%v ERROR:%v", query, id, err)
	}
	if updated, _ := rec.RowsAffected(); updated == 0 {
		return fmt.Errorf("No record deleted for id: %v", id)
	}
	return nil
}

// sPtrss makes slice of pointers to struct members for sql scanner
// expects struct value as input
func sPtrs(obj interface{}) []interface{} {
	base := reflect.Indirect(reflect.ValueOf(obj))
	t := reflect.TypeOf(base.Interface())
	data := make([]interface{}, 0, base.NumField())
	for i := 0; i < base.NumField(); i++ {
		if tag := t.Field(i).Tag.Get("sql"); len(tag) > 0 {
			data = append(data, base.Field(i).Addr().Interface())
		}
	}
	return data
}

// ObjectLoad load an object with matching record info
func ObjectLoad(db *sql.DB, obj interface{}, extra string, args ...interface{}) (err error) {
	r := reflect.Indirect(reflect.ValueOf(obj)).Interface()
	query := createQuery(r, false)
	if len(extra) > 0 {
		query += " " + extra
	}
	row := db.QueryRow(query, args...)
	dest := sPtrs(obj)
	return row.Scan(dest...)
}

// LoadMany loads many objects
func LoadMany(db *sql.DB, query string, Kind interface{}, args ...interface{}) (interface{}, error) {
	t := reflect.TypeOf(Kind)
	s2 := reflect.Zero(reflect.SliceOf(t))
	rows, err := db.Query(query, args...)
	if err == nil {
		for rows.Next() {
			v := reflect.New(t)
			dest := sPtrs(v.Interface())
			err = rows.Scan(dest...)
			s2 = reflect.Append(s2, v.Elem())
		}
	}
	rows.Close()
	return s2.Interface(), err
}

// ObjectListQuery returns a list of objects specified by query
func ObjectListQuery(db *sql.DB, kind interface{}, extra string, args ...interface{}) (interface{}, error) {
	query := createQuery(kind, false)
	if len(extra) > 0 {
		query += " " + extra
	}
	t := reflect.TypeOf(kind)
	results := reflect.Zero(reflect.SliceOf(t))
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, errors.Wrapf(err, "error on query: %s", query)
	}
	defer rows.Close()
	for rows.Next() {
		v := reflect.New(t)
		dest := sPtrs(v.Interface())
		err = rows.Scan(dest...)
		if err != nil {
			log.Println("scan error: " + err.Error())
			log.Println("scan query: "+query+" args:", args)
			return nil, err
		}
		results = reflect.Append(results, v.Elem())
	}
	return results.Interface(), nil
}

func setParams(params string) string {
	list := strings.Split(params, ",")
	for i, p := range list {
		list[i] = fmt.Sprintf("%s=?", p)
	}
	return strings.Join(list, ",")
}

func createQuery(obj interface{}, skipKey bool) string {
	var table string
	t := reflect.TypeOf(obj)
	list := make([]string, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if len(f.Tag.Get("sql")) == 0 {
			continue
		}
		name := f.Tag.Get("table")
		if len(name) > 0 {
			table = name
		}
		if skipKey {
			key := f.Tag.Get("key")
			if key == "true" {
				continue
			}
		}
		list = append(list, f.Tag.Get("sql"))
	}
	if len(table) == 0 {
		return ("error: no table name specified for object:" + t.Name())
	}
	return "select " + strings.Join(list, ",") + " from " + table
}

func keyIndex(obj interface{}) int {
	t := reflect.TypeOf(obj)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if len(f.Tag.Get("key")) > 0 {
			return i
		}
	}
	return 0 // TODO: error handling!
}

func get(db *sql.DB, members []interface{}, query string, args ...interface{}) error {
	rows, err := db.Query(query, args...)
	if err != nil {
		log.Println("error on query: " + query + " -- " + err.Error())
		return nil
	}
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(members...)
		if err != nil {
			log.Println("scan error: " + err.Error())
			log.Println("scan query: "+query+" args:", args)
			return err
		}
		return nil
	}
	return nil
}
