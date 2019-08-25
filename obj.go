package dbobj

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/pkg/errors"
)

var (
	// ErrNoKeyField is returned for tables without primary key identified
	ErrNoKeyField = errors.New("table has no key field")

	// ErrKeyMissing is returned when key value is not set
	ErrKeyMissing = errors.New("key is not set")
)

// SQLDB is a common interface for opening an sql db
type SQLDB func(string) (*sql.DB, error)

// SetHandler takes a slice of value pointer interfaces
// and returns an error if unable to set the values
type SetHandler func(...interface{}) error

type DBS interface {
	Query(fn SetHandler, query string, args ...interface{}) error
	Exec(query string, args ...interface{}) (RowsAffected, LastInsertID int64, err error)
}

// DBU is a DatabaseUnit
type DBU struct {
	BackedUp int64
	DB       *sql.DB
	log      *log.Logger
}

// SetLogger sets the logger for the B
func (db DBU) SetLogger(logger *log.Logger) {
	db.log = logger
}

func (db DBU) debugf(msg string, args ...interface{}) {
	if db.log != nil {
		db.log.Printf(msg, args...)
	}
}

/*
	DBU.Load(Loader, keys...)

*/

// Rows is copied from rqlite
type Rows struct {
	Columns []string        `json:"columns,omitempty"`
	Types   []string        `json:"types,omitempty"`
	Values  [][]interface{} `json:"values,omitempty"`
	Error   string          `json:"error,omitempty"`
	Time    float64         `json:"time,omitempty"`
}

type Loader interface {
	// SQLGet generates a plain SQL query
	// (no placeholders or parameter binding)
	SQLGet(keys ...interface{}) string
	SQLResults(values ...interface{}) error
	// generate query string
	// run query
	// apply query results to object
}

/*
 query := myObj.SQLGet(id)
 results := qyObj.
*/

//
// ***** rqlite ******
//
// generate plain text (no binding) queries
// Need to generate plain text (no binding) queries
/*
func (m *myObj) (
*/

// DBObject provides methods for object storage
type DBObject interface {
	// TableName is the name of the sql table
	TableName() string

	// KeyFields are the names of the table fields
	// comprising the primary id
	//KeyFields() []string
	KeyField() string

	// KeyNames are the struct names of the
	// primary id fields
	//KeyNames() []string
	KeyName() string

	// Names returns the struct names
	Names() []string
	SelectFields() string
	InsertFields() string
	Key() int64
	SetID(int64)
	InsertValues() []interface{}
	UpdateValues() []interface{}
	MemberPointers() []interface{}
	ModifiedBy(int64, time.Time)
}

func insertFields(o DBObject) string {
	list := strings.Split(o.InsertFields(), ",")
	keep := make([]string, 0, len(list))
	for _, p := range list {
		if p != o.KeyField() {
			keep = append(keep, p)
		}
	}
	return strings.Join(keep, ",")
}

func insertQuery(o DBObject) string {
	p := placeholders(len(o.InsertValues()))
	return fmt.Sprintf("insert into %s (%s) values(%s)", o.TableName(), insertFields(o), p)
}

func replaceQuery(o DBObject) string {
	p := placeholders(len(o.InsertValues()))
	return fmt.Sprintf("replace into %s (%s) values(%s)", o.TableName(), insertFields(o), p)
}

func updateQuery(o DBObject) string {
	return fmt.Sprintf("update %s set %s where %s=?", o.TableName(), setParams(insertFields(o)), o.KeyField())
}

func deleteQuery(o DBObject) string {
	return fmt.Sprintf("delete from %s where %s=?", o.TableName(), o.KeyField())
}

// Add new object to datastore
func (db DBU) Add(o DBObject) error {
	args := o.InsertValues()
	db.debugf(insertQuery(o), args)
	result, err := db.DB.Exec(insertQuery(o), args...)
	if result != nil {
		id, _ := result.LastInsertId()
		o.SetID(id)
	}
	return err
}

// Replace will replace an existing object in datastore
func (db DBU) Replace(o DBObject) error {
	args := o.InsertValues()
	result, err := db.DB.Exec(replaceQuery(o), args)
	if result != nil {
		id, _ := result.LastInsertId()
		o.SetID(id)
	}
	return err
}

// Save modified object in datastore
func (db DBU) Save(o DBObject) error {
	_, err := db.DB.Exec(updateQuery(o), o.UpdateValues()...)
	return err
}

// Delete object from datastore
func (db DBU) Delete(o DBObject) error {
	db.debugf(deleteQuery(o), o.Key())
	_, err := db.DB.Exec(deleteQuery(o), o.Key())
	return err
}

// DeleteByID object from datastore by id
func (db DBU) DeleteByID(o DBObject, id interface{}) error {
	db.debugf(deleteQuery(o), id)
	_, err := db.DB.Exec(deleteQuery(o), id)
	return err
}

// List objects from datastore
func (db DBU) List(o DBObject) (interface{}, error) {
	return db.ListQuery(o, "")
}

// Find loads an object matching the given keys
func (db DBU) Find(o DBObject, keys map[string]interface{}) error {
	where := make([]string, 0, len(keys))
	what := make([]interface{}, 0, len(keys))
	for k, v := range keys {
		where = append(where, k+"=?")
		what = append(what, v)
	}
	query := fmt.Sprintf("select %s from %s where %s", o.SelectFields(), o.TableName(), strings.Join(where, " and "))
	return db.get(o.MemberPointers(), query, what...)
}

// FindBy loads an  object matching the given key/value
func (db DBU) FindBy(o DBObject, key string, value interface{}) error {
	query := fmt.Sprintf("select %s from %s where %s=?", o.SelectFields(), o.TableName(), key)
	return db.get(o.MemberPointers(), query, value)
}

// FindByID loads an object based on a given ID
func (db DBU) FindByID(o DBObject, value interface{}) error {
	return db.FindBy(o, o.KeyField(), value)
}

// FindSelf loads an object based on it's current ID
func (db DBU) FindSelf(o DBObject) error {
	if len(o.KeyField()) == 0 {
		return ErrNoKeyField
	}
	if o.Key() == 0 {
		return ErrKeyMissing
	}
	return db.FindBy(o, o.KeyField(), o.Key())
}

// ListQuery returns a list of objects
func (db DBU) ListQuery(obj DBObject, extra string, args ...interface{}) (interface{}, error) {
	query := fmt.Sprintf("select %s from %s ", obj.SelectFields(), obj.TableName())
	if len(extra) > 0 {
		query += " " + extra
	}
	db.debugf(query, args)
	val := reflect.ValueOf(obj)
	base := reflect.Indirect(val)
	t := reflect.TypeOf(base.Interface())
	results := reflect.Zero(reflect.SliceOf(t))
	rows, err := db.DB.Query(query, args...)
	if err != nil {
		log.Println("error on query: " + query + " -- " + err.Error())
		return nil, err
	}
	for rows.Next() {
		v := reflect.New(t)
		dest := v.Interface().(DBObject).MemberPointers()
		if err = rows.Scan(dest...); err != nil {
			fmt.Println("query:", query, "error:", err)
			continue
		}
		results = reflect.Append(results, v.Elem())
	}
	err = rows.Err()
	rows.Close()
	//fmt.Println("LIST LEN:", results.Len())
	return results.Interface(), err
}

// NewDBU returns a new DBU
func NewDBU(file string, init bool, opener SQLDB) (DBU, error) {
	db, err := opener(file)
	return DBU{DB: db}, err
}

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

// marshal the object fields into an array
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
func (db DBU) ObjectInsert(obj interface{}) (int64, error) {
	skip := !keyIsSet(obj) // if we have a key, we should probably use it
	_, a := objFields(obj, skip)
	table, _, fields := dbFields(obj, skip)
	if len(table) == 0 {
		return -1, fmt.Errorf("no table defined for object: %v (fields: %s)", reflect.TypeOf(obj), fields)
	}
	query := fmt.Sprintf("insert into %s (%s) values (%s)", table, fields, placeholders(len(a)))
	result, err := db.DB.Exec(query, a...)
	if result != nil {
		id, _ := result.LastInsertId()
		return id, err
	}
	return -1, err
}

// ObjectUpdate updates an object
func (db DBU) ObjectUpdate(obj interface{}) error {
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

	_, err := db.DB.Exec(query, args...)
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
func (db DBU) ObjectDelete(obj interface{}) error {
	table, key, id := deleteInfo(obj)
	if len(key) == 0 {
		return ErrNoKeyField
	}
	query := fmt.Sprintf("delete from %s where %s=?", table, key)
	rec, err := db.DB.Exec(query, id)
	if err != nil {
		return fmt.Errorf("BAD QUERY:%s ID:%v ERROR:%v", query, id, err)
	}
	rows, _ := rec.RowsAffected()
	if rows == 0 {
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
func (db DBU) ObjectLoad(obj interface{}, extra string, args ...interface{}) (err error) {
	r := reflect.Indirect(reflect.ValueOf(obj)).Interface()
	query := createQuery(r, false)
	if len(extra) > 0 {
		query += " " + extra
	}
	db.debugf(query, args)
	row := db.DB.QueryRow(query, args...)
	dest := sPtrs(obj)
	return row.Scan(dest...)
}

// LoadMany loads many objects
func (db DBU) LoadMany(query string, Kind interface{}, args ...interface{}) (interface{}, error) {
	t := reflect.TypeOf(Kind)
	s2 := reflect.Zero(reflect.SliceOf(t))
	db.debugf(query, args)
	rows, err := db.DB.Query(query, args...)
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
func (db DBU) ObjectListQuery(kind interface{}, extra string, args ...interface{}) (interface{}, error) {
	query := createQuery(kind, false)
	if len(extra) > 0 {
		query += " " + extra
	}
	db.debugf("Q:%s A:%s\n", query, args)
	t := reflect.TypeOf(kind)
	results := reflect.Zero(reflect.SliceOf(t))
	rows, err := db.DB.Query(query, args...)
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

// ObjectList returns all objects
func (db DBU) ObjectList(Kind interface{}) (interface{}, error) {
	return db.ObjectListQuery(Kind, "")
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

func (db DBU) get(members []interface{}, query string, args ...interface{}) error {
	if db.log != nil {
		db.log.Printf("Q:%s A:%v\n", query, args)
	}
	rows, err := db.DB.Query(query, args...)
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
