package dbobj

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/paulstuart/dbutil"
	"github.com/pkg/errors"
)

var (
	ErrNoKeyField = fmt.Errorf("table has no key field")
	ErrKeyMissing = fmt.Errorf("key is not set")
	ErrNilDB      = fmt.Errorf("db is nil")

	numeric = regexp.MustCompile("^[0-9]+(\\.[0-9])?$")
)

type DBU struct {
	BackedUp int64
	DB       *sql.DB
	log      *log.Logger
}

func (db DBU) SetLogger(logger *log.Logger) {
	if logger == nil {
		logger = log.New(ioutil.Discard, "", 0)
	}
	db.log = logger
}

func (db DBU) debugf(msg string, args ...interface{}) {
	if db.log != nil {
		db.log.Printf(msg, args...)
	}
}

type QueryKeys map[string]interface{}

type DBObject interface {
	TableName() string
	KeyField() string
	KeyName() string
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

type DBGen interface {
	NewObj() interface{} //DBObject
}

func InsertFields(o DBObject) string {
	list := strings.Split(o.InsertFields(), ",")
	keep := make([]string, 0, len(list))
	for _, p := range list {
		if p != o.KeyField() {
			keep = append(keep, p)
		}
	}
	return strings.Join(keep, ",")
}

func SelectQuery(o DBObject) string {
	return fmt.Sprintf("select %s from %s where %s=?", o.SelectFields(), o.TableName(), o.KeyField())
}

func InsertQuery(o DBObject) string {
	p := Placeholders(len(o.InsertValues()))
	return fmt.Sprintf("insert into %s (%s) values(%s)", o.TableName(), InsertFields(o), p)
}

func ReplaceQuery(o DBObject) string {
	p := Placeholders(len(o.InsertValues()))
	return fmt.Sprintf("replace into %s (%s) values(%s)", o.TableName(), InsertFields(o), p)
}

func UpdateQuery(o DBObject) string {
	return fmt.Sprintf("update %s set %s where %s=?", o.TableName(), setParams(InsertFields(o)), o.KeyField())
}

func DeleteQuery(o DBObject) string {
	return fmt.Sprintf("delete from %s where %s=?", o.TableName(), o.KeyField())
}

// Add new object to datastore
func (db DBU) Add(o DBObject) error {
	args := o.InsertValues()
	db.debugf(InsertQuery(o), args)
	result, err := db.DB.Exec(InsertQuery(o), args...)
	if result != nil {
		id, _ := result.LastInsertId()
		o.SetID(id)
	}
	return err
}

// Add new or replace existing object in datastore
func (db DBU) Replace(o DBObject) error {
	args := o.InsertValues()
	result, err := db.DB.Exec(ReplaceQuery(o), args)
	if result != nil {
		id, _ := result.LastInsertId()
		o.SetID(id)
	}
	return err
}

// Save modified object in datastore
func (db DBU) Save(o DBObject) error {
	_, err := db.DB.Exec(UpdateQuery(o), o.UpdateValues()...)
	return err
}

// Delete object from datastore
func (db DBU) Delete(o DBObject) error {
	db.debugf(DeleteQuery(o), o.Key())
	_, err := db.DB.Exec(DeleteQuery(o), o.Key())
	return err
}

// Delete object from datastore by id
func (db DBU) DeleteByID(o DBObject, id interface{}) error {
	db.debugf(DeleteQuery(o), id)
	_, err := db.DB.Exec(DeleteQuery(o), id)
	return err
}

// List objects from datastore
func (db DBU) List(o DBObject) (interface{}, error) {
	return db.ListQuery(o, "")
}

func (db DBU) Find(o DBObject, keys QueryKeys) error {
	where := make([]string, 0, len(keys))
	what := make([]interface{}, 0, len(keys))
	for k, v := range keys {
		where = append(where, k+"=?")
		what = append(what, v)
	}
	query := fmt.Sprintf("select %s from %s where %s", o.SelectFields(), o.TableName(), strings.Join(where, " and "))
	return db.Get(o.MemberPointers(), query, what...)
}

func (db DBU) FindBy(o DBObject, key string, value interface{}) error {
	query := fmt.Sprintf("select %s from %s where %s=?", o.SelectFields(), o.TableName(), key)
	return db.Get(o.MemberPointers(), query, value)
}

func (db DBU) FindByID(o DBObject, value interface{}) error {
	return db.FindBy(o, o.KeyField(), value)
}

func (db DBU) FindSelf(o DBObject) error {
	if len(o.KeyField()) == 0 {
		return ErrNoKeyField
	}
	if o.Key() == 0 {
		return ErrKeyMissing
	}
	return db.FindBy(o, o.KeyField(), o.Key())
}

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

func NewDBU(file string, init bool) (DBU, error) {
	return NewDBUWithHook(file, "", init)
}

func NewDBUWithHook(file, hook string, init bool) (DBU, error) {
	db, err := dbutil.OpenWithHook(file, hook, init)
	return DBU{DB: db}, err
}

// helper to generate sql values placeholders
func Placeholders(n int) string {
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
// if skip_key is true, do not include the key field in the list
func dbFields(obj interface{}, skip_key bool) (table, key, fields string) {
	t := reflect.TypeOf(obj)
	list := make([]string, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if is_table := f.Tag.Get("table"); len(is_table) > 0 {
			table = is_table
		}
		k := f.Tag.Get("sql")
		if f.Tag.Get("key") == "true" {
			key = k
			if skip_key {
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
func objFields(obj interface{}, skip_key bool) (interface{}, []interface{}) {
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
			if skip_key {
				continue
			}
		}
		a = append(a, val.Field(i).Interface())
	}
	return key, a
}

func (db DBU) ObjectInsert(obj interface{}) (int64, error) {
	skip := !keyIsSet(obj) // if we have a key, we should probably use it
	_, a := objFields(obj, skip)
	table, _, fields := dbFields(obj, skip)
	if len(table) == 0 {
		return -1, fmt.Errorf("no table defined for object: %v (fields: %s)", reflect.TypeOf(obj), fields)
	}
	query := fmt.Sprintf("insert into %s (%s) values (%s)", table, fields, Placeholders(len(a)))
	result, err := db.DB.Exec(query, a...)
	if result != nil {
		id, _ := result.LastInsertId()
		return id, err
	}
	return -1, err
}

func (db DBU) ObjectUpdate(obj interface{}) error {
	var table, key string
	var id interface{}
	val := reflect.ValueOf(obj)
	t := reflect.TypeOf(obj)
	list := make([]string, 0, t.NumField())
	args := make([]interface{}, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if is_table := f.Tag.Get("table"); len(is_table) > 0 {
			table = is_table
		}
		if len(f.Tag.Get("sql")) == 0 {
			continue
		}
		if f.Tag.Get("update") == "false" {
			continue
		}
		k := f.Tag.Get("sql")
		v := val.Field(i).Interface()
		is_key := f.Tag.Get("key")
		if is_key == "true" {
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
		if is_table := f.Tag.Get("table"); len(is_table) > 0 {
			table = is_table
		}
		if len(f.Tag.Get("sql")) == 0 {
			continue
		}
		if f.Tag.Get("update") == "false" {
			continue
		}
		k := f.Tag.Get("sql")
		v := val.Field(i).Interface()
		is_key := f.Tag.Get("key")
		if is_key == "true" {
			key = k
			id = v
			break
		}
	}
	return
}

func (db DBU) ObjectDelete(obj interface{}) error {
	table, key, id := deleteInfo(obj)
	if len(key) == 0 {
		return ErrNoKeyField
	}
	query := fmt.Sprintf("delete from %s where %s=?", table, key)
	rec, _, err := dbutil.Exec(db.DB, query, id)
	if err != nil {
		return fmt.Errorf("BAD QUERY:%s ID:%v ERROR:%v", query, id, err)
	}
	if rec == 0 {
		return fmt.Errorf("No record deleted for id: %v", id)
	}
	return nil
}

func (db DBU) InsertMany(query string, args [][]interface{}) error {
	return dbutil.InsertMany(db.DB, query, args...)
}

func (db DBU) Print(Query string, args ...interface{}) {
	s, err := db.GetString(Query, args...)
	if err != nil {
		log.Println("ERROR:", err)
	} else {
		log.Println(s)
	}
}

func (db DBU) GetString(query string, args ...interface{}) (string, error) {
	var reply string
	return reply, db.GetType(query, &reply, args...)
}

func (db DBU) GetInt(query string, args ...interface{}) (int, error) {
	var reply int
	return reply, db.GetType(query, &reply, args...)
}

func (db DBU) GetType(query string, reply interface{}, args ...interface{}) error {
	db.debugf(query, args)
	_, err := dbutil.GetResults(db.DB, query, args, reply)
	return err
}

// return list of IDs
func (db DBU) GetIDs(query string, args ...interface{}) ([]int64, error) {
	db.debugf(query, args)
	ids := make([]int64, 0, 32)
	rows, err := db.DB.Query(query, args...)
	if err == nil {
		for rows.Next() {
			var id int64
			if err = rows.Scan(&id); err != nil {
				break
			}
			ids = append(ids, id)
		}
	}
	rows.Close()
	return ids, err
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

func (db DBU) LoadMany(query string, Kind interface{}, args ...interface{}) (error, interface{}) {
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
	return err, s2.Interface()
}

func isNumber(s string) bool {
	// leading zeros is likely a string
	if strings.HasPrefix(s, "00") {
		return false
	}
	return numeric.Match([]byte(strings.TrimSpace(s)))
}

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

func (db DBU) ObjectList(Kind interface{}) (interface{}, error) {
	return db.ObjectListQuery(Kind, "")
}

func (db DBU) LoadMap(what interface{}, Query string, args ...interface{}) interface{} {
	maptype := reflect.TypeOf(what)
	elem := maptype.Elem()
	themap := reflect.MakeMap(maptype)
	index := keyIndex(reflect.Zero(elem).Interface())
	rows, err := db.DB.Query(Query, args...)
	if err != nil {
		log.Println("LoadMap error:" + err.Error())
		return nil
	}
	for rows.Next() {
		v := reflect.New(elem)
		dest := sPtrs(v.Interface())
		err = rows.Scan(dest...)
		k1 := dest[index]
		k2 := reflect.ValueOf(k1)
		key := reflect.Indirect(k2)
		themap.SetMapIndex(key, v.Elem())
	}
	rows.Close()
	return themap.Interface()
}

func startsWith(data, sub string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(data)), strings.ToUpper(sub))
}

func setParams(params string) string {
	list := strings.Split(params, ",")
	for i, p := range list {
		list[i] = fmt.Sprintf("%s=?", p)
	}
	return strings.Join(list, ",")
}

func createQuery(obj interface{}, skip_key bool) string {
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
		if skip_key {
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

func (db DBU) Get(members []interface{}, query string, args ...interface{}) error {
	if db.log != nil {
		db.log.Printf("Q:%s A:%v\n", query, args)
	}
	if db.DB == nil {
		return ErrNilDB
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
