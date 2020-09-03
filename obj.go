package dbobj

import (
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/pkg/errors"
)

var (
	// ErrNoKeyField is returned for tables without primary key identified
	ErrNoKeyField = errors.New("table has no key field")

	// ErrKeyMissing is returned when key value is not set
	ErrKeyMissing = errors.New("key is not set")

	ErrNilWritePointers = errors.New("nil record dest members")

	singleQuote = regexp.MustCompile("'")
)

// Common Rows object between rqlite and /pkg/database/sql
type Common interface {
	Columns() []string
	Next() bool
	Scan(...interface{}) error
}

// SQLDB is a common interface for opening an sql db
type SQLDB func(string) (*sql.DB, error)

// SetHandler returns a slice of value pointer interfaces
// If there are no values to set it returns a nil instead
type SetHandler func() []interface{}

// DBS is an abstracted database interface, intended to work
// with both rqlite and regular sql.DB connections
type DBS interface {
	Query(fn SetHandler, query string, args ...interface{}) error
	Exec(query string, args ...interface{}) (RowsAffected, LastInsertID int64, err error)
}

/*
// fragment to rethink code structure
func commonQuery(rows Common, fn SetHandler) error {
	for rows.Next() {
		dest := fn()
		if dest == nil {
			return ErrNilWritePointers
		}
		if err := rows.Scan(dest...); err != nil {
			return err
		}
	}
	return nil
}
*/

type sqlWrapper struct {
	db *sql.DB
}

// Query satisfies DBS interface
func (s sqlWrapper) Query(fn SetHandler, query string, args ...interface{}) error {
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		dest := fn()
		if dest == nil {
			return ErrNilWritePointers
		}
		if err = rows.Scan(dest...); err != nil {
			return err
		}
	}
	return nil
}

// Exec satisfies DBS interface
func (s sqlWrapper) Exec(query string, args ...interface{}) (rowsAffected, lastInsertID int64, err error) {
	return 0, 0, nil
}

// DBU is a DatabaseUnit
type DBU struct {
	dbs DBS
	log *log.Logger
}

// SetLogger sets the logger for the db
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

/*
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
*/

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
// The functions are generated for each object
// annotated accordingly
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

	// Names returns the struct element names
	Names() []string

	// SelectFields returns the comma separated
	// list of fields to be selected
	SelectFields() string

	// InsertFields returns the comma separated
	// list of fields to be selected
	InsertFields() string

	// Key returns the int64 id value of the object
	Key() int64

	// SetID updates the id of the object
	SetID(int64)

	// InsertValues returns the values of the object to be inserted
	InsertValues() []interface{}

	// InsertValues returns the values of the object to be updated
	UpdateValues() []interface{}

	// MemberPointers  returns a slice of pointers to values
	// for the db scan function
	MemberPointers() []interface{}

	// ModifiedBy returns the user id and timestamp of when the object was last modified
	ModifiedBy(int64, time.Time)
}

// renderedFields is because rqlite doesn't support bind parameters
func renderedFields(values ...interface{}) string {
	var buf strings.Builder
	for i, value := range values {
		if i > 0 {
			buf.WriteString(", ")
		}
		switch value := value.(type) {
		case string:
			value = singleQuote.ReplaceAllString(value, "''")
			buf.WriteString("'")
			buf.WriteString(fmt.Sprint(value))
			buf.WriteString("'")
		default:
			buf.WriteString(fmt.Sprint(value))
		}
	}
	return buf.String()
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

func setParams(params string) string {
	list := strings.Split(params, ",")
	for i, p := range list {
		list[i] = fmt.Sprintf("%s=?", p)
	}
	return strings.Join(list, ",")
}

func insertQuery(o DBObject) string {
	//p := placeholders(len(o.InsertValues()))
	p := renderedFields(o.InsertValues())
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
	_, last_id, err := db.dbs.Exec(insertQuery(o), args...)
	if err == nil {
		o.SetID(last_id)
	}
	return err
}

// Replace will replace an existing object in datastore
func (db DBU) Replace(o DBObject) error {
	args := o.InsertValues()
	_, last_id, err := db.dbs.Exec(replaceQuery(o), args)
	if err != nil {
		o.SetID(last_id)
	}
	return err
}

// Save modified object in datastore
func (db DBU) Save(o DBObject) error {
	_, _, err := db.dbs.Exec(updateQuery(o), o.UpdateValues()...)
	return err
}

// Delete object from datastore
func (db DBU) Delete(o DBObject) error {
	db.debugf(deleteQuery(o), o.Key())
	_, _, err := db.dbs.Exec(deleteQuery(o), o.Key())
	return err
}

// DeleteByID object from datastore by id
func (db DBU) DeleteByID(o DBObject, id interface{}) error {
	db.debugf(deleteQuery(o), id)
	_, _, err := db.dbs.Exec(deleteQuery(o), id)
	return err
}

// List objects from datastore
func (db DBU) List(list DBList) error {
	return db.ListQuery(list, "")
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

// DBList is the interface for a list of db objects
type DBList interface {
	QueryString(extra string) string
	Receivers() []interface{}
}

// ListQuery updates a list of objects
// TODO: handle args/vs no args for rqlite
func (db DBU) ListQuery(list DBList, extra string) error {
	fn := func() []interface{} {
		return list.Receivers()
	}
	query := list.QueryString(extra)
	return db.dbs.Query(fn, query)
}

// NewDBU returns a new DBU
func NewDBU(file string, init bool, opener SQLDB) (DBU, error) {
	db, err := opener(file)
	return DBU{dbs: sqlWrapper{db}}, err
}

// helper to generate sql values placeholders
func placeholders(n int) string {
	a := make([]string, n)
	for i := range a {
		a[i] = "?"
	}
	return strings.Join(a, ",")
}

// get is the low level db wrapper
func (db DBU) get(members []interface{}, query string, args ...interface{}) error {
	if db.log != nil {
		db.log.Printf("Q:%s A:%v\n", query, args)
	}
	fn := func() []interface{} {
		return members
	}
	err := db.dbs.Query(fn, query, args...)
	if err != nil {
		log.Println("error on query: " + query + " -- " + err.Error())
		return nil
	}
	return nil
}
