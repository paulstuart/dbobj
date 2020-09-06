package dbobj

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/paulstuart/sqlite"
	"github.com/pkg/errors"
)

var (
	// ErrNoKeyField is returned for tables without primary key identified
	ErrNoKeyField = errors.New("table has no key field")

	// ErrKeyMissing is returned when key value is not set
	ErrKeyMissing = errors.New("key is not set")

	ErrNilWritePointers = errors.New("nil record dest members")
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

// Query satisfies DBS interface
func (du *DBU) Query(fn SetHandler, query string, args ...interface{}) error {
	rows, err := du.db.Query(query, args...)
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

// DBU is a DataBaseUnit
type DBU struct {
	db  *sql.DB
	mu  sync.RWMutex
	log *log.Logger
}

func (du *DBU) Exec(query string, args ...interface{}) (rowsAffected, lastInsertID int64, err error) {
	var result sql.Result
	// All locking should just happen here to avoid races
	du.mu.Lock()
	result, err = du.db.Exec(query, args...)
	du.mu.Unlock()
	if err != nil {
		return
	}
	rowsAffected, _ = result.RowsAffected()
	lastInsertID, _ = result.LastInsertId()
	return
}

// SetLogger sets the logger for the db
func (du *DBU) SetLogger(logger *log.Logger) {
	du.log = logger
}

func (du *DBU) debugf(msg string, args ...interface{}) {
	if du.log != nil {
		du.log.Printf(msg, args...)
	}
}

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
	p := Placeholders(len(o.InsertValues()))
	return fmt.Sprintf("insert into %s (%s) values(%s)", o.TableName(), insertFields(o), p)
}

func replaceQuery(o DBObject) string {
	p := Placeholders(len(o.InsertValues()))
	return fmt.Sprintf("replace into %s (%s) values(%s)", o.TableName(), insertFields(o), p)
}

func updateQuery(o DBObject) string {
	return fmt.Sprintf("update %s set %s where %s=?", o.TableName(), setParams(insertFields(o)), o.KeyField())
}

func deleteQuery(o DBObject) string {
	return fmt.Sprintf("delete from %s where %s=?", o.TableName(), o.KeyField())
}

// Add new object to datastore
func (du *DBU) Add(o DBObject) error {
	args := o.InsertValues()
	query := insertQuery(o)
	du.debugf("Q: %s A: %v\n", query, args)
	_, last_id, err := du.Exec(query, args...)
	if err == nil {
		o.SetID(last_id)
	}
	return err
}

// Replace will replace an existing object in datastore
func (du *DBU) Replace(o DBObject) error {
	args := o.InsertValues()
	_, last_id, err := du.Exec(replaceQuery(o), args)
	if err != nil {
		o.SetID(last_id)
	}
	return err
}

// Save modified object in datastore
func (du *DBU) Save(o DBObject) error {
	_, _, err := du.Exec(updateQuery(o), o.UpdateValues()...)
	return err
}

// Delete object from datastore
func (du *DBU) Delete(o DBObject) error {
	du.debugf("Q: %s  A: %v\n", deleteQuery(o), o.Key())
	_, _, err := du.Exec(deleteQuery(o), o.Key())
	return err
}

// DeleteByID object from datastore by id
func (du *DBU) DeleteByID(o DBObject, id interface{}) error {
	du.debugf(deleteQuery(o), id)
	_, _, err := du.Exec(deleteQuery(o), id)
	return err
}

// List objects from datastore
func (du *DBU) List(list DBList) error {
	return du.ListQuery(list, "")
}

// Find loads an object matching the given keys
func (du *DBU) Find(o DBObject, keys map[string]interface{}) error {
	where := make([]string, 0, len(keys))
	what := make([]interface{}, 0, len(keys))
	for k, v := range keys {
		where = append(where, k+"=?")
		what = append(what, v)
	}
	query := fmt.Sprintf("select %s from %s where %s", o.SelectFields(), o.TableName(), strings.Join(where, " and "))
	return du.get(o.MemberPointers(), query, what...)
}

// FindBy loads an  object matching the given key/value
func (du *DBU) FindBy(o DBObject, key string, value interface{}) error {
	query := fmt.Sprintf("select %s from %s where %s=?", o.SelectFields(), o.TableName(), key)
	return du.get(o.MemberPointers(), query, value)
}

// FindByID loads an object based on a given ID
func (du *DBU) FindByID(o DBObject, value interface{}) error {
	return du.FindBy(o, o.KeyField(), value)
}

// FindSelf loads an object based on it's current ID
func (du *DBU) FindSelf(o DBObject) error {
	if len(o.KeyField()) == 0 {
		return ErrNoKeyField
	}
	if o.Key() == 0 {
		return ErrKeyMissing
	}
	return du.FindBy(o, o.KeyField(), o.Key())
}

// DBList is the interface for a list of db objects
type DBList interface {
	QueryString(extra string) string
	Receivers() []interface{}
}

// ListQuery updates a list of objects
// TODO: handle args/vs no args for rqlite
func (du *DBU) ListQuery(list DBList, extra string) error {
	fn := func() []interface{} {
		return list.Receivers()
	}
	query := list.QueryString(extra)
	return du.Query(fn, query)
}

// NewDBU returns a new DBU
func NewDBU(file string, init bool, opener SQLDB) (*DBU, error) {
	db, err := opener(file)
	//return &DBU{dbs: sqlWrapper{db}}, err
	return &DBU{db: db}, err
}

// Placeholders is a helper to generate sql values placeholders
func Placeholders(n int) string {
	a := make([]string, n)
	for i := range a {
		a[i] = "?"
	}
	return strings.Join(a, ",")
}

// get is the low level db wrapper
func (du *DBU) get(members []interface{}, query string, args ...interface{}) error {
	du.debugf("Q: %s A:%v\n", query, args)
	fn := func() []interface{} {
		return members
	}
	err := du.Query(fn, query, args...)
	if err != nil {
		log.Println("error on query: " + query + " -- " + err.Error())
		return nil
	}
	return nil
}

// DB returns the *sql.DB
func (du *DBU) DB() *sql.DB {
	return du.db
}

// InsertMany inserts multiple records as a single transaction
func (du *DBU) InsertMany(query string, args ...[]interface{}) error {
	tx, err := du.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(query)
	if err != nil {
		if e := tx.Rollback(); e != nil {
			log.Printf("prepare rollback error: %v\n", e)
		}
		return err
	}
	defer stmt.Close()
	for _, arg := range args {
		if _, err = stmt.Exec(arg...); err != nil {
			if e := tx.Rollback(); e != nil {
				log.Printf("exec rollback error: %v\n", e)
			}
			return err
		}
	}
	return tx.Commit()
}

// Close shuts down the database
func (du *DBU) Close() {
	if du.db != nil {
		sqlite.Close(du.db)
		du.db = nil
	}
}
