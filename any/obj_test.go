// +build why

package dbobj

import (
	"database/sql"
	"testing"
	"time"

	"github.com/paulstuart/sqlite"
)

type testStruct struct {
	ID       int64     `sql:"id" key:"true" table:"structs"`
	Name     string    `sql:"name"`
	Kind     int       `sql:"kind"`
	Data     []byte    `sql:"data"`
	Modified time.Time `sql:"modified" update:"false"`
	astring  string
	anint    int
}

func (s *testStruct) Names() []string {
	return []string{
		"ID",
		"Name",
		"Kind",
		"Data",
		"Modified",
	}
}

func (s *testStruct) TableName() string {
	return "structs"
}

func (s *testStruct) KeyField() string {
	return "id"
}

func (s *testStruct) KeyName() string {
	return "ID"
}

func (s *testStruct) InsertFields() string {
	return "name,kind,data"
}

func (s *testStruct) SelectFields() string {
	return "id,name,kind,data,modified"
}

func (s *testStruct) UpdateValues() []interface{} {
	return []interface{}{s.Name, s.Kind, s.Data, s.ID}
}

func (s *testStruct) MemberPointers() []interface{} {
	return []interface{}{&s.ID, &s.Name, &s.Kind, &s.Data, &s.Modified}
}

func (s *testStruct) InsertValues() []interface{} {
	return []interface{}{s.Name, s.Kind, s.Data}
}

func (s *testStruct) SetID(id int64) {
	s.ID = id
}

func (s *testStruct) Key() int64 {
	return s.ID
}

func (s *testStruct) ModifiedBy(u int64, t time.Time) {
	s.Modified = t
}

const queryCreate = `create table if not exists structs (
    id integer not null primary key,
    name text,
    kind int,
    data blob,
    modified   DATETIME DEFAULT CURRENT_TIMESTAMP
);`

type testMap map[int64]testStruct

func structDb(t *testing.T) *sql.DB {
	db, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	prepare(db)
	return db
}

func structDBU(t *testing.T) DBU {
	return DBU{DB: structDb(t)}
}

func TestObjects(t *testing.T) {
	db := structDBU(t)
	s1 := testStruct{
		Name:     "Bobby Tables",
		Kind:     23,
		Data:     []byte("binary data"),
		Modified: time.Now(),
	}
	var err error
	s1.ID, err = db.ObjectInsert(s1)
	if err != nil {
		t.Errorf("OBJ INSERT ERROR: %s", err)
	}
	s2 := testStruct{
		Name:     "Master Blaster",
		Kind:     999,
		Data:     []byte("whatever you like"),
		Modified: time.Now(),
	}
	s2.ID, err = db.ObjectInsert(s2)
	if err != nil {
		t.Errorf("OBJ INSERT ERROR: %s", err)
	}
	s3 := testStruct{
		Name:     "A, Keeper",
		Kind:     123,
		Data:     []byte("stick around"),
		Modified: time.Now(),
	}
	s3.ID, err = db.ObjectInsert(s3)
	if err != nil {
		t.Errorf("OBJ INSERT ERROR: %s", err)
	}
	s1.Kind = 99
	err = db.ObjectUpdate(s1)
	if err != nil {
		t.Errorf("OBJ UPDATE ERROR: %s", err)
	}
	s2.Name = "New Name"
	err = db.ObjectUpdate(s2)
	if err != nil {
		t.Errorf("OBJ UPDATE ERROR: %s", err)
	}
	err = db.ObjectDelete(s2)
	if err != nil {
		t.Errorf("OBJ DELETE ERROR: %s", err)
	}
}

func TestFindBy(t *testing.T) {
	db := structDBU(t)
	s := testStruct{}
	if err := db.FindBy(&s, "name", "Bobby Tables"); err != nil {
		t.Error(err)
	}
	t.Log("BY NAME", s)
	u := testStruct{}
	if err := db.FindBy(&u, "id", 1); err != nil {
		t.Error(err)
	}
	t.Log("BY ID", u)
}

func TestSelf(t *testing.T) {
	db := structDBU(t)
	s := testStruct{ID: 1}
	if err := db.FindSelf(&s); err != nil {
		t.Error(err)
	}
	t.Log("BY SELF", s)
}

func TestDBObject(t *testing.T) {
	db := structDBU(t)
	s := &testStruct{
		Name: "Grammatic, Bro",
		Kind: 2001,
		Data: []byte("lorem ipsum"),
	}
	if err := db.Add(s); err != nil {
		t.Fatal(err)
	}
	s.Kind = 2015
	s.Name = "Void droid"
	if err := db.Save(s); err != nil {
		t.Fatal(err)
	}
	z := testStruct{}
	m := map[string]interface{}{"kind": 2015}
	if err := db.Find(&z, m); err != nil {
		t.Fatal(err)
	}

	if err := db.Delete(s); err != nil {
		t.Fatal(err)
	}
}

func testDBU(t *testing.T) *sql.DB {
	return nil
}

func prepare(db *sql.DB) {
	const queryInsert = "insert into structs(name, kind, data) values(?,?,?)"
	db.Exec(queryCreate)
	db.Exec(queryInsert, "abc", 23, "what ev er")
	db.Exec(queryInsert, "def", 69, "m'kay")
	db.Exec(queryInsert, "hij", 42, "meaning of life")
	db.Exec(queryInsert, "klm", 2, "of a kind")
}

func dump(t *testing.T, db *sql.DB, query string, args ...interface{}) {
	rows, err := db.Query(query)
	if err != nil {
		t.Fatal(err)
	}
	dest := make([]interface{}, len(args))
	for i, f := range args {
		dest[i] = &f
	}
	for rows.Next() {
		rows.Scan(dest...)
		t.Log(args...)
	}
	rows.Close()
}

func errLogger(t *testing.T) chan error {
	e := make(chan error, 4096)
	go func() {
		for err := range e {
			t.Error(err)
		}
	}()
	return e
}

func TestObjectInsert(t *testing.T) {
	db := structDBU(t)
	s := testStruct{
		Name: "Blur",
		Kind: 13,
	}
	i, err := db.ObjectInsert(s)
	if err != nil {
		t.Error(err)
	}
	if !(i > 0) {
		t.Errorf("expected last row to be greater than zero: %d", i)
	}
}
