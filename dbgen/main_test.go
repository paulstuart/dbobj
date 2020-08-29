package main

import (
	"database/sql/driver"
	"os"
	"testing"

	sqlite "github.com/paulstuart/sqlite"
)

const (
	test_file = "test.db"
)

func (o *testStruct) IV() []driver.Value {
	return []driver.Value{o.Name, o.Kind, o.Data, o.Created}
}

func TestInit(t *testing.T) {
	var err error
	os.Remove(test_file)
	test_db, err := sqlite.Open(test_file)
	if err != nil {
		t.Fatal(err)
	}
	_, err = test_db.Exec(testSchema)
	if err != nil {
		t.Fatal(err)
	}
}
