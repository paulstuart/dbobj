package main

import (
	"time"
)

type testStruct struct {
	ID      int64     `sql:"id" key:"true" table:"teststruct"`
	Name    string    `sql:"name"`
	Kind    int       `sql:"kind"`
	Data    []byte    `sql:"data"`
	Created time.Time `sql:"created" update:"false" audit:"time"`
}

// make lint happy, it can't otherwise detect its use
// but that's in generated output
var _ = testStruct{}

const testSchema = `create table teststruct (
	id integer not null primary key,
	name text,
	kind int,
	data blob,
	created     DATETIME DEFAULT CURRENT_TIMESTAMP
);`
