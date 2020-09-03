// +build rqlite

package dbobj

import (
	"fmt"
	"testing"

	rqlite "github.com/rqlite/gorqlite"
)

func prepareRqlite(conn *rqlite.Connection) {
	//const queryInsert = "insert into structs(name, kind, data) values('%s',%d, '%s')"
	const queryInsert = "insert into structs(name, kind, data) values(%s)"
	var queries []string
	prep := func(s string, args ...interface{}) {
		//queries = append(queries, (fmt.Sprintf(s, args...)))
		if len(args) == 0 {
			queries = append(queries, s)
			return
		}
		query := fmt.Sprintf(s, renderedFields(args...))
		queries = append(queries, query)
	}
	prep(queryCreate)
	prep(queryInsert, "abc", 23, "what ev er")
	prep(queryInsert, "def", 69, "m'kay")
	prep(queryInsert, "ghi", 42, "meaning of life")
	prep(queryInsert, "jkl", 2, "of a kind")
	prep(queryInsert, "mno", 2, "of a drag")
	prep(queryInsert, "pqr", 2, "of a sort")
	//results, err := conn.Write(queries)
	_, err := conn.Write(queries)
	if err != nil {
		panic(err)
	}
	/*
		for _, result := range results {
			fmt.Printf("RESULT: %+v\n", result)
		}
	*/
}

func structRqlite(t *testing.T) DBU {
	dbs, err := NewRqlite("http://localhost:4001")
	if err != nil {
		t.Fatal(err)
	}
	prepareRqlite(dbs.conn)
	return DBU{dbs: dbs}
}

func TestRqliteQuery(t *testing.T) {
	db := structRqlite(t)
	list := new(_testStruct)
	db.ListQuery(list, "(id % 2) = 0")
	for _, item := range *list {
		t.Logf("ITEM:  %+v\n", item)
	}
}
