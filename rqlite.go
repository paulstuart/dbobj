// +build rqlite

package dbobj

import (
	rqlite "github.com/rqlite/gorqlite"
)

type rqliteWrapper struct {
	conn *rqlite.Connection
}

func (s rqliteWrapper) Query(fn SetHandler, query string, args ...interface{}) error {
	// TODO: include args!
	// TODO: build query buffer to batch
	queries := []string{query}
	results, err := s.conn.Query(queries)
	if err != nil {
		return err
	}
	for _, result := range results {
		for result.Next() {
			dest := fn()
			if dest == nil {
				return ErrNilWritePointers
			}
			if err = result.Scan(dest...); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s rqliteWrapper) Exec(query string, args ...interface{}) (rowsAffected, lastInsertID int64, err error) {
	return 0, 0, nil
}

func NewRqlite(addr string) (*rqliteWrapper, error) {
	r, err := rqlite.Open(addr)
	return &rqliteWrapper{&r}, err
}
