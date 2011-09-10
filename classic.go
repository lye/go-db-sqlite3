// Copyright 2009 Peter H. Froehlich. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite3

// We use the "classic" stuff without channels to implement
// the nicer, more Go-like channel-based stuff. Officially
// the "classic" API is optional, but we really need it. :-D

// TODO: If someone Close()s the statement under us, we'll
// have to handle that. :-/

import (
	"db"
	"os"
	"reflect"
)

// Execute precompiled statement with given parameters
// (if any). The statement stays valid even if we fail
// to execute with given parameters.
func (self *Connection) ExecuteClassic(statement db.Statement, parameters ...interface{}) (rset db.ClassicResultSet, error os.Error) {
	s, ok := statement.(*Statement)
	if !ok {
		error = &DriverError{"Execute: Not an sqlite3 statement!"}
		return
	}

	p := reflect.ValueOf(parameters)

	if p.Len() != s.handle.sqlBindParameterCount() {
		error = &DriverError{"Execute: Number of parameters doesn't match!"}
		return
	}

	for k, param := range parameters {
		v := reflect.ValueOf(param)
		rc := 1

		switch v.Kind() {
			case reflect.Bool:
				rc = s.handle.sqlBindInt64(k, v.Int())
			case reflect.Int:
				rc = s.handle.sqlBindInt64(k, v.Int())
			case reflect.Int8:
				rc = s.handle.sqlBindInt64(k, v.Int())
			case reflect.Int16:
				rc = s.handle.sqlBindInt64(k, v.Int())
			case reflect.Int32:
				rc = s.handle.sqlBindInt64(k, v.Int())
			case reflect.Int64:
				rc = s.handle.sqlBindInt64(k, v.Int())
			case reflect.String:
				rc = s.handle.sqlBindText(k, v.String())
			case reflect.Float32:
				rc = s.handle.sqlBindDouble(k, v.Float())
			case reflect.Float64:
				rc = s.handle.sqlBindDouble(k, v.Float())
			default:
				panic("XXX: Don't know how to bind " + v.Type().Name())
		}

		if rc != StatusOk {
			error = self.error()
			s.clear()
			return
		}
	}

	rc := s.handle.sqlStep()

	if rc != StatusDone && rc != StatusRow {
		// presumably any other outcome is an error
		error = self.error()
	}

	if rc == StatusRow {
		// statement is producing results, need a cursor
		rs := new(ClassicResultSet)
		rs.statement = s
		rs.connection = self
		rs.more = true
		rset = rs
	} else if rc == StatusDone {
		// even if there are no results, we should still return a result set
		rs := new(ClassicResultSet)
		rs.statement = s
		rs.connection = self
		rs.more = false
		rset = rs
		s.clear()
	} else {
		// clean up after error
		s.clear()
	}

	return
}

// TODO
type ClassicResultSet struct {
	statement  *Statement
	connection *Connection
	more       bool // still have results left
}

// TODO
func (self *ClassicResultSet) More() bool {
	return self.more
}

// Fetch another result. Once results are exhausted, the
// the statement that produced them will be reset and
// ready for another execution.
func (self *ClassicResultSet) Fetch() (result db.Result) {
	res := new(Result)
	result = res

	if !self.more {
		res.error = &DriverError{"Fetch: No result to fetch!"}
		return
	}

	// assemble results from current row
	nColumns := self.statement.handle.sqlColumnCount()
	if nColumns <= 0 {
		res.error = &DriverError{"Fetch: No columns in result!"}
		return
	}
	res.data = make([]interface{}, nColumns)
	for i := 0; i < nColumns; i++ {
		res.data[i] = self.statement.handle.sqlColumnText(i)
	}

	// try to get another row
	rc := self.statement.handle.sqlStep()

	if rc != StatusDone && rc != StatusRow {
		// presumably any other outcome is an error
		// TODO: is res.error the right place?
		res.error = self.connection.error()
	}

	if rc == StatusDone {
		self.more = false
		// clean up when done
		self.statement.clear()
	}

	return
}

// TODO
// TODO: reset statement here as well, just like in Fetch
func (self *ClassicResultSet) Close() os.Error {
	return nil
}

// TODO
// TODO: what if something goes wrong? error? :-/
func (self *ClassicResultSet) Names() (names []string) {
	cols := self.statement.handle.sqlColumnCount()
	if cols == 0 {
		return
	}
	names = make([]string, cols)
	for i := 0; i < cols; i++ {
		names[i] = self.statement.handle.sqlColumnName(i)
	}
	return
}

func (self *ClassicResultSet) Types() (names []string) {
	cols := self.statement.handle.sqlColumnCount()
	if cols == 0 {
		return
	}
	names = make([]string, cols)
	for i := 0; i < cols; i++ {
		names[i] = self.statement.handle.sqlColumnDeclaredType(i)
	}
	return
}
