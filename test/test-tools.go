// Copyright 2014 ISRG.  All rights reserved
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copied from https://github.com/letsencrypt/boulder/blob/master/test/test-tools.go
//
// See Q5 and Q11 here: https://www.mozilla.org/en-US/MPL/2.0/FAQ/ I think if
// we want to modify this file we have to release it publicly, otherwise we're
// fine.
package test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

// Return short format caller info for printing errors, so errors don't all
// appear to come from test-tools.go.
func caller() string {
	_, file, line, _ := runtime.Caller(2)
	splits := strings.Split(file, "/")
	filename := splits[len(splits)-1]
	return fmt.Sprintf("%s:%d:", filename, line)
}

// Assert a boolean
func Assert(t testing.TB, result bool, message string) {
	if !result {
		t.Fatalf("%s %s", caller(), message)
	}
}

// AssertNotNil checks an object to be non-nil
func AssertNotNil(t testing.TB, obj interface{}, message string) {
	if obj == nil {
		t.Fatalf("%s %s", caller(), message)
	}
}

// AssertNotError checks that err is nil
func AssertNotError(t testing.TB, err error, message string) {
	if err != nil {
		t.Fatalf("%s %s: %s", caller(), message, err)
	}
}

// AssertError checks that err is non-nil
func AssertError(t testing.TB, err error, message string) {
	if err == nil {
		t.Fatalf("%s %s: expected error but received none", caller(), message)
	}
}

// AssertEquals uses the equality operator (==) to measure one and two
func AssertEquals(t testing.TB, one interface{}, two interface{}) {
	if one != two {
		t.Fatalf("%s [%v] != [%v]", caller(), one, two)
	}
}

// AssertDeepEquals uses the reflect.DeepEqual method to measure one and two
func AssertDeepEquals(t testing.TB, one interface{}, two interface{}) {
	if !reflect.DeepEqual(one, two) {
		t.Fatalf("%s [%+v] !(deep)= [%+v]", caller(), one, two)
	}
}

// AssertMarshaledEquals marshals one and two to JSON, and then uses
// the equality operator to measure them
func AssertMarshaledEquals(t testing.TB, one interface{}, two interface{}) {
	oneJSON, err := json.Marshal(one)
	AssertNotError(t, err, "Could not marshal 1st argument")
	twoJSON, err := json.Marshal(two)
	AssertNotError(t, err, "Could not marshal 2nd argument")

	if !bytes.Equal(oneJSON, twoJSON) {
		t.Fatalf("%s [%s] !(json)= [%s]", caller(), oneJSON, twoJSON)
	}
}

// AssertNotEquals uses the equality operator to measure that one and two
// are different
func AssertNotEquals(t testing.TB, one interface{}, two interface{}) {
	if one == two {
		t.Fatalf("%s [%v] == [%v]", caller(), one, two)
	}
}

// AssertByteEquals uses bytes.Equal to measure one and two for equality.
func AssertByteEquals(t testing.TB, one []byte, two []byte) {
	if !bytes.Equal(one, two) {
		t.Fatalf("%s Byte [%s] != [%s]",
			caller(),
			base64.StdEncoding.EncodeToString(one),
			base64.StdEncoding.EncodeToString(two))
	}
}

// AssertIntEquals uses the equality operator to measure one and two.
func AssertIntEquals(t testing.TB, one int, two int) {
	if one != two {
		t.Fatalf("%s Int [%d] != [%d]", caller(), one, two)
	}
}

// AssertBigIntEquals uses the big.Int.cmp() method to measure whether
// one and two are equal
func AssertBigIntEquals(t testing.TB, one *big.Int, two *big.Int) {
	if one.Cmp(two) != 0 {
		t.Fatalf("%s Int [%d] != [%d]", caller(), one, two)
	}
}

// AssertContains determines whether needle can be found in haystack
func AssertContains(t testing.TB, haystack string, needle string) {
	if !strings.Contains(haystack, needle) {
		t.Fatalf("%s String [%s] does not contain [%s]", caller(), haystack, needle)
	}
}

// AssertNotContains determines if needle is not found in haystack
func AssertNotContains(t testing.TB, haystack string, needle string) {
	if strings.Contains(haystack, needle) {
		t.Fatalf("%s String [%s] contains [%s]", caller(), haystack, needle)
	}
}

// AssertSeverity determines if a string matches the Severity formatting
func AssertSeverity(t testing.TB, data string, severity int) {
	expected := fmt.Sprintf("\"severity\":%d", severity)
	AssertContains(t, data, expected)
}

// AssertBetween determines if a is between b and c
func AssertBetween(t testing.TB, a, b, c int64) {
	if a < b || a > c {
		t.Fatalf("%d is not between %d and %d", a, b, c)
	}
}
