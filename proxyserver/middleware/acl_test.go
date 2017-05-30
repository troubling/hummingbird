//  Copyright (c) 2017 Rackspace
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
//  implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package middleware

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCleanACL(t *testing.T) {
	var tests = []struct {
		s        string // input
		expected string // expected result
	}{
		{"bob, sue", "bob,sue"},
		{"bob , sue", "bob,sue"},
		{"bob,,,sue", "bob,sue"},
		{".r:-*.ending.with", ".r:-.ending.with"},
		{".ref:*", ".r:*"},
		{" .r : one , ,, .r:two , .r : - three ", ".r:one,.r:two,.r:-three"},
		{".r:one,.r:-two,account,account:user", ".r:one,.r:-two,account,account:user"},
		{"TEST_account", "TEST_account"},
		{".r:*,.r:-specific.host", ".r:*,.r:-specific.host"},
	}

	for _, tt := range tests {
		actual, err := CleanACL("X-Container-Read", tt.s)
		assert.Nil(t, err)
		if actual != tt.expected {
			t.Errorf("CleanACL(%v): expected %v, actual %v", tt.s, tt.expected, actual)
		}
	}
	var errortests = []struct {
		s        string // input
		expected string // expected result
	}{
		{".unknown:test", "Unknown designator .unknown in ACL: .unknown:test"},
		{".r:", "No host/domain value after referrer designation in ACL: .r:"},
		{".r:*.", "No host/domain value after referrer designation in ACL: .r:*."},
		{".r : * . ", "No host/domain value after referrer designation in ACL: .r : * ."},
		{".r:-*.", "No host/domain value after referrer designation in ACL: .r:-*."},
		{".r : - * . ", "No host/domain value after referrer designation in ACL: .r : - * ."},
		{" .r : ", "No host/domain value after referrer designation in ACL: .r :"},
		{"user , .r : ", "No host/domain value after referrer designation in ACL: .r :"},
		{".r:-", "No host/domain value after referrer designation in ACL: .r:-"},
		{"user , .r : - ", "No host/domain value after referrer designation in ACL: .r : -"},
		{" .r : - ", "No host/domain value after referrer designation in ACL: .r : -"},
	}
	for _, tt := range errortests {
		val, err := CleanACL("X-Container-Read", tt.s)
		assert.Equal(t, val, "")
		if assert.Error(t, err, "An error was expected") {
			assert.Equal(t, err.Error(), tt.expected)
		}
	}
	val, err := CleanACL("X-Container-Write", ".r:r")
	assert.Equal(t, val, "")
	if assert.Error(t, err, "An error was expected") {
		assert.Equal(t, err.Error(), "Referrers not allowed in write ACL: .r:r")
	}

}

func TestParseACL(t *testing.T) {
	var tests = []struct {
		s                 string // input
		expectedRefferers []string
		expectedGroups    []string
	}{
		{"", []string{}, []string{}},
		{".r:ref1", []string{"ref1"}, []string{}},
		{".r:-ref1", []string{"-ref1"}, []string{}},
		{"account:user", []string{}, []string{"account:user"}},
		{"account", []string{}, []string{"account"}},
		{"acc1,acc2:usr2,.r:ref3,.r:-ref4", []string{"ref3", "-ref4"}, []string{"acc1", "acc2:usr2"}},
		{"acc1,acc2:usr2,.r:ref3,acc3,acc4:usr4,.r:ref5,.r:-ref6",
			[]string{"ref3", "ref5", "-ref6"},
			[]string{"acc1", "acc2:usr2", "acc3", "acc4:usr4"}},
	}

	for _, tt := range tests {
		actualRefferers, actualGroups := ParseACL(tt.s)
		if !reflect.DeepEqual(actualRefferers, tt.expectedRefferers) {
			t.Errorf("ParseACL(%v): expected %v, actual %v", tt.s, tt.expectedRefferers, actualRefferers)
		}
		if !reflect.DeepEqual(actualGroups, tt.expectedGroups) {
			t.Errorf("ParseACL(%v): expected %v, actual %v", tt.s, tt.expectedGroups, actualGroups)
		}
	}
}

func TestReferrerAllowed(t *testing.T) {
	var tests = []struct {
		referrer    string
		referrerACL []string
		expected    bool
	}{
		{"host", []string{}, false},
		{"", []string{"*"}, true},
		{"", []string{"specific.host"}, false},
		{"http://www.example.com/index.html", []string{".example.com"}, true},
		{"http://user@www.example.com/index.html", []string{".example.com"}, true},
		{"http://user:pass@www.example.com/index.html", []string{".example.com"}, true},
		{"http://www.example.com:8080/index.html", []string{".example.com"}, true},
		{"http://user@www.example.com:8080/index.html", []string{".example.com"}, true},
		{"http://user:pass@www.example.com:8080/index.html", []string{".example.com"}, true},
		{"http://user:pass@www.example.com:8080", []string{".example.com"}, true},
		{"http://www.example.com", []string{".example.com"}, true},
		{"http://thief.example.com", []string{".example.com", "-thief.example.com"}, false},
		{"http://thief.example.com", []string{"*", "-thief.example.com"}, false},
		{"http://www.example.com", []string{".other.com", "www.example.com"}, true},
		{"http://www.example.com", []string{"-.example.com", "www.example.com"}, true},
		{"www.example.com", []string{".example.com"}, false},
		{"../index.htm", []string{".example.com"}, false},
		{"www.example.com", []string{"*"}, true},
	}

	for _, tt := range tests {
		actual := ReferrerAllowed(tt.referrer, tt.referrerACL)
		if actual != tt.expected {
			t.Errorf("ReferrerAllowed(%v, %v): expected %v, actual %v", tt.referrer, tt.referrerACL, tt.expected, actual)
		}
	}
}
