//  Copyright (c) 2015 Rackspace
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

package common

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckNameFormat(t *testing.T) {
	req, err := http.NewRequest("PUT", "/v1/a/c/o", nil)
	s := "asdf"
	target := "ignored"

	checked, err := CheckNameFormat(req, s, target)
	require.Nil(t, err)
	require.Equal(t, checked, s)
}

func TestNoNameSlashes(t *testing.T) {
	req, err := http.NewRequest("PUT", "/v1/a/c/o", nil)
	s := "as/df"
	target := "ignored"

	checked, err := CheckNameFormat(req, s, target)
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "ignored name cannot contain slashes")
	require.Equal(t, checked, "")
}

func TestNoEmptyName(t *testing.T) {
	req, err := http.NewRequest("PUT", "/v1/a/c/o", nil)
	s := ""
	target := "ignored"

	checked, err := CheckNameFormat(req, s, target)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "ignored name cannot be empty")
	require.Equal(t, checked, "")
}
