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

package tools

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArg0(t *testing.T) {
	arg0 := "a/c/o"
	a, c, o := parseArg0(arg0)

	require.Equal(t, "a", a)
	require.Equal(t, "c", c)
	require.Equal(t, "o", o)

	arg0 = "a/c"
	a, c, o = parseArg0(arg0)

	require.Equal(t, "a", a)
	require.Equal(t, "c", c)
	require.Equal(t, "", o)

	arg0 = "a"
	a, c, o = parseArg0(arg0)

	require.Equal(t, "a", a)
	require.Equal(t, "", c)
	require.Equal(t, "", o)

	arg0 = "a/c/o/1/2/3"
	a, c, o = parseArg0(arg0)

	require.Equal(t, "a", a)
	require.Equal(t, "c", c)
	require.Equal(t, "o/1/2/3", o)

	arg0 = "/v1/a/c/o"
	a, c, o = parseArg0(arg0)

	require.Equal(t, "a", a)
	require.Equal(t, "c", c)
	require.Equal(t, "o", o)
}
