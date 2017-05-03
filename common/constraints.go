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
	"errors"
	"fmt"
	"net/http"
	"strings"
)

func CheckNameFormat(req *http.Request, name string, target string) (string, error) {
	if name == "" {
		return "", errors.New(fmt.Sprintf("%s name cannot be empty", target))
	}
	if strings.Contains(name, "/") {
		return "", errors.New(fmt.Sprintf("%s name cannot contain slashes", target))
	}
	return name, nil
}
