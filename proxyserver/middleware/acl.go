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
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/troubling/hummingbird/common"
)

// Returns a cleaned ACL header value, validating that it meets the formatting
// requirements for standard Hummingbird ACL strings.
func CleanACL(name string, value string) (string, error) {
	name = strings.ToLower(name)
	var values []string
	for _, rawValue := range strings.Split(value, ",") {
		rawValue = strings.TrimSpace(rawValue)
		if rawValue == "" {
			continue
		}
		if !strings.Contains(rawValue, ":") {
			values = append(values, rawValue)
			continue
		}
		v := strings.SplitN(rawValue, ":", 2)
		v[0] = strings.TrimSpace(v[0])
		v[1] = strings.TrimSpace(v[1])
		if v[0] == "" || !strings.HasPrefix(v[0], ".") {
			values = append(values, rawValue)
		} else if common.StringInSlice(v[0], []string{".r", ".ref", ".referer", ".referrer"}) {
			if strings.Contains(name, "write") {
				return "", fmt.Errorf("Referrers not allowed in write ACL: %s", rawValue)
			}
			negate := false
			if v[1] != "" && strings.HasPrefix(v[1], "-") {
				negate = true
				v[1] = strings.TrimSpace(v[1][1:])
			}
			if v[1] != "" && v[1] != "*" && strings.HasPrefix(v[1], "*") {
				v[1] = strings.TrimSpace(v[1][1:])
			}
			if v[1] == "" || v[1] == "." {
				return "", fmt.Errorf("No host/domain value after referrer designation in ACL: %s", rawValue)
			}
			if negate {
				values = append(values, fmt.Sprintf(".r:-%s", v[1]))
			} else {
				values = append(values, fmt.Sprintf(".r:%s", v[1]))
			}
		} else {
			return "", fmt.Errorf("Unknown designator %s in ACL: %s", v[0], rawValue)
		}
	}
	return strings.Join(values, ","), nil

}

// Parses a standard Hummingbird ACL string into a referrers list and groups list.
func ParseACL(acl string) ([]string, []string) {
	referrers := []string{}
	groups := []string{}
	if acl != "" {
		for _, value := range strings.Split(acl, ",") {
			if strings.HasPrefix(value, ".r:") {
				referrers = append(referrers, value[len(".r:"):])
			} else {
				groups = append(groups, value)
			}
		}
	}
	return referrers, groups
}

//Returns True if the referrer should be allowed based on the referrerACL list
func ReferrerAllowed(referrer string, referrerACL []string) bool {
	allow := false
	if len(referrerACL) > 0 {
		rHost := "unknown"
		if u, err := url.Parse(referrer); err == nil {
			rHost = u.Hostname()
		}
		for _, mHost := range referrerACL {
			if strings.HasPrefix(mHost, "-") {
				mHost = mHost[1:]
				if mHost == rHost || (strings.HasPrefix(mHost, ".") && strings.HasSuffix(rHost, mHost)) {
					allow = false
				}
			} else if mHost == "*" || mHost == rHost || (strings.HasPrefix(mHost, ".") && strings.HasSuffix(rHost, mHost)) {
				allow = true
			}
		}
	}
	return allow
}

func AuthorizeUnconfirmedIdentity(r *http.Request, obj string, referrers []string, roles []string) (bool, error) {
	if ReferrerAllowed(r.Referer(), referrers) {
		if obj != "" || common.StringInSlice(".rlistings", roles) {
			return true, nil
		}
		return false, nil
	}
	return false, errors.New("unable to confirm identity")
}
