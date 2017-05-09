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
	"net/http"

	"go.uber.org/zap"

	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/srv"
)

func Recover(w http.ResponseWriter, r *http.Request, msg string) {
	if err := recover(); err != nil {
		transactionId := r.Header.Get("X-Trans-Id")
		if ctx := GetProxyContext(r); ctx != nil {
			ctx.Logger.Error(msg, zap.Any("err", err), zap.String("txn", transactionId))
			// if we haven't set a status code yet, we can send a 500 response.
			if started, _ := ctx.capWriter.Response(); !started {
				srv.StandardResponse(w, http.StatusInternalServerError)
			}
		}
	}
}

func NewCatchError(config conf.Section) (func(http.Handler) http.Handler, error) {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				defer Recover(w, r, "PANIC")
				next.ServeHTTP(w, r)
			},
		)
	}, nil
}
