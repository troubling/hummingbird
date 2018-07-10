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
	"strconv"
	"strings"
	"time"
)

const (
	MAX_FILE_SIZE             = int64(5368709122)
	MAX_META_NAME_LENGTH      = 128
	MAX_META_VALUE_LENGTH     = 256
	MAX_META_COUNT            = 90
	MAX_META_OVERALL_SIZE     = 4096
	MAX_HEADER_SIZE           = 8192
	MAX_OBJECT_NAME_LENGTH    = 1024
	CONTAINER_LISTING_LIMIT   = 10000
	ACCOUNT_LISTING_LIMIT     = 10000
	MAX_ACCOUNT_NAME_LENGTH   = 256
	MAX_CONTAINER_NAME_LENGTH = 256
	EXTRA_HEADER_COUNT        = 0
)

var DEFAULT_CONSTRAINTS = map[string]interface{}{
	"max_file_size":             MAX_FILE_SIZE,
	"max_meta_name_length":      MAX_META_NAME_LENGTH,
	"max_meta_value_length":     MAX_META_VALUE_LENGTH,
	"max_meta_count":            MAX_META_COUNT,
	"max_meta_overall_size":     MAX_META_OVERALL_SIZE,
	"max_header_size":           MAX_HEADER_SIZE,
	"max_object_name_length":    MAX_OBJECT_NAME_LENGTH,
	"container_listing_limit":   CONTAINER_LISTING_LIMIT,
	"account_listing_limit":     ACCOUNT_LISTING_LIMIT,
	"max_account_name_length":   MAX_ACCOUNT_NAME_LENGTH,
	"max_container_name_length": MAX_CONTAINER_NAME_LENGTH,
	"extra_header_count":        EXTRA_HEADER_COUNT,
}

var OwnerHeaders = map[string]bool{
	"x-container-read":                true,
	"x-container-write":               true,
	"x-container-sync-key":            true,
	"x-container-sync-to":             true,
	"x-account-meta-temp-url-key":     true,
	"x-account-meta-temp-url-key-2":   true,
	"x-container-meta-temp-url-key":   true,
	"x-container-meta-temp-url-key-2": true,
	"x-account-access-control":        true,
}

var ErrBadRequest = errors.New("bad request")
var ErrNotFound = errors.New("not found")
var ErrConflict = errors.New("conflict")
var ErrDisconnect = errors.New("disconnect")

func CheckMetadata(req *http.Request, targetType string) (int, string) {
	metaCount := 0
	metaSize := 0
	metaPrefix := fmt.Sprintf("X-%s-Meta-", targetType)
	fixKeys := make(map[string]string)
	for key := range req.Header {
		value := req.Header.Get(key)
		if len(value) > MAX_HEADER_SIZE {
			errStr := fmt.Sprintf("Header value too long: %s", key)
			if len(key) > MAX_META_NAME_LENGTH {
				errStr = fmt.Sprintf("Header value too long: %s", key[:MAX_META_NAME_LENGTH])
			}
			return http.StatusBadRequest, errStr
		}
		if !strings.HasPrefix(key, metaPrefix) {
			continue
		}
		key = key[len(metaPrefix):]
		metaCount += 1
		metaSize += len(key) + len(value)
		if key == "" {
			return http.StatusBadRequest, "Metadata name cannot be empty"
		}
		if StringInSlice(targetType, []string{"Account", "Container"}) && (strings.Contains(key, "\x00") || strings.Contains(value, "\x00")) {
			return http.StatusBadRequest, "Metadata must be valid UTF-8"
		}
		if len(key) > MAX_META_NAME_LENGTH {
			return http.StatusBadRequest, fmt.Sprintf("Metadata name too long: %s%s", metaPrefix, key)
		}
		if len(value) > MAX_META_VALUE_LENGTH {
			return http.StatusBadRequest, fmt.Sprintf("Metadata value longer than %d: %s%s", MAX_META_VALUE_LENGTH, metaPrefix, key)
		}
		if metaCount > MAX_META_COUNT {
			return http.StatusBadRequest, fmt.Sprintf("Too many metadata items; max %d", MAX_META_COUNT)
		}
		if metaSize > MAX_META_OVERALL_SIZE {
			return http.StatusBadRequest, fmt.Sprintf("Total metadata too large; max %d", MAX_META_OVERALL_SIZE)
		}
		fixedKey := strings.Replace(key, "_", "-", -1)
		if key != fixedKey {
			fixKeys[key] = fixedKey
		}
	}
	for oldKey, newKey := range fixKeys {
		oldKey = metaPrefix + oldKey
		newKey = metaPrefix + newKey
		req.Header.Set(newKey, req.Header.Get(oldKey))
		req.Header.Del(oldKey)
	}
	return http.StatusOK, ""
}

func handleObjDeleteHeaders(req *http.Request) (int, string) {
	if xda := req.Header.Get("X-Delete-At"); xda != "" {
		if deleteAfter, err := strconv.ParseInt(xda, 10, 64); err != nil {
			return http.StatusBadRequest, "Non-integer X-Delete-At"
		} else if deleteAfter < time.Now().Unix() {
			return http.StatusBadRequest, "X-Delete-At in past"
		}
	} else if xda := req.Header.Get("X-Delete-After"); xda != "" {
		if deleteAfter, err := strconv.ParseInt(xda, 10, 64); err != nil {
			return http.StatusBadRequest, "Non-integer X-Delete-After"
		} else if deleteAfter < 0 {
			return http.StatusBadRequest, "X-Delete-After in past"
		} else {
			req.Header.Set("X-Delete-At", strconv.FormatInt(time.Now().Unix()+deleteAfter, 10))
		}
	}
	return http.StatusOK, ""
}

func CheckObjPost(req *http.Request, objectName string) (int, string) {
	if status, msg := handleObjDeleteHeaders(req); status != http.StatusOK {
		return status, msg
	}
	return CheckMetadata(req, "Object")
}

func CheckObjPut(req *http.Request, objectName string) (int, string) {
	if req.ContentLength > MAX_FILE_SIZE {
		return http.StatusRequestEntityTooLarge, "Your request is too large."
	}
	if req.Header.Get("X-Copy-From") != "" && req.ContentLength != 0 {
		return http.StatusBadRequest, "Copy requests require a zero byte body"
	}
	if req.Header.Get("Content-Length") == "" && !StringInSlice("chunked", req.TransferEncoding) {
		return http.StatusLengthRequired, "Missing Content-Length header."
	}
	if len(objectName) > MAX_OBJECT_NAME_LENGTH {
		return http.StatusBadRequest, fmt.Sprintf("Object name length of %d longer than %d", len(objectName), MAX_OBJECT_NAME_LENGTH)
	}
	if req.Header.Get("Content-Type") == "" {
		return http.StatusBadRequest, "No content type"
	}
	if status, msg := handleObjDeleteHeaders(req); status != http.StatusOK {
		return status, msg
	}
	if strings.Contains(req.Header.Get("Content-Type"), "\x00") {
		return http.StatusBadRequest, "Invalid Content-Type"
	}
	return CheckMetadata(req, "Object")
}

func CheckContainerPut(req *http.Request, containerName string) (int, string) {
	if len(containerName) > MAX_CONTAINER_NAME_LENGTH {
		return http.StatusBadRequest, fmt.Sprintf("Container name length of %d longer than %d", len(containerName), MAX_CONTAINER_NAME_LENGTH)
	}
	return CheckMetadata(req, "Container")
}
