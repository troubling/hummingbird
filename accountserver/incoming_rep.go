//  Copyright (c) 2016-2017 Rackspace
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

package accountserver

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/troubling/hummingbird/common/fs"
	"github.com/troubling/hummingbird/common/srv"
	"go.uber.org/zap"
)

func isOkayFilename(s string) bool {
	if len(s) < 5 || len(s) > 100 {
		return false
	}
	for _, c := range s {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
			(c == '-') || (c == '_') || (c == '.')) {
			return false
		}
	}
	return true
}

// TmpUploadHandler handles uploading account files to the tmp directory for various replication strategies.
// This replaces the swift replicator's use of rsync.
func (server *AccountServer) TmpUploadHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	if !isOkayFilename(vars["filename"]) {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	filename := filepath.Join(server.driveRoot, vars["device"], "tmp", vars["filename"])
	if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	fp, err := os.Create(filename)
	if err != nil {
		srv.GetLogger(request).LogError("Unable to create file.",
			zap.String("filename", filename),
			zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	defer fp.Close()
	if _, err := io.Copy(fp, request.Body); err != nil {
		os.RemoveAll(filename)
		srv.GetLogger(request).LogError("Error saving file contents.",
			zap.String("filename", filename),
			zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
	} else {
		srv.StandardResponse(writer, http.StatusCreated)
	}
}

// AccountReplicateHandler handles the REPLICATE call for accounts.
func (server *AccountServer) AccountReplicateHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	// make sure there's a tmp dir to rsync to
	if err := os.MkdirAll(filepath.Join(server.driveRoot, vars["device"], "tmp"), 0777); err != nil {
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	message := []json.RawMessage{}
	decoder := json.NewDecoder(request.Body)
	if err := decoder.Decode(&message); err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	var op string
	if err := json.Unmarshal(message[0], &op); err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	extractArgs := func(args ...interface{}) error {
		if len(message)-1 < len(args) {
			return errors.New("Not enough arguments in payload.")
		}
		for i, arg := range args {
			if err := json.Unmarshal(message[i+1], arg); err != nil {
				return err
			}
		}
		return nil
	}
	switch op {
	case "rsync_then_merge":
		var tmpFileName string
		if err := extractArgs(&tmpFileName); err != nil {
			srv.StandardResponse(writer, http.StatusBadRequest)
		} else {
			status := server.replicateRsyncThenMerge(request, vars, tmpFileName)
			srv.StandardResponse(writer, status)
		}
	case "complete_rsync":
		var tmpFileName string
		if err := extractArgs(&tmpFileName); err != nil {
			srv.StandardResponse(writer, http.StatusBadRequest)
		} else {
			status := server.replicateCompleteRsync(request, vars, tmpFileName)
			srv.StandardResponse(writer, status)
		}
	case "merge_items":
		var records []*ContainerRecord
		var remoteID string
		if err := extractArgs(&records, &remoteID); err != nil {
			srv.StandardResponse(writer, http.StatusBadRequest)
		} else {
			status := server.replicateMergeItems(request, vars, records, remoteID)
			srv.StandardResponse(writer, status)
		}
	case "merge_syncs":
		var records []*SyncRecord
		if err := extractArgs(&records); err != nil {
			srv.StandardResponse(writer, http.StatusBadRequest)
		} else {
			status := server.replicateMergeSyncs(request, vars, records)
			srv.StandardResponse(writer, status)
		}
	case "sync":
		var maxRow int64
		var hash, id, createdAt, putTimestamp, deleteTimestamp, metadata string
		if err := extractArgs(&maxRow, &hash, &id, &createdAt, &putTimestamp, &deleteTimestamp, &metadata); err != nil {
			srv.StandardResponse(writer, http.StatusBadRequest)
		} else if status, data := server.replicateSync(request, vars, maxRow, hash, id, createdAt, putTimestamp, deleteTimestamp, metadata); status == http.StatusOK {
			writer.WriteHeader(http.StatusOK)
			writer.Write(data)
		} else {
			srv.StandardResponse(writer, status)
		}
	default:
		srv.GetLogger(request).LogError("Unknown replication op.", zap.String("op", op))
		srv.StandardResponse(writer, http.StatusBadRequest)
	}
}

func (server *AccountServer) replicateRsyncThenMerge(request *http.Request, vars map[string]string, tmpFileName string) int {
	accountFile := filepath.Join(server.driveRoot, vars["device"], "accounts", vars["partition"], vars["hash"][29:32], vars["hash"], vars["hash"]+".db")
	tmpAccountFile := filepath.Join(server.driveRoot, vars["device"], "tmp", tmpFileName)
	tmpDb, err := sqliteOpenAccount(tmpAccountFile)
	if err != nil {
		return http.StatusNotFound
	}
	defer tmpDb.Close()
	localDb, err := server.accountEngine.GetByHash(vars["device"], vars["hash"], vars["partition"])
	if err != nil {
		return http.StatusNotFound
	}
	defer localDb.Close()
	point := int64(-1)
	for {
		records, err := localDb.ItemsSince(point, 10000)
		if err != nil {
			srv.GetLogger(request).LogError("Error fetching items.",
				zap.String("accountFile", accountFile),
				zap.Error(err))
			return http.StatusInternalServerError
		}
		if len(records) == 0 {
			break
		}
		point = records[len(records)-1].Rowid
		if err := tmpDb.MergeItems(records, ""); err != nil {
			srv.GetLogger(request).LogError("Error merging items.",
				zap.String("tmpAccountFile", tmpAccountFile),
				zap.Error(err))
			return http.StatusInternalServerError
		}
	}
	if tmpDb.NewID() != nil || os.MkdirAll(filepath.Dir(accountFile), 0777) != nil || os.Rename(tmpAccountFile, accountFile) != nil {
		srv.GetLogger(request).LogError("Error blessing new account db.",
			zap.String("accountFile", accountFile))
		return http.StatusInternalServerError
	}
	server.accountEngine.Invalidate(localDb)
	return http.StatusNoContent
}

func (server *AccountServer) replicateCompleteRsync(request *http.Request, vars map[string]string, tmpFileName string) int {
	accountFile := filepath.Join(server.driveRoot, vars["device"], "accounts", vars["partition"], vars["hash"][29:32], vars["hash"], vars["hash"]+".db")
	tmpAccountFile := filepath.Join(server.driveRoot, vars["device"], "tmp", tmpFileName)
	if !fs.Exists(tmpAccountFile) || fs.Exists(accountFile) {
		return http.StatusNotFound
	}
	tmpDb, err := sqliteOpenAccount(tmpAccountFile)
	if err != nil {
		return http.StatusNotFound
	}
	defer tmpDb.Close()
	if tmpDb.NewID() != nil || os.MkdirAll(filepath.Dir(accountFile), 0777) != nil || os.Rename(tmpAccountFile, accountFile) != nil {
		srv.GetLogger(request).LogError("Error blessing new account db.",
			zap.String("accountFile", accountFile))
		return http.StatusInternalServerError
	}
	return http.StatusNoContent
}

func (server *AccountServer) replicateMergeItems(request *http.Request, vars map[string]string, records []*ContainerRecord, remoteID string) int {
	db, err := server.accountEngine.GetByHash(vars["device"], vars["hash"], vars["partition"])
	if err != nil {
		return http.StatusNotFound
	}
	defer server.accountEngine.Return(db)
	if err := db.MergeItems(records, remoteID); err != nil {
		srv.GetLogger(request).LogError("Error merging records.",
			zap.String("db.RingHash", db.RingHash()),
			zap.Error(err))
		return http.StatusInternalServerError
	}
	return http.StatusAccepted
}

func (server *AccountServer) replicateMergeSyncs(request *http.Request, vars map[string]string, records []*SyncRecord) int {
	db, err := server.accountEngine.GetByHash(vars["device"], vars["hash"], vars["partition"])
	if err != nil {
		return http.StatusNotFound
	}
	defer server.accountEngine.Return(db)
	if err := db.MergeSyncTable(records); err != nil {
		srv.GetLogger(request).LogError("Error merging sync table.",
			zap.String("db.RingHash", db.RingHash()),
			zap.Error(err))
		return http.StatusInternalServerError
	}
	return http.StatusAccepted
}

func (server *AccountServer) replicateSync(request *http.Request, vars map[string]string, maxRow int64, hash, id, createdAt, putTimestamp, deleteTimestamp, metadata string) (int, []byte) {
	db, err := server.accountEngine.GetByHash(vars["device"], vars["hash"], vars["partition"])
	if err != nil {
		return http.StatusNotFound, nil
	}
	defer server.accountEngine.Return(db)
	info, err := db.SyncRemoteData(maxRow, hash, id, createdAt, putTimestamp, deleteTimestamp, metadata)
	if err != nil {
		srv.GetLogger(request).LogError("Error syncing remote data.",
			zap.String("vars['hash']", vars["hash"]),
			zap.Error(err))
		return http.StatusInternalServerError, nil
	}
	response, err := json.Marshal(info)
	if err != nil {
		srv.GetLogger(request).LogError("Error marshaling info.",
			zap.String("vars['hash']", vars["hash"]),
			zap.Error(err))
		return http.StatusInternalServerError, nil
	}
	return http.StatusOK, response
}
