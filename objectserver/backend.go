package objectserver

import (
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"hummingbird/common"
)

func ReadMetadataFilename(filename string) (map[interface{}]interface{}, error) {
	var pickledMetadata [8192]byte
	offset := 0
	for index := 0; ; index += 1 {
		var metadataName string
		if index == 0 {
			metadataName = "user.swift.metadata"
		} else {
			metadataName = fmt.Sprintf("user.swift.metadata%d", index)
		}
		length, _ := syscall.Getxattr(filename, metadataName, pickledMetadata[offset:])
		if length <= 0 {
			break
		}
		offset += length
	}
	if offset == 0 {
		return nil, errors.New("No metadata data")
	}
	v, err := hummingbird.PickleLoads(pickledMetadata[0:offset])
	if err != nil {
		return nil, err
	}
	return v.(map[interface{}]interface{}), nil
}

func WriteMetadata(fd int, v map[string]interface{}) {
	// TODO: benchmark this with and without chunking up the metadata
	buf := hummingbird.PickleDumps(v)
	for index := 0; len(buf) > 0; index++ {
		var metadataName string
		if index == 0 {
			metadataName = "user.swift.metadata"
		} else {
			metadataName = fmt.Sprintf("user.swift.metadata%d", index)
		}
		writelen := 254
		if len(buf) < writelen {
			writelen = len(buf)
		}
		hummingbird.FSetXattr(fd, metadataName, []byte(buf[0:writelen]))
		buf = buf[writelen:len(buf)]
	}
}

func QuarantineHash(hashDir string) error {
	// FYI- this does not invalidate the hash like swift's version. Please
	// do that yourself
	hash := filepath.Base(hashDir)
	//          drive        objects      partition    suffix       hash
	driveDir := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(hashDir))))
	// TODO: this will need to be slightly more complicated once policies
	quarantineDir := filepath.Join(driveDir, "quarantined", "objects")
	if err := os.MkdirAll(quarantineDir, 0770); err != nil {
		return err
	}
	destDir := filepath.Join(quarantineDir, fmt.Sprintf("%s-%s", hash, hummingbird.UUID()))
	if err := os.Rename(hashDir, destDir); err != nil {
		return err
	}
	return nil
}

func InvalidateHash(hashDir string, atomic bool) {
	// TODO: return errors
	suffDir := filepath.Dir(hashDir)
	partitionDir := filepath.Dir(suffDir)
	partitionLock, err := hummingbird.LockPath(partitionDir, 10)
	if err != nil {
		return
	}
	defer partitionLock.Close()
	pklFile := fmt.Sprintf("%s/hashes.pkl", partitionDir)
	data, err := ioutil.ReadFile(pklFile)
	if err != nil {
		return
	}
	v, _ := hummingbird.PickleLoads(data)
	v.(map[interface{}]interface{})[suffDir] = nil
	if atomic {
		hummingbird.WriteFileAtomic(pklFile, hummingbird.PickleDumps(v), 0600)
	} else {
		ioutil.WriteFile(pklFile, hummingbird.PickleDumps(v), 0600)
	}
}

func HashCleanupListDir(hashDir string, logger hummingbird.LoggingContext) ([]string, *hummingbird.BackendError) {
	fileList, err := ioutil.ReadDir(hashDir)
	returnList := []string{}
	if err != nil {

		if os.IsNotExist(err) {
			return returnList, nil
		}
		if hummingbird.IsNotDir(err) {
			return returnList, &hummingbird.BackendError{err, hummingbird.PathNotDirErrorCode}
		}
		return returnList, &hummingbird.BackendError{err, hummingbird.OsErrorCode}
	}
	deleteRest := false
	deleteRestMeta := false
	if len(fileList) == 1 {
		filename := fileList[0].Name()
		if strings.HasSuffix(filename, ".ts") {
			withoutSuffix := strings.Split(filename, ".")[0]
			if strings.Contains(withoutSuffix, "_") {
				withoutSuffix = strings.Split(withoutSuffix, "_")[0]
			}
			timestamp, _ := strconv.ParseFloat(withoutSuffix, 64)
			if time.Now().Unix()-int64(timestamp) > int64(hummingbird.ONE_WEEK) {
				os.RemoveAll(fmt.Sprintf("%s/%s", hashDir, filename))
				return returnList, nil
			}
		}
		returnList = append(returnList, filename)
	} else {
		for index := len(fileList) - 1; index >= 0; index-- {
			filename := fileList[index].Name()
			if deleteRest {
				os.RemoveAll(fmt.Sprintf("%s/%s", hashDir, filename))
			} else {
				if strings.HasSuffix(filename, ".meta") {
					if deleteRestMeta {
						os.RemoveAll(fmt.Sprintf("%s/%s", hashDir, filename))
						continue
					}
					deleteRestMeta = true
				}
				if strings.HasSuffix(filename, ".ts") || strings.HasSuffix(filename, ".data") {
					// TODO: check .ts time for expiration
					deleteRest = true
				}
				returnList = append(returnList, filename)
			}
		}
	}
	return returnList, nil
}

func RecalculateSuffixHash(suffixDir string, logger hummingbird.LoggingContext) (string, *hummingbird.BackendError) {
	// the is hash_suffix in swift
	h := md5.New()

	hashList, err := ioutil.ReadDir(suffixDir)
	if err != nil {
		if hummingbird.IsNotDir(err) {
			return "", &hummingbird.BackendError{err, hummingbird.PathNotDirErrorCode}
		}
		return "", &hummingbird.BackendError{err, hummingbird.OsErrorCode}
	}
	for _, fullHash := range hashList {
		hashPath := fmt.Sprintf("%s/%s", suffixDir, fullHash.Name())
		fileList, err := HashCleanupListDir(hashPath, logger)
		if err != nil {
			if err.Code == hummingbird.PathNotDirErrorCode {
				if QuarantineHash(hashPath) == nil {
					InvalidateHash(hashPath, true)
				}
				continue
			}
			return "", err
		}
		if len(fileList) > 0 {
			for _, fileName := range fileList {
				io.WriteString(h, fileName)
			}
		} else {
			os.Remove(hashPath) // leaves the suffix (swift removes it but who cares)
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func GetHashes(driveRoot string, device string, partition string, recalculate []string, logger hummingbird.LoggingContext) (map[string]string, *hummingbird.BackendError) {
	/*
			    TODO: this needs to be added later but SAIOs aren't mounted like this
		        devicePath := filepath.Join(driveRoot, device)
				if mounted, err := hummingbird.IsMount(devicePath); err != nil || mounted != true {
					return nil, &hummingbird.BackendError{err, hummingbird.NotMountedErrorCode}
				}
	*/
	partitionDir := filepath.Join(driveRoot, device, "objects", partition)
	pklFile := filepath.Join(partitionDir, "hashes.pkl")

	modified := false
	mtime := int64(-1)
	hashes := make(map[string]string, 4096)
	lsForSuffixes := true
	data, err := ioutil.ReadFile(pklFile)
	if err == nil {
		v, err := hummingbird.PickleLoads(data)
		if err == nil {
			pickledHashes, ok := v.(map[string]string)
			if ok {
				fileInfo, err := os.Stat(pklFile)
				if err == nil {
					mtime = fileInfo.ModTime().Unix()
					lsForSuffixes = false
					for suff, hash := range pickledHashes {
						hashes[suff] = hash
					}
				}
			}
		}
	}
	if lsForSuffixes {
		// couldn't load hashes pickle, start building new one
		suffs, _ := ioutil.ReadDir(partitionDir)

		for _, suff := range suffs {
			suffName := suff.Name()
			if len(suffName) == 3 && hashes[suffName] == "" {
				hashes[suffName] = ""
			}
		}
	}
	for _, suffix := range recalculate {
		if len(suffix) == 3 {
			hashes[suffix] = ""
		}
	}
	for suffix, hash := range hashes {
		if hash == "" {
			modified = true
			suffixDir := fmt.Sprintf("%s/%s/objects/%s/%s", driveRoot, device, partition, suffix)
			recalc_hash, err := RecalculateSuffixHash(suffixDir, logger)
			if err == nil {
				hashes[suffix] = recalc_hash
			} else {
				switch {
				case err.Code == hummingbird.PathNotDirErrorCode:
					delete(hashes, suffix)
				case err.Code == hummingbird.OsErrorCode:
					logger.LogError("Error hashing suffix: %s/%s (%s)", partitionDir, suffix, "asdf")
				}
			}
		}
	}
	if modified {
		partitionLock, err := hummingbird.LockPath(partitionDir, 10)
		defer partitionLock.Close()
		if err != nil {
			return nil, &hummingbird.BackendError{err, hummingbird.LockPathError}
		} else {
			fileInfo, err := os.Stat(pklFile)
			if lsForSuffixes || os.IsNotExist(err) || mtime != fileInfo.ModTime().Unix() {
				hummingbird.WriteFileAtomic(pklFile, hummingbird.PickleDumps(hashes), 0600)
				return hashes, nil
			}
			logger.LogError("Made recursive call to GetHashes: %s", partitionDir)
			return GetHashes(driveRoot, device, partition, recalculate, logger)
		}
	}
	return hashes, nil
}

func ObjHashDir(vars map[string]string, driveRoot string, hashPathPrefix string, hashPathSuffix string, checkMounts bool) (string, error) {
	h := md5.New()
	io.WriteString(h, fmt.Sprintf("%s/%s/%s/%s%s", hashPathPrefix, vars["account"],
		vars["container"], vars["obj"], hashPathSuffix))
	hexHash := fmt.Sprintf("%x", h.Sum(nil))
	suffix := hexHash[29:32]
	devicePath := fmt.Sprintf("%s/%s", driveRoot, vars["device"])
	if checkMounts {
		if mounted, err := hummingbird.IsMount(devicePath); err != nil || mounted != true {
			return "", errors.New("Not mounted")
		}
	}
	return fmt.Sprintf("%s/%s/%s/%s/%s", devicePath, "objects", vars["partition"], suffix, hexHash), nil
}

func ObjectFiles(directory string) (string, string) {
	fileList, err := ioutil.ReadDir(directory)
	metaFile := ""
	if err != nil {
		return "", ""
	}
	for index := len(fileList) - 1; index >= 0; index-- {
		filename := fileList[index].Name()
		if strings.HasSuffix(filename, ".meta") {
			metaFile = filename
		}
		if strings.HasSuffix(filename, ".ts") || strings.HasSuffix(filename, ".data") {
			if metaFile != "" {
				return filepath.Join(directory, filename), filepath.Join(directory, metaFile)
			} else {
				return filepath.Join(directory, filename), ""
			}
		}
	}
	return "", ""
}

func ObjTempDir(vars map[string]string, driveRoot string) string {
	return fmt.Sprintf("%s/%s/%s", driveRoot, vars["device"], "tmp")
}

func ObjectMetadata(dataFile string, metaFile string) (map[interface{}]interface{}, error) {
	datafileMetadata, err := ReadMetadataFilename(dataFile)
	if err != nil {
		return nil, err
	}

	if metaFile == "" {
		return datafileMetadata, nil
	} else {
		metadata, err := ReadMetadataFilename(metaFile)
		if err != nil {
			return nil, err
		}
		for k, v := range datafileMetadata {
			if k == "Content-Length" || k == "Content-Type" || k == "deleted" || k == "Etag" || strings.HasPrefix(k.(string), "X-Object-Sysmeta-") {
				metadata[k] = v
			}
		}
		return metadata, nil
	}
}
