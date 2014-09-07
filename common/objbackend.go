package hummingbird

import (
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

func ReadMetadataFd(fd int) (map[interface{}]interface{}, error) {
	var pickledMetadata [32768]byte
	offset := 0
	for index := 0; ; index += 1 {
		var metadataName string
		if index == 0 {
			metadataName = "user.swift.metadata"
		} else {
			metadataName = fmt.Sprintf("user.swift.metadata%d", index)
		}
		length := FGetXattr(fd, metadataName, pickledMetadata[offset:])
		if length <= 0 {
			break
		}
		offset += length
	}
	v := PickleLoads(pickledMetadata[0:offset])
	return v.(map[interface{}]interface{}), nil
}

func ReadMetadataFilename(filename string) (map[interface{}]interface{}, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, errors.New("Unable to open file.")
	}
	defer file.Close()
	return ReadMetadataFd(int(file.Fd()))
}

func WriteMetadata(fd int, v map[string]interface{}) {
	buf := PickleDumps(v)
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
		FSetXattr(fd, metadataName, []byte(buf[0:writelen]))
		buf = buf[writelen:len(buf)]
	}
}

func InvalidateHash(hashDir string, atomic bool) {
	// TODO: return errors
	suffDir := filepath.Dir(hashDir)
	partitionDir := filepath.Dir(suffDir)
	partitionLock, err := LockPath(partitionDir, 10)
	if err != nil {
		return
	}
	defer partitionLock.Close()
	pklFile := fmt.Sprintf("%s/hashes.pkl", partitionDir)
	data, err := ioutil.ReadFile(pklFile)
	if err != nil {
		return
	}
	v := PickleLoads(data)
	v.(map[string]interface{})[suffDir] = nil
	if atomic {
		WriteFileAtomic(pklFile, PickleDumps(v), 0666)
	} else {
		ioutil.WriteFile(pklFile, PickleDumps(v), 0666)
	}
}

func HashCleanupListdir(hashDir string) ([]string, error) {
	fileList, err := ioutil.ReadDir(hashDir)
	if err != nil {
		return nil, nil
	}
	deleteRest := false
	returnList := []string{}
	for index := len(fileList) - 1; index >= 0; index-- {
		filename := fileList[index].Name()
		if deleteRest {
			os.RemoveAll(fmt.Sprintf("%s/%s", hashDir, filename))
		} else {
			returnList = append(returnList, filename)
			if strings.HasSuffix(filename, ".ts") || strings.HasSuffix(filename, ".data") {
				// TODO: check .ts time for expiration
				deleteRest = true
			}
		}
	}
	return returnList, nil
}

func CleanupHashDir(directory string) {
	_, _ = HashCleanupListdir(directory)
}

func RecalculateSuffixHash(suffixDir string) (string, error) {
	h := md5.New()
	hashList, err := ioutil.ReadDir(suffixDir)
	if err != nil {
		return "", err
	}
	for index := len(hashList) - 1; index >= 0; index-- {
		fileList, err := HashCleanupListdir(fmt.Sprintf("%s/%s", suffixDir, hashList[index]))
		// TODO: handle errors?
		if err != nil {
			return "", err
		}
		for _, fileName := range fileList {
			io.WriteString(h, fileName)
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func GetHashes(driveRoot string, device string, partition string, recalculate []string) (map[string]interface{}, error) {
	// TODO: check for updates and recurse, etc.
	partitionDir := filepath.Join(driveRoot, device, partition)
	pklFile := filepath.Join(partitionDir, "hashes.pkl")
	data, err := ioutil.ReadFile(pklFile)
	if err != nil {
		return nil, err
	}
	v := PickleLoads(data).(map[string]interface{})
	for _, suffix := range recalculate {
		v[suffix] = nil
	}
	for suffix, hash := range v {
		if hash == nil || hash == "" {
			v[suffix], err = RecalculateSuffixHash(fmt.Sprintf("%s/%s/%s/%s", driveRoot, device, partition, suffix))
			if err != nil {
				v[suffix] = nil
			}
		}
	}
	partitionLock, err := LockPath(partitionDir, 10)
	if err != nil {
		return nil, err
	}
	defer partitionLock.Close()
	WriteFileAtomic(pklFile, PickleDumps(v), 0666)
	return v, nil
}

func ObjHashDir(vars map[string]string, driveRoot string, hashPathPrefix string, hashPathSuffix string, checkMounts bool) (string, error) {
	h := md5.New()
	io.WriteString(h, fmt.Sprintf("%s/%s/%s/%s%s", hashPathPrefix, vars["account"],
		vars["container"], vars["obj"], hashPathSuffix))
	hexHash := fmt.Sprintf("%x", h.Sum(nil))
	suffix := hexHash[29:32]
	devicePath := fmt.Sprintf("%s/%s", driveRoot, vars["device"])
	if checkMounts {
		if mounted, err := IsMount(devicePath); err != nil || mounted != true {
			return "", errors.New("Not mounted")
		}
	}
	return fmt.Sprintf("%s/%s/%s/%s/%s", devicePath, "objects", vars["partition"], suffix, hexHash), nil
}

func PrimaryFile(directory string) string {
	fileList, err := ioutil.ReadDir(directory)
	if err != nil {
		return ""
	}
	for index := len(fileList) - 1; index >= 0; index-- {
		filename := fileList[index].Name()
		if strings.HasSuffix(filename, ".ts") || strings.HasSuffix(filename, ".data") {
			return filename
		}
	}
	return ""
}
