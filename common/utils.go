package hummingbird

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log/syslog"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/vaughan0/go-ini"
)

var (
	PathNotDirErrorCode = 1
	OsErrorCode         = 2
	NotMountedErrorCode = 3
	LockPathError       = 4
	BlowUp              = 5 // :)
	ONE_WEEK            = 604800
)

type BackendError struct {
	Err  error
	Code int
}

func (e BackendError) Error() string {
	return fmt.Sprintf("%s ( %d )", e.Err, e.Code)
}

type httpRange struct {
	Start, End int64
}

var GMT *time.Location

type IniFile struct{ ini.File }

func (f IniFile) GetDefault(section string, key string, dfl string) string {
	if value, ok := f.Get(section, key); ok {
		return value
	}
	return dfl
}

func (f IniFile) GetBool(section string, key string, dfl bool) bool {
	if value, ok := f.Get(section, key); ok {
		return LooksTrue(value)
	}
	return dfl
}

func (f IniFile) GetInt(section string, key string, dfl int64) int64 {
	if value, ok := f.Get(section, key); ok {
		if val, err := strconv.ParseInt(value, 10, 64); err == nil {
			return val
		}
		panic(fmt.Sprintf("Error parsing integer %s/%s from config.", section, key))
	}
	return dfl
}

func LoadIniFile(filename string) (IniFile, error) {
	file := IniFile{make(ini.File)}
	return file, file.LoadFile(filename)
}

func WriteFileAtomic(filename string, data []byte, perm os.FileMode) error {
	partDir := filepath.Dir(filename)
	tmpFile, err := ioutil.TempFile(partDir, ".tmp-o-file")
	if err != nil {
		return err
	}
	defer tmpFile.Close()
	defer os.RemoveAll(tmpFile.Name())
	if err = tmpFile.Chmod(perm); err != nil {
		return err
	}
	if _, err = tmpFile.Write(data); err != nil {
		return err
	}
	if err = tmpFile.Sync(); err != nil {
		return err
	}
	if err = syscall.Rename(tmpFile.Name(), filename); err != nil {
		return err
	}
	return nil
}

func LockPath(directory string, timeout int) (*os.File, error) {
	sleepTime := 5
	lockfile := filepath.Join(directory, ".lock")
	file, err := os.OpenFile(lockfile, os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		if os.IsNotExist(err) && os.MkdirAll(directory, 0770) == nil {
			file, err = os.OpenFile(lockfile, os.O_RDWR|os.O_CREATE, 0660)
		}
		if file == nil {
			return nil, errors.New(fmt.Sprintf("Unable to open file ccc. ( %s )", err.Error()))
		}
	}
	for stop := time.Now().Add(time.Duration(timeout) * time.Second); time.Now().Before(stop); {
		err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return file, nil
		}
		time.Sleep(time.Millisecond * time.Duration(sleepTime))
		sleepTime += 5
	}
	file.Close()
	return nil, errors.New("Timed out")
}

func LockParent(file string, timeout int) (*os.File, error) {
	return LockPath(filepath.Dir(file), timeout)
}

func IsMount(dir string) (bool, error) {
	dir = filepath.Clean(dir)
	if fileinfo, err := os.Stat(dir); err == nil {
		if parentinfo, err := os.Stat(filepath.Dir(dir)); err == nil {
			return fileinfo.Sys().(*syscall.Stat_t).Dev != parentinfo.Sys().(*syscall.Stat_t).Dev, nil
		} else {
			return false, errors.New("Unable to stat parent")
		}
	} else {
		return false, errors.New("Unable to stat directory")
	}
}

func Urlencode(str string) string {
	return strings.Replace(url.QueryEscape(str), "+", "%20", -1)
}

func ParseDate(date string) (time.Time, error) {
	if GMT == nil {
		GMT, _ = time.LoadLocation("GMT")
	}
	if ius, err := time.ParseInLocation(time.RFC1123, date, GMT); err == nil {
		return ius, nil
	}
	if ius, err := time.ParseInLocation(time.RFC1123Z, date, GMT); err == nil {
		return ius, nil
	}
	if ius, err := time.ParseInLocation(time.ANSIC, date, GMT); err == nil {
		return ius, nil
	}
	if ius, err := time.ParseInLocation(time.RFC850, date, GMT); err == nil {
		return ius, nil
	}
	if strings.Contains(date, "_") {
		all_date_parts := strings.Split(date, "_")
		date = all_date_parts[0]
	}
	if timestamp, err := strconv.ParseFloat(date, 64); err == nil {
		nans := int64((timestamp - float64(int64(timestamp))) * 1.0e9)
		return time.Unix(int64(timestamp), nans).In(GMT), nil
	}
	return time.Now(), errors.New("invalid time")
}

func FormatTimestamp(timestamp string) (string, error) {
	parsed, err := ParseDate(timestamp)
	if err != nil {
		return "", err
	}
	return parsed.Format("2006-01-02T15:04:05.999999"), nil
}

func LooksTrue(check string) bool {
	check = strings.TrimSpace(strings.ToLower(check))
	return check == "true" || check == "yes" || check == "1" || check == "on" || check == "t" || check == "y"
}

func SetupLogger(facility string, prefix string) *syslog.Writer {
	facility_mapping := map[string]syslog.Priority{"LOG_USER": syslog.LOG_USER,
		"LOG_MAIL": syslog.LOG_MAIL, "LOG_DAEMON": syslog.LOG_DAEMON,
		"LOG_AUTH": syslog.LOG_AUTH, "LOG_SYSLOG": syslog.LOG_SYSLOG,
		"LOG_LPR": syslog.LOG_LPR, "LOG_NEWS": syslog.LOG_NEWS,
		"LOG_UUCP": syslog.LOG_UUCP, "LOG_CRON": syslog.LOG_CRON,
		"LOG_AUTHPRIV": syslog.LOG_AUTHPRIV, "LOG_FTP": syslog.LOG_FTP,
		"LOG_LOCAL0": syslog.LOG_LOCAL0, "LOG_LOCAL1": syslog.LOG_LOCAL1,
		"LOG_LOCAL2": syslog.LOG_LOCAL2, "LOG_LOCAL3": syslog.LOG_LOCAL3,
		"LOG_LOCAL4": syslog.LOG_LOCAL4, "LOG_LOCAL5": syslog.LOG_LOCAL5,
		"LOG_LOCAL6": syslog.LOG_LOCAL6, "LOG_LOCAL7": syslog.LOG_LOCAL7}
	logger, err := syslog.Dial("udp", "127.0.0.1:514", facility_mapping[facility], prefix)
	if err != nil || logger == nil {
		panic(fmt.Sprintf("Unable to dial logger: %s", err))
	}
	return logger
}

func UUID() string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", rand.Int63n(0xffffffff), rand.Int63n(0xffff), rand.Int63n(0xffff), rand.Int63n(0xffff), rand.Int63n(0xffffffffffff))
}

func GetTimestamp() string {
	return fmt.Sprintf("%016.05f", float64(time.Now().UnixNano())/1000000000.0)
}

func GetTransactionId() string {
	return fmt.Sprintf("%x", time.Now().UnixNano())
}

func HeaderGetDefault(h http.Header, key string, dfl string) string {
	val := h.Get(key)
	if val == "" {
		return dfl
	}
	return val
}

func ParseRange(rangeHeader string, fileSize int64) ([]httpRange, error) {
	rangeHeader = strings.Replace(strings.ToLower(rangeHeader), " ", "", -1)
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return nil, nil
	}
	rangeHeader = rangeHeader[6:]
	var reqRanges []httpRange
	rangeStrings := strings.Split(rangeHeader, ",")
	for _, rng := range rangeStrings {
		beginend := strings.Split(rng, "-")
		if len(beginend) != 2 || (beginend[0] == "" && beginend[1] == "") {
			return nil, errors.New("invalid range format")
		}
		if beginend[0] == "" {
			end, err := strconv.ParseInt(beginend[1], 10, 64)
			if err != nil {
				return nil, errors.New("invalid end with no begin")
			}
			if end == 0 {
				return nil, errors.New("zero end with no begin")
			} else if end > fileSize {
				reqRanges = append(reqRanges, httpRange{0, fileSize})
			} else {
				reqRanges = append(reqRanges, httpRange{fileSize - end, fileSize})
			}
		} else if beginend[1] == "" {
			begin, err := strconv.ParseInt(beginend[0], 10, 64)
			if err != nil {
				return nil, errors.New("invalid begin with no end")
			}
			if begin < fileSize {
				reqRanges = append(reqRanges, httpRange{begin, fileSize})
			} else {
				continue
			}
		} else {
			begin, err := strconv.ParseInt(beginend[0], 10, 64)
			if err != nil {
				return nil, errors.New("invalid begin")
			}
			end, err := strconv.ParseInt(beginend[1], 10, 64)
			if err != nil {
				return nil, errors.New("invalid end")
			}
			if end < begin {
				continue // not an error, just ignored.
			}
			if begin > fileSize {
				return nil, errors.New("Begin bigger than file")
			}
			if end+1 < fileSize {
				reqRanges = append(reqRanges, httpRange{begin, end + 1})
			} else {
				reqRanges = append(reqRanges, httpRange{begin, fileSize})
			}
		}
	}
	return reqRanges, nil
}

func UseMaxProcs() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func SetRlimits() {
	syscall.Setrlimit(syscall.RLIMIT_NOFILE, &syscall.Rlimit{65536, 65536})
}

func GetEpochFromTimestamp(timestamp string) (string, error) {
	split_timestamp := strings.Split(timestamp, "_")
	floatTimestamp, err := strconv.ParseFloat(split_timestamp[0], 64)
	if err != nil {
		return "", errors.New(fmt.Sprintf("Could not parse float from '%s'.", split_timestamp[0]))
	}
	return fmt.Sprintf("%016.5f", floatTimestamp), nil
}

func StandardizeTimestamp(timestamp string) (string, error) {
	offset := strings.Contains(timestamp, "_")
	if offset {
		split_timestamp := strings.Split(timestamp, "_")
		floatTimestamp, err := strconv.ParseFloat(split_timestamp[0], 64)
		if err != nil {
			return "", errors.New(fmt.Sprintf("Could not parse float from '%s'.", split_timestamp[0]))
		}
		intOffset, err := strconv.ParseInt(split_timestamp[1], 16, 64)
		if err != nil {
			return "", errors.New(fmt.Sprintf("Could not parse int from '%s'.", split_timestamp[1]))
		}

		split_timestamp[0] = fmt.Sprintf("%016.5f", floatTimestamp)
		split_timestamp[1] = fmt.Sprintf("%016x", intOffset)
		timestamp = strings.Join(split_timestamp, "_")
	} else {
		floatTimestamp, err := strconv.ParseFloat(timestamp, 64)
		if err != nil {
			return "", errors.New(fmt.Sprintf("Could not parse float from '%s'.", timestamp))
		}
		timestamp = fmt.Sprintf("%016.5f", floatTimestamp)
	}
	return timestamp, nil
}

func ValidTimestamp(timestamp string) bool {
	var a, b int
	count, err := fmt.Sscanf(timestamp, "%d.%d", &a, &b)
	return err == nil && count == 2
}

func IsNotDir(err error) bool {
	if se, ok := err.(*os.SyscallError); ok {
		return se.Err == syscall.ENOTDIR
	}
	return false
}

var buf64kpool = NewFreePool(64)

func Copy(src io.Reader, dsts ...io.Writer) (written int64, err error) {
	var buf []byte
	var ok bool
	if buf, ok = buf64kpool.Get().([]byte); !ok {
		buf = make([]byte, 64*1024)
	}
	defer buf64kpool.Put(buf)
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			written += int64(nr)
			for _, dst := range dsts {
				nw, ew := dst.Write(buf[0:nr])
				if ew != nil {
					return written, ew
				}
				if nr != nw {
					return written, io.ErrShortWrite
				}
			}
		}
		if er != nil {
			if er == io.EOF {
				er = nil
			}
			return written, er
		}
	}
}
