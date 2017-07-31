package middleware

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/uber-go/tally"
)

func NewBulk(config conf.Section, metricsScope tally.Scope) (func(http.Handler) http.Handler, error) {
	yieldFrequency := time.Duration(time.Duration(config.GetInt("yield_frequency", 10)) * time.Second) // for yielding prepending whitespace to keep a client response alive
	maxContainersPerExtraction := int(config.GetInt("max_containers_per_extraction", 10000))
	maxFailedExtractions := int(config.GetInt("max_failed_extractions", 1000))
	maxDeletesPerRequest := int(config.GetInt("max_deletes_per_request", 10000))
	maxFailedDeletes := int(config.GetInt("max_failed_deletes", 1000))
	// TODO: We may implement these later:
	// delete_concurrency
	// delete_container_retry_count
	RegisterInfo("bulk_upload", map[string]interface{}{
		"max_containers_per_extraction": maxContainersPerExtraction,
		"max_failed_extractions":        maxFailedExtractions,
	})
	RegisterInfo("bulk_delete", map[string]interface{}{
		"max_deletes_per_request": maxDeletesPerRequest,
		"max_failed_deletes":      maxFailedDeletes,
	})
	return bulk(metricsScope, yieldFrequency, maxContainersPerExtraction, maxFailedExtractions, maxDeletesPerRequest, maxFailedDeletes), nil
}

func bulk(metricsScope tally.Scope, yieldFrequency time.Duration, maxContainersPerExtraction, maxFailedExtractions, maxDeletesPerRequest, maxFailedDeletes int) func(next http.Handler) http.Handler {
	putRequestsMetric := metricsScope.Counter("bulk_put_requests")
	deleteRequestsMetric := metricsScope.Counter("bulk_delete_requests")
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			switch request.Method {
			case "PUT":
				var f func(r io.Reader, f func(name string, header http.Header, reader io.Reader)) error
				switch request.URL.Query().Get("extract-archive") {
				case "tar":
					f = processBulkTar
				case "tar.gz":
					f = processBulkTarGz
				case "tar.bz2":
					f = processBulkTarBz2
				}
				if f != nil {
					(&bulkPut{
						next:                       next,
						requestsMetric:             putRequestsMetric,
						yieldFrequency:             yieldFrequency,
						maxContainersPerExtraction: maxContainersPerExtraction,
						maxFailedExtractions:       maxFailedExtractions,
						processBodyFunc:            f,
					}).ServeHTTP(writer, request)
					return
				}
			case "DELETE", "POST":
				if _, ok := request.URL.Query()["bulk-delete"]; ok {
					(&bulkDelete{
						next:                 next,
						requestsMetric:       deleteRequestsMetric,
						yieldFrequency:       yieldFrequency,
						maxDeletesPerRequest: maxDeletesPerRequest,
						maxFailedDeletes:     maxFailedDeletes,
					}).ServeHTTP(writer, request)
					return
				}
			default:
			}
			next.ServeHTTP(writer, request)
		})
	}
}

type bulkPut struct {
	next                       http.Handler
	requestsMetric             tally.Counter
	yieldFrequency             time.Duration
	maxContainersPerExtraction int
	maxFailedExtractions       int
	processBodyFunc            func(r io.Reader, f func(name string, header http.Header, reader io.Reader)) error
}

func (b *bulkPut) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	contentType := request.Header.Get("Content-Type")
	if ok, _ := strconv.ParseBool(request.Header.Get("X-Detect-Content-Type")); ok {
		contentType = ""
	}
	accept := request.Header.Get("Accept")
	outputType := "text"
	if strings.Contains(accept, "/json") {
		outputType = "json"
	} else if strings.Contains(accept, "/xml") {
		outputType = "xml"
	}
	writer.WriteHeader(http.StatusOK)
	if outputType == "xml" {
		writer.Write([]byte(xml.Header))
	}
	stopTheSpaces := make(chan struct{})
	hasEmittedSpaces := make(chan bool)
	go func() {
		spacesWereEmitted := false
		for {
			select {
			case <-time.After(b.yieldFrequency):
				writer.Write([]byte("  "))
				spacesWereEmitted = true
			case <-stopTheSpaces:
				hasEmittedSpaces <- spacesWereEmitted
				close(hasEmittedSpaces)
				return
			}
		}
	}()
	ctx := GetProxyContext(request)
	numberFilesCreated := 0
	failures := [][]string{}
	failureResponseType := http.StatusBadRequest
	failureResponseBody := ""
	containerPuts := map[string]bool{}
	processItemFunc := func(name string, header http.Header, reader io.Reader) {
		if len(failures) >= b.maxFailedExtractions || len(containerPuts) > b.maxContainersPerExtraction {
			return
		}
		subpath := path.Join(request.URL.Path, name)
		apiRequest, account, container, object := getPathSegments(subpath)
		if object == "" {
			return
		}
		containerPath := "/" + path.Join(apiRequest, account, container)
		if !containerPuts[containerPath] {
			containerPuts[containerPath] = true
			if len(containerPuts) > b.maxContainersPerExtraction {
				failures = append(failures, []string{subpath, httpStatusString(http.StatusBadRequest)})
				failureResponseType = http.StatusBadRequest
				failureResponseBody = fmt.Sprintf("More than %d containers to create from tar.", b.maxContainersPerExtraction)
				return
			}
			// We continue no matter what because the future object PUT can
			// still succeed, such as when the container PUTs aren't authorized
			// but the container already exists.
			subreq, err := ctx.newSubrequest("PUT", containerPath, reader, request, "bulkput")
			if err != nil {
				failures = append(failures, []string{containerPath, httpStatusString(http.StatusInternalServerError)})
			} else {
				subrec := httptest.NewRecorder()
				ctx.serveHTTPSubrequest(subrec, subreq)
				subresp := subrec.Result()
				subresp.Body.Close()
				if subresp.StatusCode/100 != 2 {
					failures = append(failures, []string{containerPath, httpStatusString(subresp.StatusCode)})
				}
			}
		}
		subreq, err := ctx.newSubrequest("PUT", subpath, reader, request, "bulkput")
		if err != nil {
			failures = append(failures, []string{subpath, httpStatusString(http.StatusInternalServerError)})
			return
		}
		if contentType != "" {
			subreq.Header.Set("Content-Type", contentType)
		}
		// We just do Set as none of hummingbird supports multi-value headers.
		for k := range header {
			subreq.Header.Set(k, header.Get(k))
		}
		subrec := httptest.NewRecorder()
		ctx.serveHTTPSubrequest(subrec, subreq)
		subresp := subrec.Result()
		subresp.Body.Close()
		if subresp.StatusCode/100 == 5 {
			failures = append(failures, []string{subpath, httpStatusString(subresp.StatusCode)})
			failureResponseType = http.StatusBadGateway
			return
		} else if subresp.StatusCode/100 != 2 {
			failures = append(failures, []string{subpath, httpStatusString(subresp.StatusCode)})
			return
		}
		numberFilesCreated++
	}
	responseStatus := http.StatusCreated
	responseBody := ""
	if err := b.processBodyFunc(request.Body, processItemFunc); err != nil {
		responseStatus = http.StatusBadGateway
		responseBody = fmt.Sprintf("Invalid Tar File: %s", err)
	} else if len(failures) > 0 {
		responseStatus = failureResponseType
		responseBody = failureResponseBody
	} else if numberFilesCreated < 1 {
		responseStatus = http.StatusBadRequest
		responseBody = "Invalid Tar File: No Valid Files"
	}
	close(stopTheSpaces)
	if <-hasEmittedSpaces {
		// Not sure why, but the Swift code uses \r\n here and \n everywhere else.
		writer.Write([]byte("\r\n\r\n"))
	}
	switch outputType {
	case "json":
		type js struct {
			ResponseStatus     string `json:"Response Status"`
			ResponseBody       string `json:"Response Body"`
			NumberFilesCreated int    `json:"Number Files Created"`
			Errors             [][]string
		}
		j := &js{
			ResponseStatus:     fmt.Sprintf("%d %s", responseStatus, http.StatusText(responseStatus)),
			ResponseBody:       responseBody,
			NumberFilesCreated: numberFilesCreated,
			Errors:             [][]string{},
		}
		j.Errors = failures
		b, err := json.Marshal(j)
		if err != nil {
			writer.Write([]byte(fmt.Sprintf("JSON encoding error: %s\n%#v\n", err, j)))
		}
		writer.Write(b)
		writer.Write([]byte("\n"))
	case "xml":
		type errorObject struct {
			Name   string `xml:"name"`
			Status string `xml:"status"`
		}
		type replaceNameWithDeleteLowercase struct {
			ResponseStatus     string `xml:"response_status"`
			ResponseBody       string `xml:"response_body"`
			NumberFilesCreated int    `xml:"number_files_created"`
			Error              struct {
				Object []*errorObject `xml:"object"`
			} `xml:"errors"`
		}
		x := &replaceNameWithDeleteLowercase{}
		x.ResponseStatus = fmt.Sprintf("%d %s", responseStatus, http.StatusText(responseStatus))
		x.ResponseBody = responseBody
		x.NumberFilesCreated = numberFilesCreated
		for _, failure := range failures {
			x.Error.Object = append(x.Error.Object, &errorObject{failure[0], failure[1]})
		}
		b, err := xml.Marshal(x)
		if err != nil {
			writer.Write([]byte(fmt.Sprintf("XML encoding error: %s\n%#v\n", err, x)))
		}
		// Yes, even the PUTs are labeled <delete> in the XML from Swift's code.
		b = bytes.Replace(b, []byte("replaceNameWithDeleteLowercase"), []byte("delete"), -1)
		writer.Write(b)
		writer.Write([]byte("\n"))
	default:
		writer.Write([]byte(fmt.Sprintf("Response Status: %d %s\n", responseStatus, http.StatusText(responseStatus))))
		writer.Write([]byte(fmt.Sprintf("Response Body: %s\n", responseBody)))
		writer.Write([]byte(fmt.Sprintf("Number Files Created: %d\n", numberFilesCreated)))
		writer.Write([]byte("Errors:\n"))
		for _, failure := range failures {
			writer.Write([]byte(fmt.Sprintf("%s, %s\n", failure[0], failure[1])))
		}
	}
	return
}

type bulkDelete struct {
	next                 http.Handler
	requestsMetric       tally.Counter
	yieldFrequency       time.Duration
	maxDeletesPerRequest int
	maxFailedDeletes     int
}

func (b *bulkDelete) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	accept := request.Header.Get("Accept")
	outputType := "text"
	if strings.Contains(accept, "/json") {
		outputType = "json"
	} else if strings.Contains(accept, "/xml") {
		outputType = "xml"
	}
	writer.WriteHeader(http.StatusOK)
	if outputType == "xml" {
		writer.Write([]byte(xml.Header))
	}
	stopTheSpaces := make(chan struct{})
	hasEmittedSpaces := make(chan bool)
	go func() {
		spacesWereEmitted := false
		for {
			select {
			case <-time.After(b.yieldFrequency):
				writer.Write([]byte("  "))
				spacesWereEmitted = true
			case <-stopTheSpaces:
				hasEmittedSpaces <- spacesWereEmitted
				close(hasEmittedSpaces)
				return
			}
		}
	}()
	ctx := GetProxyContext(request)
	apiReq, account, _, _ := getPathSegments(request.URL.Path)
	numberDeleted := 0
	numberNotFound := 0
	failures := [][]string{}
	failureResponseType := http.StatusBadRequest
	failureResponseBody := ""
	containersToDelete := []string{}
	scanner := bufio.NewScanner(request.Body)
	// "/c/o\n" *3 because everything could be url-encoded excepting the newline
	maxLineLength := (common.MAX_CONTAINER_NAME_LENGTH+common.MAX_OBJECT_NAME_LENGTH+2)*3 + 1
	scanner.Buffer(make([]byte, maxLineLength), maxLineLength)
	for scanner.Scan() {
		if numberDeleted+numberNotFound+len(failures) > b.maxDeletesPerRequest {
			break
		}
		if len(failures) > b.maxFailedDeletes {
			break
		}
		subpath := scanner.Text()
		u, err := url.Parse(subpath)
		if err != nil {
			failures = append(failures, []string{subpath, httpStatusString(http.StatusBadRequest)})
			continue
		}
		subpath = u.Path
		parts := strings.SplitN(subpath, "/", 3)
		switch len(parts) {
		case 0, 1:
			failures = append(failures, []string{subpath, httpStatusString(http.StatusBadRequest)})
			continue
		case 2:
			containersToDelete = append(containersToDelete, parts[1])
			continue
		}
		subreq, err := ctx.newSubrequest("DELETE", "/"+path.Join(apiReq, account, subpath), nil, request, "bulkdelete")
		if err != nil {
			failures = append(failures, []string{subpath, httpStatusString(http.StatusInternalServerError)})
			return
		}
		subrec := httptest.NewRecorder()
		ctx.serveHTTPSubrequest(subrec, subreq)
		subresp := subrec.Result()
		subresp.Body.Close()
		if subresp.StatusCode/100 == 5 {
			failures = append(failures, []string{subpath, httpStatusString(subresp.StatusCode)})
			failureResponseType = http.StatusBadGateway
		} else if subresp.StatusCode == http.StatusNotFound {
			numberNotFound++
		} else if subresp.StatusCode/100 != 2 {
			failures = append(failures, []string{subpath, httpStatusString(subresp.StatusCode)})
		} else {
			numberDeleted++
		}
	}
	for _, container := range containersToDelete {
		if numberDeleted+numberNotFound+len(failures) > b.maxDeletesPerRequest {
			break
		}
		if len(failures) > b.maxFailedDeletes {
			break
		}
		subpath := "/" + container
		subreq, err := ctx.newSubrequest("DELETE", "/"+path.Join(apiReq, account, subpath), nil, request, "bulkdelete")
		if err != nil {
			failures = append(failures, []string{subpath, httpStatusString(http.StatusInternalServerError)})
			return
		}
		subrec := httptest.NewRecorder()
		ctx.serveHTTPSubrequest(subrec, subreq)
		subresp := subrec.Result()
		subresp.Body.Close()
		if subresp.StatusCode/100 == 5 {
			failures = append(failures, []string{subpath, httpStatusString(subresp.StatusCode)})
			failureResponseType = http.StatusBadGateway
		} else if subresp.StatusCode == http.StatusNotFound {
			numberNotFound++
		} else if subresp.StatusCode/100 != 2 {
			failures = append(failures, []string{subpath, httpStatusString(subresp.StatusCode)})
		} else {
			numberDeleted++
		}
	}
	responseStatus := http.StatusOK
	responseBody := ""
	if err := scanner.Err(); err != nil {
		responseStatus = http.StatusBadGateway
		responseBody = fmt.Sprintf("Invalid bulk delete: %s", err)
	} else if len(failures) > 0 {
		responseStatus = failureResponseType
		responseBody = failureResponseBody
	} else if numberDeleted < 1 && numberNotFound < 1 {
		responseStatus = http.StatusBadRequest
		responseBody = "Invalid bulk delete."
	} else if numberDeleted+numberNotFound+len(failures) > b.maxDeletesPerRequest {
		responseStatus = http.StatusRequestEntityTooLarge
		responseBody = fmt.Sprintf("Maximum Bulk Deletes: %d per request", b.maxDeletesPerRequest)
	}
	close(stopTheSpaces)
	if <-hasEmittedSpaces {
		// Not sure why, but the Swift code uses \r\n here and \n everywhere else.
		writer.Write([]byte("\r\n\r\n"))
	}
	switch outputType {
	case "json":
		type js struct {
			ResponseStatus string `json:"Response Status"`
			ResponseBody   string `json:"Response Body"`
			NumberDeleted  int    `json:"Number Deleted"`
			NumberNotFound int    `json:"Number Not Found"`
			Errors         [][]string
		}
		j := &js{
			ResponseStatus: fmt.Sprintf("%d %s", responseStatus, http.StatusText(responseStatus)),
			ResponseBody:   responseBody,
			NumberDeleted:  numberDeleted,
			NumberNotFound: numberNotFound,
			Errors:         [][]string{},
		}
		j.Errors = failures
		b, err := json.Marshal(j)
		if err != nil {
			writer.Write([]byte(fmt.Sprintf("JSON encoding error: %s\n%#v\n", err, j)))
		}
		writer.Write(b)
		writer.Write([]byte("\n"))
	case "xml":
		type errorObject struct {
			Name   string `xml:"name"`
			Status string `xml:"status"`
		}
		type replaceNameWithDeleteLowercase struct {
			ResponseStatus string `xml:"response_status"`
			ResponseBody   string `xml:"response_body"`
			NumberDeleted  int    `xml:"number_deleted"`
			NumberNotFound int    `xml:"number_not_found"`
			Error          struct {
				Object []*errorObject `xml:"object"`
			} `xml:"errors"`
		}
		x := &replaceNameWithDeleteLowercase{}
		x.ResponseStatus = fmt.Sprintf("%d %s", responseStatus, http.StatusText(responseStatus))
		x.ResponseBody = responseBody
		x.NumberDeleted = numberDeleted
		x.NumberNotFound = numberNotFound
		for _, failure := range failures {
			x.Error.Object = append(x.Error.Object, &errorObject{failure[0], failure[1]})
		}
		b, err := xml.Marshal(x)
		if err != nil {
			writer.Write([]byte(fmt.Sprintf("XML encoding error: %s\n%#v\n", err, x)))
		}
		b = bytes.Replace(b, []byte("replaceNameWithDeleteLowercase"), []byte("delete"), -1)
		writer.Write(b)
		writer.Write([]byte("\n"))
	default:
		writer.Write([]byte(fmt.Sprintf("Response Status: %d %s\n", responseStatus, http.StatusText(responseStatus))))
		writer.Write([]byte(fmt.Sprintf("Response Body: %s\n", responseBody)))
		writer.Write([]byte(fmt.Sprintf("Number Deleted: %d\n", numberDeleted)))
		writer.Write([]byte(fmt.Sprintf("Number Not Found: %d\n", numberNotFound)))
		writer.Write([]byte("Errors:\n"))
		for _, failure := range failures {
			writer.Write([]byte(fmt.Sprintf("%s, %s\n", failure[0], failure[1])))
		}
	}
	return
}

func processBulkTar(r io.Reader, f func(name string, header http.Header, reader io.Reader)) error {
	t := tar.NewReader(r)
	for {
		h, err := t.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if h.Typeflag != tar.TypeReg && h.Typeflag != tar.TypeRegA {
			continue
		}
		if len(h.Name) == 0 || h.Name[len(h.Name)-1] == '/' {
			continue
		}
		header := http.Header{}
		for k, v := range h.Xattrs {
			if k == "user.mime_type" {
				header.Set("Content-Type", v)
			} else if strings.HasPrefix(k, "user.meta.") {
				header.Set("X-Object-Meta-"+strings.Title(strings.Replace(k[len("user.meta."):], ".", "-", -1)), v)
			}
		}
		header.Set("Content-Length", strconv.FormatInt(h.Size, 10))
		n := h.Name
		if strings.HasPrefix(n, "./") {
			n = n[len("./"):]
		}
		f(n, header, t)
	}
	return nil
}

func processBulkTarGz(r io.Reader, f func(name string, header http.Header, reader io.Reader)) error {
	g, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	return processBulkTar(g, f)
}

func processBulkTarBz2(r io.Reader, f func(name string, header http.Header, reader io.Reader)) error {
	return processBulkTar(bzip2.NewReader(r), f)
}

func httpStatusString(status int) string {
	return fmt.Sprintf("%d %s", status, http.StatusText(status))
}
