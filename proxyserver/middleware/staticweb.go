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
	"bytes"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/srv"
)

func NewStaticWeb(config conf.Section) (func(http.Handler) http.Handler, error) {
	RegisterInfo("staticweb", map[string]interface{}{})
	return staticWeb, nil
}

func staticWeb(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		(&staticWebHandler{next: next}).ServeHTTP(writer, request)
	})
}

type staticWebHandler struct {
	next             http.Handler
	ctx              *ProxyContext
	account          string
	container        string
	object           string
	webIndex         string
	webError         string
	webListings      bool
	webListingsLabel string
	webListingsCSS   string
	webDirType       string
}

func (s *staticWebHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != "GET" && request.Method != "HEAD" {
		s.next.ServeHTTP(writer, request)
		return
	}
	s.ctx = GetProxyContext(request)
	if s.ctx.Source == "staticweb" {
		s.next.ServeHTTP(writer, request)
		return
	}
	if s.ctx.RemoteUser != "" && !common.LooksTrue(request.Header.Get("X-Web-Mode")) {
		s.next.ServeHTTP(writer, request)
		return
	}
	var apiReq bool
	apiReq, s.account, s.container, s.object = getPathParts(request)
	if !apiReq || s.account == "" || s.container == "" {
		s.next.ServeHTTP(writer, request)
		return
	}
	ci := s.ctx.C.GetContainerInfo(s.account, s.container)
	if ci == nil {
		s.next.ServeHTTP(writer, request)
		return
	}
	s.webIndex = strings.TrimSpace(ci.Metadata["Web-Index"])
	s.webError = strings.TrimSpace(ci.Metadata["Web-Error"])
	s.webListings = common.LooksTrue(strings.TrimSpace(ci.Metadata["Web-Listings"]))
	s.webListingsLabel = strings.TrimSpace(ci.Metadata["Web-Listings-Label"])
	s.webListingsCSS = strings.TrimSpace(ci.Metadata["Web-Listings-Css"])
	s.webDirType = strings.TrimSpace(ci.Metadata["Web-Directory-Type"])
	if s.webDirType == "" {
		s.webDirType = "application/directory"
	}
	if s.object != "" {
		s.handleObject(writer, request)
		return
	}
	s.ctx.ACL = ci.ReadACL
	if s.ctx.Authorize != nil && !s.ctx.Authorize(request) {
		if s.ctx.RemoteUser != "" {
			srv.StandardResponse(writer, 403)
			return
		}
		srv.StandardResponse(writer, 401)
		return
	}
	s.handleDirectory(writer, request)
}

func (s *staticWebHandler) handleObject(writer http.ResponseWriter, request *http.Request) {
	subreq, err := s.ctx.newSubrequest("HEAD", request.URL.Path, nil, request, "staticweb")
	if err != nil {
		s.handleError(writer, request, http.StatusInternalServerError, err)
		return
	}
	subrec := httptest.NewRecorder()
	s.ctx.serveHTTPSubrequest(subrec, subreq)
	subresp := subrec.Result()
	if subresp.StatusCode >= 200 && subresp.StatusCode <= 399 {
		if subresp.Header.Get("Content-Type") == s.webDirType && subresp.ContentLength <= 1 {
			s.handleDirectory(writer, request)
			return
		}
		s.next.ServeHTTP(writer, request)
		return
	}
	if subresp.StatusCode != http.StatusNotFound {
		s.handleError(writer, request, subresp.StatusCode, nil)
		return
	}
	s.handleDirectory(writer, request)
}

func (s *staticWebHandler) handleDirectory(writer http.ResponseWriter, request *http.Request) {
	if s.webIndex == "" && !s.webListings {
		if common.LooksTrue(request.Header.Get("X-Web-Mode")) {
			s.handleError(writer, request, http.StatusNotFound, nil)
			return
		}
		s.next.ServeHTTP(writer, request)
		return
	}
	if !strings.HasSuffix(request.URL.Path, "/") {
		if s.webIndex != "" {
			subreq, err := s.ctx.newSubrequest("HEAD", request.URL.Path+"/"+s.webIndex, nil, request, "staticweb")
			if err != nil {
				s.handleError(writer, request, http.StatusInternalServerError, err)
				return
			}
			subrec := httptest.NewRecorder()
			s.ctx.serveHTTPSubrequest(subrec, subreq)
			subresp := subrec.Result()
			if subresp.StatusCode >= 200 && subresp.StatusCode <= 399 {
				writer.Header().Set("Location", request.URL.Path+"/")
				srv.StandardResponse(writer, http.StatusMovedPermanently)
				return
			}
		}
		if s.webListings {
			if s.object == "" {
				writer.Header().Set("Location", request.URL.Path+"/")
				srv.StandardResponse(writer, http.StatusMovedPermanently)
				return
			}
			subreq, err := s.ctx.newSubrequest("GET", fmt.Sprintf("/v1/%s/%s?limit=1&format=json&delimiter=/&prefix=%s/", url.PathEscape(s.account), url.PathEscape(s.container), url.QueryEscape(s.object)), nil, request, "staticweb")
			if err != nil {
				s.handleError(writer, request, http.StatusInternalServerError, err)
				return
			}
			subrec := httptest.NewRecorder()
			s.ctx.serveHTTPSubrequest(subrec, subreq)
			subresp := subrec.Result()
			if subresp.StatusCode/100 != 2 {
				s.handleError(writer, request, subresp.StatusCode, nil)
				return
			}
			listing := make([]struct{}, 0)
			if err = json.NewDecoder(subresp.Body).Decode(&listing); err != nil {
				if err == io.EOF {
					s.handleError(writer, request, http.StatusNotFound, err)
				} else {
					s.handleError(writer, request, http.StatusInternalServerError, err)
				}
				return
			}
			if len(listing) == 0 {
				s.handleError(writer, request, http.StatusNotFound, nil)
				return
			}
			writer.Header().Set("Location", request.URL.Path+"/")
			srv.StandardResponse(writer, http.StatusMovedPermanently)
			return
		}
		s.handleError(writer, request, http.StatusNotFound, nil)
		return
	}
	if s.webIndex != "" {
		subreq, err := s.ctx.newSubrequest("HEAD", request.URL.Path+s.webIndex, nil, request, "staticweb")
		if err != nil {
			s.handleError(writer, request, http.StatusInternalServerError, err)
			return
		}
		subrec := httptest.NewRecorder()
		s.ctx.serveHTTPSubrequest(subrec, subreq)
		subresp := subrec.Result()
		if subresp.StatusCode/100 == 2 {
			subreq, err := s.ctx.newSubrequest(request.Method, request.URL.Path+s.webIndex, request.Body, request, "staticweb")
			if err != nil {
				s.handleError(writer, request, http.StatusInternalServerError, err)
				return
			}
			for k, v := range request.Header {
				subreq.Header[k] = v
			}
			s.ctx.serveHTTPSubrequest(writer, subreq)
			return
		} else if subresp.StatusCode != http.StatusNotFound {
			s.handleError(writer, request, subresp.StatusCode, nil)
			return
		}
	}
	label := request.URL.Path
	if s.webListingsLabel != "" {
		label = s.webListingsLabel + "/" + s.object
	}
	css := defaultCSS
	if s.webListingsCSS != "" {
		path := s.webListingsCSS
		if !strings.HasPrefix(s.webListingsCSS, "/") && !strings.HasPrefix(s.webListingsCSS, "http://") && !strings.HasPrefix(s.webListingsCSS, "https://") {
			path = ""
			for i := strings.Count(s.webListingsCSS, "/"); i > 0; i-- {
				path += "../"
			}
			path += s.webListingsCSS
		}
		css = fmt.Sprintf(`<link rel="stylesheet" type="text/css" href="%s" />`, path)
	}
	if !s.webListings {
		webIndexNote := ""
		if s.webIndex != "" {
			webIndexNote = fmt.Sprintf(webIndexNoteHTML, html.EscapeString(s.webIndex))
		}
		body := fmt.Sprintf(disabledListingHTML, html.EscapeString(label), css, webIndexNote)
		writer.Header().Set("Content-Type", "text/html")
		writer.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
		writer.WriteHeader(http.StatusNotFound)
		writer.Write([]byte(body))
		return
	}
	listingPath := fmt.Sprintf("/v1/%s/%s?format=json&delimiter=/", url.PathEscape(s.account), url.PathEscape(s.container))
	if s.object != "" {
		listingPath += fmt.Sprintf("&prefix=%s", url.QueryEscape(s.object))
	}
	subreq, err := s.ctx.newSubrequest("GET", listingPath, nil, request, "staticweb")
	if err != nil {
		s.handleError(writer, request, http.StatusInternalServerError, err)
		return
	}
	subrec := httptest.NewRecorder()
	s.ctx.serveHTTPSubrequest(subrec, subreq)
	subresp := subrec.Result()
	if subresp.StatusCode/100 != 2 {
		s.handleError(writer, request, subresp.StatusCode, nil)
		return
	}
	type entry struct {
		Name         string `json:"name"`
		ContentType  string `json:"content_type"`
		Bytes        int    `json:"bytes"`
		LastModified string `json:"last_modified"`
		Subdir       string `json:"subdir"`
	}
	listing := make([]*entry, 0)
	if err = json.NewDecoder(subresp.Body).Decode(&listing); err != nil {
		if err == io.EOF {
			s.handleError(writer, request, http.StatusNotFound, err)
		} else {
			s.handleError(writer, request, http.StatusInternalServerError, err)
		}
		return
	}
	if len(listing) == 0 {
		s.handleError(writer, request, http.StatusNotFound, nil)
		return
	}
	var body bytes.Buffer
	fmt.Fprintf(&body, preambleListingHTML, html.EscapeString(label), css, html.EscapeString(label))
	if s.object != "" {
		body.WriteString(`<tr id="parent" class="item"><td class="colname"><a href="../">../</a></td><td class="colsize">&nbsp;</td><td class="coldate">&nbsp;</td></tr>`)
	}
	for _, e := range listing {
		if e.Subdir == "" {
			continue
		}
		p := strings.TrimPrefix(e.Subdir, s.object)
		fmt.Fprintf(&body, `<tr class="item subdir"><td class="colname"><a href="%s">%s</a></td><td class="colsize">&nbsp;</td><td class="coldate">&nbsp;</td></tr>`, html.EscapeString(p), html.EscapeString(p))
	}
	for _, e := range listing {
		if e.Subdir != "" {
			continue
		}
		var c []string
		for _, s := range strings.Split(strings.ToLower(e.ContentType), "/") {
			c = append(c, "type-"+s)
		}
		p := strings.TrimPrefix(e.Name, s.object)
		m := strings.Replace(strings.Split(e.LastModified, ".")[0], "T", " ", 1)
		fmt.Fprintf(&body, `<tr class="item %s"><td class="colname"><a href="%s">%s</a></td><td class="colsize">%s</td><td class="coldate">%s</td></tr>`, html.EscapeString(strings.Join(c, " ")), html.EscapeString(p), html.EscapeString(p), humanReadable(e.Bytes), html.EscapeString(m))
	}
	body.WriteString(`</table></body></html>`)
	writer.Header().Set("Content-Type", "text/html")
	writer.Header().Set("Content-Length", fmt.Sprintf("%d", body.Len()))
	writer.WriteHeader(http.StatusOK)
	writer.Write(body.Bytes())
}

const defaultCSS string = `
<style type="text/css">
    h1 {font-size: 1em; font-weight: bold;}
    th {text-align: left; padding: 0px 1em 0px 1em;}
    td {padding: 0px 1em 0px 1em;}
    p {font-size: 2}
    a {text-decoration: none;}
</style>
`

const disabledListingHTML string = `<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
    <head>
        <title>Listing of %s</title>
        %s
    </head>
    <body>
        <h1>Web Listing Disabled</h1>
        <p>The owner of this web site has disabled web listing.</p>
        <p>If you are the owner of this web site, you can enableweb listing by setting X-Container-Meta-Web-Listings.</p>
        %s
    </body>
</html>
`

const webIndexNoteHTML string = `
<h1>Index File Not Found</h1>
<p>The owner of this web site has set <b>X-Container-Meta-Web-Index: %s</b>. However, this file was not found.</p>
`

const preambleListingHTML string = `<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
    <head>
        <title>Listing of %s</title>
        %s
    </head>
    <body>
        <h1 id="title">Listing of %s</h1>
        <table id="listing">
            <tr id="heading">
                <th class="colname">Name</th>
                <th class="colsize">Size</th>
                <th class="coldate">Date</th>
            </tr>
`

func humanReadable(value int) string {
	v := float64(value)
	i := -1
	suffixes := "KMGTPEZY"
	for v >= 1024 && i+1 < len(suffixes) {
		i += 1
		v = math.Floor(v/1024 + .5)
	}
	if i == -1 {
		return fmt.Sprintf("%d", int(v))
	}
	return fmt.Sprintf("%d%ci", int(v), suffixes[i])
}

func (s *staticWebHandler) handleError(writer http.ResponseWriter, request *http.Request, status int, err error) {
	if s.webError == "" {
		srv.StandardResponse(writer, status)
		return
	}
	subreq, err := s.ctx.newSubrequest("GET", fmt.Sprintf("/v1/%s/%s/%d%s", url.PathEscape(s.account), url.PathEscape(s.container), status, s.webError), nil, request, "staticweb")
	if err != nil {
		srv.StandardResponse(writer, status)
		return
	}
	subrec := httptest.NewRecorder()
	s.ctx.serveHTTPSubrequest(subrec, subreq)
	subresp := subrec.Result()
	defer subresp.Body.Close()
	if subresp.StatusCode/100 != 2 {
		srv.StandardResponse(writer, status)
		return
	}
	writer.Header().Set("Content-Type", subresp.Header.Get("Content-Type"))
	writer.Header().Set("Content-Length", subresp.Header.Get("Content-Length"))
	writer.WriteHeader(status)
	io.Copy(writer, subresp.Body)
}
