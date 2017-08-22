package client

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// userClient is a Client to be used by end-users.  It knows how to authenticate with auth v1 and v2.
type userClient struct {
	client                                              *http.Client
	ServiceURL                                          string
	AuthToken                                           string
	tenant, username, password, apikey, region, authurl string
	private                                             bool
}

var _ Client = &userClient{}

func (c *userClient) authedRequest(method string, path string, body io.Reader, headers map[string]string) (*http.Request, error) {
	req, err := http.NewRequest(method, c.ServiceURL+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Auth-Token", c.AuthToken)
	req.Header.Set("User-Agent", "Hummingbird Client")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return req, nil
}

func (c *userClient) do(req *http.Request) *http.Response {
	resp, err := c.client.Do(req)
	if err != nil {
		return ResponseStub(http.StatusBadRequest, err.Error())
	}
	return resp
}

func (c *userClient) doRequest(method string, path string, body io.Reader, headers map[string]string) *http.Response {
	req, err := c.authedRequest(method, path, body, headers)
	if err != nil {
		return ResponseStub(http.StatusBadRequest, err.Error())
	}
	return c.do(req)
}

func (c *userClient) GetURL() string {
	return c.ServiceURL
}

func (c *userClient) PutAccount(headers map[string]string) *http.Response {
	return c.doRequest("PUT", "", nil, headers)
}

func (c *userClient) PostAccount(headers map[string]string) *http.Response {
	return c.doRequest("POST", "", nil, headers)
}

func (c *userClient) GetAccount(marker string, endMarker string, limit int, prefix string, delimiter string, reverse bool, headers map[string]string) ([]ContainerRecord, *http.Response) {
	limitStr := ""
	if limit > 0 {
		limitStr = strconv.Itoa(limit)
	}
	reverseStr := ""
	if reverse {
		reverseStr = "true"
	}
	path := mkquery(map[string]string{"marker": marker, "end_marker": endMarker, "prefix": prefix, "delimiter": delimiter, "limit": limitStr, "reverse": reverseStr})
	req, err := c.authedRequest("GET", path, nil, headers)
	if err != nil {
		return nil, ResponseStub(http.StatusBadRequest, err.Error())
	}
	req.Header.Set("Accept", "application/json")
	resp := c.do(req)
	if resp.StatusCode/100 != 2 {
		return nil, resp
	}
	var accountListing []ContainerRecord
	if err := json.NewDecoder(resp.Body).Decode(&accountListing); err != nil {
		resp.Body.Close()
		return nil, ResponseStub(http.StatusInternalServerError, err.Error())
	}
	resp.Body.Close()
	return accountListing, resp
}

func (c *userClient) GetAccountRaw(marker string, endMarker string, limit int, prefix string, delimiter string, reverse bool, headers map[string]string) *http.Response {
	limitStr := ""
	if limit > 0 {
		limitStr = strconv.Itoa(limit)
	}
	reverseStr := ""
	if reverse {
		reverseStr = "true"
	}
	path := mkquery(map[string]string{"marker": marker, "end_marker": endMarker, "prefix": prefix, "delimiter": delimiter, "limit": limitStr, "reverse": reverseStr})
	req, err := c.authedRequest("GET", path, nil, headers)
	if err != nil {
		return ResponseStub(http.StatusBadRequest, err.Error())
	}
	req.Header.Set("Accept", "application/json")
	return c.do(req)
}

func (c *userClient) HeadAccount(headers map[string]string) *http.Response {
	return c.doRequest("HEAD", "", nil, headers)
}

func (c *userClient) DeleteAccount(headers map[string]string) *http.Response {
	return c.doRequest("DELETE", "", nil, nil)
}

func (c *userClient) PutContainer(container string, headers map[string]string) *http.Response {
	return c.doRequest("PUT", "/"+container, nil, headers)
}

func (c *userClient) PostContainer(container string, headers map[string]string) *http.Response {
	return c.doRequest("POST", "/"+container, nil, headers)
}

func (c *userClient) GetContainer(container string, marker string, endMarker string, limit int, prefix string, delimiter string, reverse bool, headers map[string]string) ([]ObjectRecord, *http.Response) {
	limitStr := ""
	if limit > 0 {
		limitStr = strconv.Itoa(limit)
	}
	reverseStr := ""
	if reverse {
		reverseStr = "true"
	}
	path := "/" + container + mkquery(map[string]string{"marker": marker, "end_marker": endMarker, "prefix": prefix, "delimiter": delimiter, "limit": limitStr, "reverse": reverseStr})
	req, err := c.authedRequest("GET", path, nil, headers)
	if err != nil {
		return nil, ResponseStub(http.StatusBadRequest, err.Error())
	}
	req.Header.Set("Accept", "application/json")
	resp := c.do(req)
	if resp.StatusCode/100 != 2 {
		return nil, resp
	}
	var containerListing []ObjectRecord
	if err := json.NewDecoder(resp.Body).Decode(&containerListing); err != nil {
		resp.Body.Close()
		return nil, ResponseStub(http.StatusInternalServerError, err.Error())
	}
	resp.Body.Close()
	return containerListing, resp
}

func (c *userClient) GetContainerRaw(container string, marker string, endMarker string, limit int, prefix string, delimiter string, reverse bool, headers map[string]string) *http.Response {
	limitStr := ""
	if limit > 0 {
		limitStr = strconv.Itoa(limit)
	}
	reverseStr := ""
	if reverse {
		reverseStr = "true"
	}
	path := "/" + container + mkquery(map[string]string{"marker": marker, "end_marker": endMarker, "prefix": prefix, "delimiter": delimiter, "limit": limitStr, "reverse": reverseStr})
	req, err := c.authedRequest("GET", path, nil, headers)
	if err != nil {
		return ResponseStub(http.StatusBadRequest, err.Error())
	}
	req.Header.Set("Accept", "application/json")
	return c.do(req)
}

func (c *userClient) HeadContainer(container string, headers map[string]string) *http.Response {
	return c.doRequest("HEAD", "/"+container, nil, headers)
}

func (c *userClient) DeleteContainer(container string, headers map[string]string) *http.Response {
	return c.doRequest("DELETE", "/"+container, nil, headers)
}

func (c *userClient) PutObject(container string, obj string, headers map[string]string, src io.Reader) *http.Response {
	return c.doRequest("PUT", "/"+container+"/"+obj, src, headers)
}

func (c *userClient) PostObject(container string, obj string, headers map[string]string) *http.Response {
	return c.doRequest("POST", "/"+container+"/"+obj, nil, headers)
}

func (c *userClient) GetObject(container string, obj string, headers map[string]string) *http.Response {
	return c.doRequest("GET", "/"+container+"/"+obj, nil, headers)
}

func (c *userClient) HeadObject(container string, obj string, headers map[string]string) *http.Response {
	return c.doRequest("HEAD", "/"+container+"/"+obj, nil, headers)
}

func (c *userClient) DeleteObject(container string, obj string, headers map[string]string) *http.Response {
	return c.doRequest("DELETE", "/"+container+"/"+obj, nil, headers)
}

func (c *userClient) Raw(method, urlAfterAccount string, headers map[string]string, body io.Reader) *http.Response {
	return c.doRequest(method, urlAfterAccount, body, headers)
}

func (c *userClient) authenticatev1() *http.Response {
	req, err := http.NewRequest("GET", c.authurl, nil)
	if err != nil {
		return ResponseStub(http.StatusBadRequest, err.Error())
	}
	au := c.username
	if c.tenant != "" {
		au = c.tenant + ":" + c.username
	}
	req.Header.Set("X-Auth-User", au)
	ak := c.apikey
	if ak == "" {
		ak = c.password
	}
	req.Header.Set("X-Auth-Key", ak)
	resp, err := c.client.Do(req)
	if err != nil {
		return ResponseStub(http.StatusBadRequest, err.Error())
	}
	if resp.StatusCode/100 != 2 {
		return resp
	}
	c.ServiceURL = resp.Header.Get("X-Storage-Url")
	c.AuthToken = resp.Header.Get("X-Auth-Token")
	if c.ServiceURL == "" || c.AuthToken == "" {
		resp.Body.Close()
		return ResponseStub(http.StatusInternalServerError, "Response did not have X-Storage-Url or X-Auth-Token headers.")
	}
	return resp
}

type KeystoneRequestV2 struct {
	Auth interface{} `json:"auth"`
}

type KeystonePasswordAuthV2 struct {
	TenantName          string `json:"tenantName"`
	PasswordCredentials struct {
		Username string `json:"username"`
		Password string `json:"password"`
	} `json:"passwordCredentials"`
}

type RaxAPIKeyAuthV2 struct {
	APIKeyCredentials struct {
		Username string `json:"username"`
		APIKey   string `json:"apiKey"`
	} `json:"RAX-KSKEY:apiKeyCredentials"`
}

type KeystoneResponseV2 struct {
	Access struct {
		Token struct {
			ID     string `json:"id"`
			Tenant struct {
				Name string `json:"name"`
				ID   string `json:"id"`
			} `json:"tenant"`
		} `json:"token"`
		ServiceCatalog []struct {
			Endpoints []struct {
				PublicURL   string `json:"publicURL"`
				InternalURL string `json:"internalURL"`
				Region      string `json:"region"`
			} `json:"endpoints"`
			Type string `json:"type"`
		} `json:"serviceCatalog"`
		User struct {
			RaxDefaultRegion string `json:"RAX-AUTH:defaultRegion"`
		} `json:"user"`
	} `json:"access"`
}

func (c *userClient) authenticatev2() *http.Response {
	if !strings.HasSuffix(c.authurl, "tokens") {
		if c.authurl[len(c.authurl)-1] == '/' {
			c.authurl = c.authurl + "tokens"
		} else {
			c.authurl = c.authurl + "/tokens"
		}
	}
	var authReq []byte
	var err error
	if c.password != "" {
		creds := &KeystonePasswordAuthV2{TenantName: c.tenant}
		creds.PasswordCredentials.Username = c.username
		creds.PasswordCredentials.Password = c.password
		authReq, err = json.Marshal(&KeystoneRequestV2{Auth: creds})
	} else if c.apikey != "" {
		creds := &RaxAPIKeyAuthV2{}
		creds.APIKeyCredentials.Username = c.username
		creds.APIKeyCredentials.APIKey = c.apikey
		authReq, err = json.Marshal(&KeystoneRequestV2{Auth: creds})
	} else {
		return ResponseStub(http.StatusInternalServerError, "Couldn't figure out what credentials to use.")
	}
	if err != nil {
		return ResponseStub(http.StatusInternalServerError, err.Error())
	}
	resp, err := c.client.Post(c.authurl, "application/json", bytes.NewBuffer(authReq))
	if err != nil {
		return ResponseStub(http.StatusBadRequest, err.Error())
	}
	if resp.StatusCode/100 != 2 {
		return resp
	}
	var authResponse KeystoneResponseV2
	if err := json.NewDecoder(resp.Body).Decode(&authResponse); err != nil {
		resp.Body.Close()
		return ResponseStub(http.StatusInternalServerError, err.Error())
	}
	resp.Body.Close()
	c.AuthToken = authResponse.Access.Token.ID
	region := c.region
	if region == "" {
		region = authResponse.Access.User.RaxDefaultRegion
	}
	for _, s := range authResponse.Access.ServiceCatalog {
		if s.Type == "object-store" {
			for _, e := range s.Endpoints {
				if e.Region == region || region == "" || len(s.Endpoints) == 1 {
					if c.private {
						c.ServiceURL = e.InternalURL
					} else {
						c.ServiceURL = e.PublicURL
					}
					return ResponseStub(http.StatusOK, "")
				}
			}
		}
	}
	return ResponseStub(http.StatusInternalServerError, "Didn't find endpoint")
}

type keystoneRequestV3 struct {
	Auth struct {
		Identity struct {
			Methods  []string `json:"methods"`
			Password struct {
				User struct {
					Name   string `json:"name"`
					Domain struct {
						Name string `json:"name"`
					} `json:"domain"`
					Password string `json:"password"`
				} `json:"user"`
			} `json:"password"`
		} `json:"identity"`
	} `json:"auth"`
}

type keystoneResponseV3 struct {
	Token struct {
		Catalog []struct {
			Type      string `json:"type"`
			Endpoints []struct {
				Region    string `json:"region"`
				URL       string `json:"url"`
				Interface string `json:"interface"`
			} `json:"endpoints"`
		} `json:"catalog"`
	} `json:"token"`
}

func (c *userClient) authenticatev3() *http.Response {
	if !strings.HasSuffix(c.authurl, "auth/tokens") {
		if c.authurl[len(c.authurl)-1] == '/' {
			c.authurl = c.authurl + "auth/tokens"
		} else {
			c.authurl = c.authurl + "/auth/tokens"
		}
	}
	var authReq []byte
	var err error
	if c.password != "" {
		creds := &keystoneRequestV3{}
		creds.Auth.Identity.Methods = []string{"password"}
		creds.Auth.Identity.Password.User.Domain.Name = "Default"
		creds.Auth.Identity.Password.User.Name = c.username
		creds.Auth.Identity.Password.User.Password = c.password
		authReq, err = json.Marshal(creds)
	} else if c.apikey != "" {
		panic("v3 by api key not implemented yet")
	} else {
		return ResponseStub(http.StatusInternalServerError, "Couldn't figure out what credentials to use.")
	}
	if err != nil {
		return ResponseStub(http.StatusInternalServerError, err.Error())
	}
	resp, err := c.client.Post(c.authurl, "application/json", bytes.NewBuffer(authReq))
	if err != nil {
		return ResponseStub(http.StatusBadRequest, err.Error())
	}
	if resp.StatusCode/100 != 2 {
		return resp
	}
	defer resp.Body.Close()
	c.AuthToken = resp.Header.Get("X-Subject-Token")
	if c.AuthToken == "" {
		return ResponseStub(http.StatusInternalServerError, "No X-Subject-Token in response.")
	}
	var authResponse keystoneResponseV3
	if err := json.NewDecoder(resp.Body).Decode(&authResponse); err != nil {
		return ResponseStub(http.StatusInternalServerError, err.Error())
	}
	intrfc := "public"
	if c.private {
		intrfc = "private"
	}
	for _, s := range authResponse.Token.Catalog {
		if s.Type == "object-store" {
			for _, e := range s.Endpoints {
				if ((e.Region == c.region || c.region == "") && e.Interface == intrfc) || len(s.Endpoints) == 1 {
					c.ServiceURL = e.URL
					return ResponseStub(http.StatusOK, "")
				}
			}
		}
	}
	return ResponseStub(http.StatusInternalServerError, "Didn't find endpoint")
}

func (c *userClient) authenticate() *http.Response {
	if strings.Contains(c.authurl, "/v3") {
		return c.authenticatev3()
	} else if strings.Contains(c.authurl, "/v2") {
		return c.authenticatev2()
	} else {
		return c.authenticatev1()
	}
}

// NewClient creates a new end-user client. It authenticates immediately, and
// returns the error response if unable to.
func NewClient(tenant string, username string, password string, apikey string, region string, authurl string, private bool) (Client, *http.Response) {
	c := &userClient{
		client: &http.Client{
			Timeout: 30 * time.Minute,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 300,
			},
		},
		tenant:   tenant,
		username: username,
		password: password,
		apikey:   apikey,
		region:   region,
		authurl:  authurl,
		private:  private,
	}
	if aResp := c.authenticate(); aResp.StatusCode/100 != 2 {
		return nil, aResp
	} else {
		aResp.Body.Close()
	}
	return c, nil
}

// NewInsecureClient creates a new end-user client with SSL verification turned
// off. It authenticates immediately, and returns the error response if unable
// to.
func NewInsecureClient(tenant string, username string, password string, apikey string, region string, authurl string, private bool) (Client, *http.Response) {
	c := &userClient{
		client: &http.Client{
			Timeout: 30 * time.Minute,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		},
		tenant:   tenant,
		username: username,
		password: password,
		apikey:   apikey,
		region:   region,
		authurl:  authurl,
		private:  private,
	}
	if aResp := c.authenticate(); aResp.StatusCode/100 != 2 {
		return nil, aResp
	} else {
		aResp.Body.Close()
	}
	return c, nil
}
