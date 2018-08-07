package conf

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

// OSSCryptSecret returns the (secret, source, error) for the OpenStack Swift
// Style Encryption layer for Hummingbird. The source is just a textual
// description of where the secret was obtained, for debugging purposes.
func OSSCryptSecret() ([]byte, string, error) {
	proxyConfPath := FindConfig("proxy")
	if proxyConfPath == "" {
		return nil, "", fmt.Errorf("unable to find proxy config file")
	}
	proxyConf, err := LoadConfig(proxyConfPath)
	if err != nil {
		return nil, "", fmt.Errorf("LoadConfig %s %s", proxyConfPath, err)
	}
	secret, source, err := configSecret(proxyConfPath, proxyConf)
	if secret != nil || err != nil {
		return secret, source, err
	}
	return barbicanSecret(proxyConfPath, proxyConf)
}

func configSecret(proxyConfPath string, proxyConf Config) ([]byte, string, error) {
	var secret []byte
	var source string
	var err error
	secretString, ok := proxyConf.GetSection("filter:keymaster").Get("encryption_root_secret")
	if ok {
		source = proxyConfPath + " [filter:keymaster]"
	} else {
		keymasterConfPath, ok := proxyConf.GetSection("filter:keymaster").Get("keymaster_config_path")
		if !ok {
			return nil, "", nil
		}
		keymasterConf, err := LoadConfig(keymasterConfPath)
		if err != nil {
			return nil, "", fmt.Errorf("LoadConfig %s %s", keymasterConfPath, err)
		}
		secretString, ok = keymasterConf.GetSection("keymaster").Get("encryption_root_secret")
		if ok {
			source = keymasterConfPath + " [keymaster]"
		} else {
			return nil, "", nil
		}
	}
	if len(secretString) < 44 {
		return nil, "", fmt.Errorf("stored secret %q set in %s is invalid; root secret values MUST be at least 44 valid base-64 characters", secretString, source)
	}
	secret, err = base64.StdEncoding.DecodeString(secretString)
	if err != nil {
		return nil, "", fmt.Errorf("stored secret %q set in %s is invalid: %s", secretString, source, err)
	}
	disabled := proxyConf.GetSection("filter:encryption").GetBool("disable_encryption", false)
	if disabled {
		return nil, "", fmt.Errorf("encryption is currently disabled by %s [filter:encryption]", proxyConfPath)
	}
	return secret, source, nil
}

func barbicanSecret(proxyConfPath string, proxyConf Config) ([]byte, string, error) {
	keymasterConfPath, ok := proxyConf.GetSection("filter:kms_keymaster").Get("keymaster_config_path")
	if !ok {
		return nil, "", nil
	}
	keymasterConf, err := LoadConfig(keymasterConfPath)
	if err != nil {
		return nil, "", fmt.Errorf("LoadConfig %s %s", keymasterConfPath, err)
	}
	kmsKeymasterConf := keymasterConf.GetSection("kms_keymaster")
	keyID, ok := kmsKeymasterConf.Get("key_id")
	if !ok {
		return nil, "", fmt.Errorf("no key_id in %s [kms_keymaster]", keymasterConfPath)
	}
	projectName, ok := kmsKeymasterConf.Get("project_name")
	if !ok {
		projectName = "Default"
	}
	username, ok := kmsKeymasterConf.Get("username")
	if !ok {
		return nil, "", fmt.Errorf("no username in %s [kms_keymaster]", keymasterConfPath)
	}
	password, ok := kmsKeymasterConf.Get("password")
	if !ok {
		return nil, "", fmt.Errorf("no password in %s [kms_keymaster]", keymasterConfPath)
	}
	authEndpoint, ok := kmsKeymasterConf.Get("auth_endpoint")
	if !ok {
		return nil, "", fmt.Errorf("no auth_endpoint in %s [kms_keymaster]", keymasterConfPath)
	}
	var authToken, barbicanEndpoint string
	authToken, barbicanEndpoint, err = authenticatev3(authEndpoint, projectName, username, password)
	if err != nil {
		return nil, "", fmt.Errorf("erroring authenticating %s", err)
	}
	barbicanEndpointOverride, ok := kmsKeymasterConf.Get("barbican_endpoint")
	if ok {
		barbicanEndpoint = barbicanEndpointOverride
	}
	if barbicanEndpoint == "" {
		return nil, "", fmt.Errorf("could not determine Barbican endpoint from Keystone and no barbican_endpoint in %s [kms_keymaster]", keymasterConfPath)
	}
	client := &http.Client{
		Timeout: 30 * time.Minute,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 300,
			MaxIdleConns:        0,
			IdleConnTimeout:     5 * time.Second,
			DisableCompression:  true,
		},
	}
	reqURL := barbicanEndpoint
	if reqURL[len(reqURL)-1] != '/' {
		reqURL += "/"
	}
	reqURL += "secrets/" + keyID + "/payload"
	req, err := http.NewRequest("GET", reqURL, nil)
	if err != nil {
		return nil, "", fmt.Errorf("error creating auth request %s", err)
	}
	req.Header.Set("X-Auth-Token", authToken)
	resp, err := client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("error making auth request %s", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, "", fmt.Errorf("unexpected status code %d from auth", resp.StatusCode)
	}
	secretStringBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("erroring reading response from auth %s", err)
	}
	secret, err := base64.StdEncoding.DecodeString(string(secretStringBytes))
	if err != nil {
		return nil, "", fmt.Errorf("stored secret %q set in Barbican is invalid: %s", string(secretStringBytes), err)
	}
	return secret, "Barbican", nil
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
				URL string `json:"url"`
			} `json:"endpoints"`
		} `json:"catalog"`
	} `json:"token"`
}

func authenticatev3(authEndpoint, projectName, username, password string) (string, string, error) {
	if !strings.HasSuffix(authEndpoint, "auth/tokens") {
		if authEndpoint[len(authEndpoint)-1] == '/' {
			authEndpoint = authEndpoint + "auth/tokens"
		} else {
			authEndpoint = authEndpoint + "/auth/tokens"
		}
	}
	var authReq []byte
	var err error
	creds := &keystoneRequestV3{}
	creds.Auth.Identity.Methods = []string{"password"}
	creds.Auth.Identity.Password.User.Domain.Name = projectName
	creds.Auth.Identity.Password.User.Name = username
	creds.Auth.Identity.Password.User.Password = password
	authReq, err = json.Marshal(creds)
	if err != nil {
		return "", "", fmt.Errorf("error encoding auth json request %s", err)
	}
	client := &http.Client{
		Timeout: 30 * time.Minute,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 300,
			MaxIdleConns:        0,
			IdleConnTimeout:     5 * time.Second,
			DisableCompression:  true,
		},
	}
	resp, err := client.Post(authEndpoint, "application/json", bytes.NewBuffer(authReq))
	if err != nil {
		return "", "", fmt.Errorf("error making auth request %s", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return "", "", fmt.Errorf("unexpected status code %d from auth", resp.StatusCode)
	}
	defer resp.Body.Close()
	authToken := resp.Header.Get("X-Subject-Token")
	if authToken == "" {
		return "", "", fmt.Errorf("no X-Subject-Token in response")
	}
	var authResponse keystoneResponseV3
	if err := json.NewDecoder(resp.Body).Decode(&authResponse); err != nil {
		return "", "", fmt.Errorf("error decoding auth json response %s", err)
	}
	barbicanEndpoint := ""
	for _, s := range authResponse.Token.Catalog {
		if s.Type == "barbican" && len(s.Endpoints) > 0 {
			barbicanEndpoint = s.Endpoints[0].URL
		}
	}
	return authToken, barbicanEndpoint, nil
}
