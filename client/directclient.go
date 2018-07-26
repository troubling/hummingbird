package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/nectar"
	"github.com/troubling/nectar/nectarutil"
)

type directClient struct {
	pc      RequestClient
	account string
}

var _ nectar.Client = &directClient{}

func NewDirectClient(account string, cnf srv.ConfigLoader, certFile, keyFile string, logger srv.LowLevelLogger) (nectar.Client, error) {
	policies, err := cnf.GetPolicies()
	if err != nil {
		return nil, err
	}
	pdc, err := NewProxyClient(policies, cnf, logger, certFile, keyFile, "", "", "", conf.Config{})
	if err != nil {
		return nil, fmt.Errorf("Could not make client: %v", err)
	}
	return &directClient{pc: pdc.NewRequestClient(nil, nil, logger), account: account}, nil
}

func (c *directClient) SetUserAgent(v string) {
	c.pc.SetUserAgent(v)
}

func (c *directClient) GetURL() string {
	return "<direct>/" + c.account
}

func (c *directClient) PutAccount(headers map[string]string) *http.Response {
	return c.pc.PutAccount(context.Background(), c.account, common.Map2Headers(headers))
}

func (c *directClient) PostAccount(headers map[string]string) *http.Response {
	return c.pc.PostAccount(context.Background(), c.account, common.Map2Headers(headers))
}

func (c *directClient) GetAccount(marker string, endMarker string, limit int, prefix string, delimiter string, reverse bool, headers map[string]string) ([]*nectar.ContainerRecord, *http.Response) {
	resp := c.GetAccountRaw(marker, endMarker, limit, prefix, delimiter, reverse, headers)
	if resp.StatusCode/100 != 2 {
		return nil, resp
	}
	var accountListing []*nectar.ContainerRecord
	if err := json.NewDecoder(resp.Body).Decode(&accountListing); err != nil {
		resp.Body.Close()
		// FIXME. Log something.
		return nil, nectarutil.ResponseStub(http.StatusInternalServerError, err.Error())
	}
	resp.Body.Close()
	return accountListing, resp
}

func (c *directClient) GetAccountRaw(marker string, endMarker string, limit int, prefix string, delimiter string, reverse bool, headers map[string]string) *http.Response {
	options := map[string]string{
		"format":     "json",
		"marker":     marker,
		"end_marker": endMarker,
		"prefix":     prefix,
		"delimiter":  delimiter,
	}
	if limit != 0 {
		options["limit"] = strconv.Itoa(limit)
	}
	if reverse {
		options["reverse"] = "true"
	}
	return c.pc.GetAccountRaw(context.Background(), c.account, options, common.Map2Headers(headers))
}

func (c *directClient) HeadAccount(headers map[string]string) *http.Response {
	return c.pc.HeadAccount(context.Background(), c.account, common.Map2Headers(headers))
}

func (c *directClient) DeleteAccount(headers map[string]string) *http.Response {
	return c.pc.DeleteAccount(context.Background(), c.account, common.Map2Headers(headers))
}

func (c *directClient) PutContainer(container string, headers map[string]string) *http.Response {
	return c.pc.PutContainer(context.Background(), c.account, container, common.Map2Headers(headers))
}

func (c *directClient) PostContainer(container string, headers map[string]string) *http.Response {
	return c.pc.PostContainer(context.Background(), c.account, container, common.Map2Headers(headers))
}

func (c *directClient) GetContainer(container string, marker string, endMarker string, limit int, prefix string, delimiter string, reverse bool, headers map[string]string) ([]*nectar.ObjectRecord, *http.Response) {
	resp := c.GetContainerRaw(container, marker, endMarker, limit, prefix, delimiter, reverse, headers)
	if resp.StatusCode/100 != 2 {
		return nil, resp
	}
	var containerListing []*nectar.ObjectRecord
	if err := json.NewDecoder(resp.Body).Decode(&containerListing); err != nil {
		resp.Body.Close()
		// FIXME. Log something.
		return nil, nectarutil.ResponseStub(http.StatusInternalServerError, err.Error())
	}
	resp.Body.Close()
	return containerListing, resp
}

func (c *directClient) GetContainerRaw(container string, marker string, endMarker string, limit int, prefix string, delimiter string, reverse bool, headers map[string]string) *http.Response {
	options := map[string]string{
		"format":     "json",
		"marker":     marker,
		"end_marker": endMarker,
		"prefix":     prefix,
		"delimiter":  delimiter,
	}
	if limit != 0 {
		options["limit"] = strconv.Itoa(limit)
	}
	if reverse {
		options["reverse"] = "true"
	}
	return c.pc.GetContainerRaw(context.Background(), c.account, container, options, common.Map2Headers(headers))
}

func (c *directClient) HeadContainer(container string, headers map[string]string) *http.Response {
	return c.pc.HeadContainer(context.Background(), c.account, container, common.Map2Headers(headers))
}

func (c *directClient) DeleteContainer(container string, headers map[string]string) *http.Response {
	return c.pc.DeleteContainer(context.Background(), c.account, container, common.Map2Headers(headers))
}

func (c *directClient) PutObject(container string, obj string, headers map[string]string, src io.Reader) *http.Response {
	return c.pc.PutObject(context.Background(), c.account, container, obj, common.Map2Headers(headers), src)
}

func (c *directClient) PostObject(container string, obj string, headers map[string]string) *http.Response {
	return c.pc.PostObject(context.Background(), c.account, container, obj, common.Map2Headers(headers))
}

func (c *directClient) GetObject(container string, obj string, headers map[string]string) *http.Response {
	return c.pc.GetObject(context.Background(), c.account, container, obj, common.Map2Headers(headers))
}

func (c *directClient) HeadObject(container string, obj string, headers map[string]string) *http.Response {
	return c.pc.HeadObject(context.Background(), c.account, container, obj, common.Map2Headers(headers))
}

func (c *directClient) DeleteObject(container string, obj string, headers map[string]string) *http.Response {
	return c.pc.DeleteObject(context.Background(), c.account, container, obj, common.Map2Headers(headers))
}

func (c *directClient) Raw(method, urlAfterAccount string, headers map[string]string, body io.Reader) *http.Response {
	return nectarutil.ResponseStub(http.StatusNotImplemented, "Raw requests not implemented for direct clients")
}
