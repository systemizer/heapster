package honeycomb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/golang/glog"
)

type config struct {
	APIHost  string
	Dataset  string
	WriteKey string
}

func buildConfig(uri *url.URL) (*config, error) {
	opts := uri.Query()
	dataset, err := getOpt(opts, "dataset", true)
	if err != nil {
		return nil, err
	}
	writekey, err := getOpt(opts, "writekey", true)
	if err != nil {
		return nil, err
	}
	apihost, err := getOpt(opts, "api_host", false)
	if err != nil {
		return nil, err
	}
	if apihost == "" {
		apihost = "https://api.honeycomb.io"
	}
	config := &config{
		APIHost:  apihost,
		WriteKey: writekey,
		Dataset:  dataset,
	}
	return config, err
}

func getOpt(opts url.Values, key string, required bool) (string, error) {
	if len(opts[key]) == 0 {
		if required {
			return "", fmt.Errorf("Missing required option `%v'", key)
		} else {
			return "", nil
		}
	} else if len(opts[key]) > 1 {
		return "", fmt.Errorf("Repeated option `%v'", key)
	}
	return opts[key][0], nil
}

type Client struct {
	config     config
	httpClient http.Client
}

func NewClient(uri *url.URL) (*Client, error) {
	config, err := buildConfig(uri)
	if err != nil {
		return nil, err
	}
	return &Client{config: *config}, nil
}

type BatchPoint struct {
	Data      interface{}
	Timestamp time.Time
}

type Batch []*BatchPoint

func (c *Client) SendBatch(batch Batch) error {
	if len(batch) == 0 {
		// Nothing to send
		return nil
	}
	buf := new(bytes.Buffer)
	err := json.NewEncoder(buf).Encode(batch)
	if err != nil {
		return err
	}
	err = c.makeRequest(buf)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) makeRequest(body io.Reader) error {
	url, err := url.Parse(c.config.APIHost)
	if err != nil {
		return err
	}
	url.Path = path.Join(url.Path, "/1/batch", c.config.Dataset)
	req, err := http.NewRequest("POST", url.String(), body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Add("X-Honeycomb-Team", c.config.WriteKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		glog.Warningf("Failed to send event: %v", err)
		return err
	}
	defer resp.Body.Close()
	ioutil.ReadAll(resp.Body)
	return nil
}
