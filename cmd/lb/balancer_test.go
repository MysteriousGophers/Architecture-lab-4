package main

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type FakeAlwaysTrueHealthChecker struct{}

func (hc *FakeAlwaysTrueHealthChecker) Check(server string) bool {
	return true
}

type FakeReturnRequestBodyRequestSender struct{}

func (rs *FakeReturnRequestBodyRequestSender) Send(request *http.Request) (*http.Response, error) {
	bodyBytes, err := ioutil.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	return &http.Response{
		StatusCode: 200,
		Proto:      "HTTP/1.1",
		Header:     make(http.Header),
		Body:       ioutil.NopCloser(bytes.NewBuffer(bodyBytes)),
		Request:    &http.Request{},
		Close:      false,
	}, nil
}

func TestScheme(t *testing.T) {
	*https = true
	assert.Equal(t, "https", scheme(), "Expected scheme to be https")

	*https = false
	assert.Equal(t, "http", scheme(), "Expected scheme to be http")
}

func TestBalancer(t *testing.T) {
	healthChecker = &FakeAlwaysTrueHealthChecker{}
	requestSender = &FakeReturnRequestBodyRequestSender{}
	serversPool = []string{"http://server1:1", "http://server2:1", "http://server3:1"}
	healthCheck(serversPool)
	time.Sleep(10 * time.Second)

	server := chooseServer()
	fmt.Println(server)
	assert.NotNil(t, server, "Assert server is not nil")
	assert.Contains(t, server, "http://server", "Assert server is not nil")

	err := forward("http://server1:1", httptest.NewRecorder(), httptest.NewRequest("GET", "http://server1:1", strings.NewReader("body length 14")))
	assert.NoError(t, err)
	err = forward("http://server3:1", httptest.NewRecorder(), httptest.NewRequest("GET", "http://server3:1", strings.NewReader("body length 14")))
	assert.NoError(t, err)

	server = chooseServer()

	assert.Equal(t, "http://server2:1", server, "Expected server2 to be chosen")
}
