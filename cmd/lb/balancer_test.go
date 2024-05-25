package main

import (
	"bytes"
	. "gopkg.in/check.v1"
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

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestScheme(c *C) {
	*https = true
	c.Assert(scheme(), Equals, "https")

	*https = false
	c.Assert(scheme(), Equals, "http")
}

func (s *MySuite) TestBalancer(c *C) {
	healthChecker = &FakeAlwaysTrueHealthChecker{}
	requestSender = &FakeReturnRequestBodyRequestSender{}
	serversPool = []string{"http://server1:1", "http://server2:1", "http://server3:1"}
	healthCheck(serversPool)
	time.Sleep(10 * time.Second)

	server := chooseServer()
	c.Assert(server, NotNil)
	c.Assert(strings.Contains(server, "http://server"), Equals, true)

	err := forward("http://server1:1", httptest.NewRecorder(), httptest.NewRequest("GET", "http://server1:1", strings.NewReader("body length 14")))
	c.Assert(err, Equals, nil)
	err = forward("http://server3:1", httptest.NewRecorder(), httptest.NewRequest("GET", "http://server3:1", strings.NewReader("body length 14")))
	c.Assert(err, Equals, nil)

	server = chooseServer()
	c.Assert(server, Equals, "http://server2:1")
}
