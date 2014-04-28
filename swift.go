package swiftclient

import (
	"fmt"
	"git.koofr.lan/go-httpclient.git"
	"git.koofr.lan/go-ioutils.git"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type Swift struct {
	HTTPClient *httpclient.HTTPClient
}

func NewSwift() (swift *Swift) {
	return &Swift{
		HTTPClient: httpclient.New(),
	}
}

func (s *Swift) AuthenticateV1(endpoint string, user string, key string) (err error) {
	headers := make(http.Header)
	headers.Set("X-Auth-User", user)
	headers.Set("X-Auth-Key", key)

	req := httpclient.RequestData{
		Method:      "GET",
		FullURL:     endpoint,
		Headers:     headers,
		RespConsume: true,
	}

	res, err := s.HTTPClient.Request(&req)

	if err != nil {
		return
	}

	storageURL := res.Header.Get("X-Storage-Url")
	authToken := res.Header.Get("X-Auth-Token")

	if authToken == "" {
		authToken = res.Header.Get("X-Storage-Token")
	}

	if storageURL == "" || authToken == "" {
		err = fmt.Errorf("swift authentication failed")
		return
	}

	s.HTTPClient.Headers.Set("X-Auth-Token", authToken)

	if !strings.HasSuffix(storageURL, "/") {
		storageURL += "/"
	}

	u, err := url.Parse(storageURL)

	if err != nil {
		return
	}

	s.HTTPClient.BaseURL = u

	return
}

func (s *Swift) Request(req *httpclient.RequestData) (response *http.Response, err error) {
	return s.HTTPClient.Request(req)
}

func (s *Swift) Path(container string, path string) string {
	p := container

	if path != "" {
		p += "/" + path
	}

	return p
}

func (s *Swift) PutContainer(container string) (err error) {
	_, err = s.Request(&httpclient.RequestData{
		Method:         "PUT",
		Path:           s.Path(container, ""),
		ExpectedStatus: []int{http.StatusCreated, http.StatusAccepted},
		RespConsume:    true,
	})

	return
}

func (s *Swift) ListObjects(container string, path string, recursive bool) (objects []*SwiftObject, err error) {
	objects = []*SwiftObject{}

	params := make(url.Values)
	params.Set("format", "json")

	if path != "" {
		if !strings.HasSuffix(path, "/") {
			path += "/"
		}

		params.Set("prefix", path)
	}

	if !recursive {
		params.Set("delimiter", "/")
	}

	_, err = s.Request(&httpclient.RequestData{
		Method:         "GET",
		Path:           s.Path(container, ""),
		Params:         params,
		ExpectedStatus: []int{http.StatusOK},
		RespEncoding:   httpclient.EncodingJSON,
		RespValue:      &objects,
	})

	return
}

func (s *Swift) ObjectInfo(container string, path string) (info *SwiftObject, err error) {
	res, err := s.Request(&httpclient.RequestData{
		Method:         "HEAD",
		Path:           s.Path(container, path),
		ExpectedStatus: []int{http.StatusOK},
		RespConsume:    true,
	})

	if err != nil {
		return
	}

	info = SwiftObjectFromHeaders(path, res.Header)

	return
}

func (s *Swift) GetObject(container string, path string, span *ioutils.FileSpan) (obj *SwiftObject, err error) {
	req := httpclient.RequestData{
		Method:         "GET",
		Path:           s.Path(container, path),
		ExpectedStatus: []int{http.StatusOK, http.StatusPartialContent},
	}

	if span != nil {
		req.Headers = make(http.Header)
		req.Headers.Set("Range", fmt.Sprintf("bytes=%d-%d", span.Start, span.End))
	}

	res, err := s.Request(&req)

	if err != nil {
		return
	}

	obj = SwiftObjectFromHeaders(path, res.Header)
	obj.Reader = res.Body

	return
}

func (s *Swift) PutObject(container string, path string, reader io.Reader) (err error) {
	_, err = s.Request(&httpclient.RequestData{
		Method:         "PUT",
		Path:           s.Path(container, path),
		ReqReader:      reader,
		ExpectedStatus: []int{http.StatusCreated},
		RespConsume:    true,
	})

	return
}

func (s *Swift) PutObjectManifest(container string, path string, manifestContainer string, manifestPath string) (err error) {
	req := httpclient.RequestData{
		Method:         "PUT",
		Path:           s.Path(container, path),
		Headers:        make(http.Header),
		ExpectedStatus: []int{http.StatusCreated},
		RespConsume:    true,
	}

	manifest := s.Path(manifestContainer, manifestPath)
	req.Headers.Set("X-Object-Manifest", manifest)

	_, err = s.Request(&req)

	return
}

func (s *Swift) DeleteObject(container string, path string) (err error) {
	_, err = s.Request(&httpclient.RequestData{
		Method:         "DELETE",
		Path:           s.Path(container, path),
		ExpectedStatus: []int{http.StatusNoContent},
		RespConsume:    true,
	})

	return
}
