package swiftclient

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/koofr/go-httpclient"
	"github.com/koofr/go-ioutils"
)

type Swift struct {
	HTTPClient        *httpclient.HTTPClient
	canReauthenticate bool
	endpoint          string
	user              string
	key               string
}

func NewSwift() (swift *Swift) {
	return &Swift{
		HTTPClient: httpclient.New(),
	}
}

func (s *Swift) AuthenticateV1(ctx context.Context, endpoint string, user string, key string) (err error) {
	headers := make(http.Header)
	headers.Set("X-Auth-User", user)
	headers.Set("X-Auth-Key", key)

	req := httpclient.RequestData{
		Context:     ctx,
		Method:      "GET",
		FullURL:     endpoint,
		Headers:     headers,
		RespConsume: true,
	}

	res, err := s.HTTPClient.Request(&req)
	if err != nil {
		return err
	}

	storageURL := res.Header.Get("X-Storage-Url")
	authToken := res.Header.Get("X-Auth-Token")

	if authToken == "" {
		authToken = res.Header.Get("X-Storage-Token")
	}

	if storageURL == "" || authToken == "" {
		return fmt.Errorf("swift authentication failed")
	}

	s.HTTPClient.Headers.Set("X-Auth-Token", authToken)

	if !strings.HasSuffix(storageURL, "/") {
		storageURL += "/"
	}

	u, err := url.Parse(storageURL)
	if err != nil {
		return err
	}

	s.HTTPClient.BaseURL = u

	s.canReauthenticate = true
	s.endpoint = endpoint
	s.user = user
	s.key = key

	return nil
}

func (s *Swift) Reauthenticate(ctx context.Context) (err error) {
	if !s.canReauthenticate {
		return fmt.Errorf("Swift not authenticated yet")
	}

	return s.AuthenticateV1(ctx, s.endpoint, s.user, s.key)
}

func (s *Swift) canRetryRequest(req *httpclient.RequestData) bool {
	return req.ReqReader == nil
}

func (s *Swift) Request(req *httpclient.RequestData) (response *http.Response, err error) {
	res, err := s.HTTPClient.Request(req)

	if res != nil && res.StatusCode == 401 {
		ctx := req.Context
		if ctx == nil {
			ctx = context.TODO()
		}

		reauthErr := s.Reauthenticate(ctx)
		if reauthErr != nil {
			return res, err // must return err, not reauthErr
		}

		if s.canRetryRequest(req) {
			res, err = s.HTTPClient.Request(req)

			return res, err
		}
	}

	return res, err
}

func (s *Swift) Path(container string, path string) string {
	p := container

	if path != "" {
		p += "/" + path
	}

	return p
}

func (s *Swift) PutContainer(ctx context.Context, container string) (err error) {
	_, err = s.Request(&httpclient.RequestData{
		Context:        ctx,
		Method:         "PUT",
		Path:           s.Path(container, ""),
		ExpectedStatus: []int{http.StatusCreated, http.StatusAccepted},
		RespConsume:    true,
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *Swift) ListObjects(ctx context.Context, container string, path string, recursive bool) (objects []*SwiftObject, err error) {
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
		Context:        ctx,
		Method:         "GET",
		Path:           s.Path(container, ""),
		Params:         params,
		ExpectedStatus: []int{http.StatusOK},
		RespEncoding:   httpclient.EncodingJSON,
		RespValue:      &objects,
	})
	if err != nil {
		return nil, err
	}

	return objects, nil
}

func (s *Swift) ObjectInfo(ctx context.Context, container string, path string) (info *SwiftObject, err error) {
	res, err := s.Request(&httpclient.RequestData{
		Context:        ctx,
		Method:         "HEAD",
		Path:           s.Path(container, path),
		ExpectedStatus: []int{http.StatusOK},
		RespConsume:    true,
	})
	if err != nil {
		return nil, err
	}

	info = SwiftObjectFromHeaders(path, res.Header)

	return info, nil
}

func (s *Swift) GetObject(ctx context.Context, container string, path string, span *ioutils.FileSpan) (obj *SwiftObject, err error) {
	req := httpclient.RequestData{
		Context:        ctx,
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
		return nil, err
	}

	obj = SwiftObjectFromHeaders(path, res.Header)
	obj.Reader = res.Body

	return obj, nil
}

func (s *Swift) PutObject(ctx context.Context, container string, path string, reader io.Reader) (err error) {
	_, err = s.Request(&httpclient.RequestData{
		Context:        ctx,
		Method:         "PUT",
		Path:           s.Path(container, path),
		ReqReader:      reader,
		ExpectedStatus: []int{http.StatusCreated},
		RespConsume:    true,
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *Swift) PutObjectManifest(ctx context.Context, container string, path string, manifestContainer string, manifestPath string) (err error) {
	req := httpclient.RequestData{
		Context:        ctx,
		Method:         "PUT",
		Path:           s.Path(container, path),
		Headers:        make(http.Header),
		ExpectedStatus: []int{http.StatusCreated},
		RespConsume:    true,
	}

	manifest := s.Path(manifestContainer, manifestPath)
	req.Headers.Set("X-Object-Manifest", manifest)

	_, err = s.Request(&req)
	if err != nil {
		return err
	}

	return nil
}

func (s *Swift) DeleteObject(ctx context.Context, container string, path string) (err error) {
	_, err = s.Request(&httpclient.RequestData{
		Context:        ctx,
		Method:         "DELETE",
		Path:           s.Path(container, path),
		ExpectedStatus: []int{http.StatusNoContent},
		RespConsume:    true,
	})
	if err != nil {
		return err
	}

	return nil
}
