package swiftclient

import (
	"io"
	"net/http"
	"strconv"
	"time"
)

type SwiftObject struct {
	Name           string
	Hash           string
	Bytes          int64
	ContentType    string
	LastModified   *time.Time
	Subdir         string
	ObjectManifest string
	Reader         io.ReadCloser
}

func SwiftObjectFromHeaders(path string, headers http.Header) *SwiftObject {
	contentLength, _ := strconv.ParseInt(headers.Get("Content-Length"), 10, 0)

	lastModified := TimeFromTimestamp(headers.Get("X-Timestamp"))

	return &SwiftObject{
		Name:           path,
		Hash:           headers.Get("Etag"),
		Bytes:          contentLength,
		ContentType:    headers.Get("Content-Type"),
		LastModified:   lastModified,
		ObjectManifest: headers.Get("X-Object-Manifest"),
	}
}

func TimeFromTimestamp(timestamp string) *time.Time {
	secs, _ := strconv.ParseFloat(timestamp, 64)
	nano := int64(secs*1000) * 1000000
	t := time.Unix(0, nano).In(time.UTC)
	return &t
}
