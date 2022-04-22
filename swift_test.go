package swiftclient_test

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/google/uuid"
	"github.com/koofr/go-ioutils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/koofr/go-swiftclient"
)

var _ = Describe("Swift", func() {
	var ctx context.Context
	var baseURL string
	var container string
	var swift *Swift

	authenticate := func() {
		err := swift.AuthenticateV1(ctx, baseURL+"/auth/v1.0", "test:test", "test")
		Expect(err).NotTo(HaveOccurred())
	}

	BeforeEach(func() {
		ctx = context.Background()

		baseURL = os.Getenv("SWIFT_BASE_URL")
		if baseURL == "" {
			baseURL = "http://localhost:8080"
		}

		container = uuid.New().String()

		swift = NewSwift()

		authenticate()

		Expect(swift.PutContainer(ctx, container)).To(Succeed())

		err := swift.PutObject(ctx, container, "file.txt", bytes.NewBufferString("12345"))
		Expect(err).NotTo(HaveOccurred())
	})

	Describe("Auth", func() {
		It("should reauthenticate", func() {
			authenticate()

			err := swift.PutObject(ctx, container, "f1", bytes.NewBufferString("12345"))
			Expect(err).NotTo(HaveOccurred())

			swift.HTTPClient.Headers.Set("X-Auth-Token", "INVALIDTOKEN")

			// first PUT must fail because we cannot cache request reader
			err = swift.PutObject(ctx, container, "f2", bytes.NewBufferString("12345"))
			Expect(err).To(HaveOccurred())

			// second PUT must not fail, because we reauthenticated
			err = swift.PutObject(ctx, container, "f2", bytes.NewBufferString("12345"))
			Expect(err).NotTo(HaveOccurred())
		})

		It("should reauthenticate and retry idempotent requests", func() {
			authenticate()

			_, err := swift.ListObjects(ctx, container, "/", true)
			Expect(err).NotTo(HaveOccurred())

			swift.HTTPClient.Headers.Set("X-Auth-Token", "INVALIDTOKEN")

			// GET must not fail because it's an idempotent request and can be repeated
			_, err = swift.ListObjects(ctx, container, "/", true)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("API", func() {
		Describe("ObjectInfo", func() {
			It("should get object info", func() {
				info, err := swift.ObjectInfo(ctx, container, "file.txt")
				Expect(err).NotTo(HaveOccurred())

				Expect(info).To(Equal(&SwiftObject{
					Name:         "file.txt",
					Hash:         "827ccb0eea8a706c4c34a16891f84e7b",
					Bytes:        5,
					ContentType:  "text/plain",
					LastModified: info.LastModified,
				}))
				Expect(info.LastModified).NotTo(BeNil())
			})
		})

		Describe("GetObject", func() {
			It("should get object", func() {
				obj, err := swift.GetObject(ctx, container, "file.txt", nil)
				Expect(err).NotTo(HaveOccurred())
				Expect(obj.Reader).NotTo(BeNil())
				defer obj.Reader.Close()

				Expect(obj).To(Equal(&SwiftObject{
					Name:         "file.txt",
					Hash:         "827ccb0eea8a706c4c34a16891f84e7b",
					Bytes:        5,
					ContentType:  "text/plain",
					LastModified: obj.LastModified,
					Reader:       obj.Reader,
				}))
				Expect(obj.LastModified).NotTo(BeNil())

				data, err := ioutil.ReadAll(obj.Reader)
				Expect(err).NotTo(HaveOccurred())

				Expect(string(data)).To(Equal("12345"))
			})

			It("should get object with slash in name", func() {
				err := swift.PutObject(ctx, container, "test/file.txt", bytes.NewBufferString("0123456789"))
				Expect(err).NotTo(HaveOccurred())

				obj, err := swift.GetObject(ctx, container, "test/file.txt", nil)
				Expect(err).NotTo(HaveOccurred())
				defer obj.Reader.Close()

				Expect(obj.Bytes).To(Equal(int64(10)))
			})

			It("should get object range", func() {
				span := &ioutils.FileSpan{Start: 2, End: 3}
				obj, err := swift.GetObject(ctx, container, "file.txt", span)
				Expect(err).NotTo(HaveOccurred())
				defer obj.Reader.Close()

				Expect(obj.Bytes).To(Equal(int64(2)))

				data, err := ioutil.ReadAll(obj.Reader)
				Expect(err).NotTo(HaveOccurred())

				Expect(string(data)).To(Equal("34"))
			})
		})

		Describe("PutObject", func() {
			It("should put object", func() {
				body := bytes.NewBufferString("12345")

				err := swift.PutObject(ctx, container, "new-file.txt", body)
				Expect(err).NotTo(HaveOccurred())

				info, err := swift.ObjectInfo(ctx, container, "new-file.txt")
				Expect(err).NotTo(HaveOccurred())

				Expect(info.Bytes).To(Equal(int64(5)))
			})

			It("should not put object if body is broken", func() {
				body := ioutils.NewErrorReader(fmt.Errorf("Broken body"))

				err := swift.PutObject(ctx, container, "error.txt", body)
				Expect(err).To(HaveOccurred())
			})
		})

		Describe("PutObjectManifest", func() {
			It("should put object manifest", func() {
				err := swift.PutObject(ctx, container, "segments/new-file/00001", bytes.NewBufferString("01234"))
				Expect(err).NotTo(HaveOccurred())

				err = swift.PutObject(ctx, container, "segments/new-file/00002", bytes.NewBufferString("56789"))
				Expect(err).NotTo(HaveOccurred())

				err = swift.PutObjectManifest(ctx, container, "new-file.txt", container, "segments/new-file")
				Expect(err).NotTo(HaveOccurred())

				info, err := swift.ObjectInfo(ctx, container, "new-file.txt")
				Expect(err).NotTo(HaveOccurred())

				Expect(info.Bytes).To(Equal(int64(10)))
			})
		})

		Describe("DeleteObject", func() {
			It("should delete object", func() {
				err := swift.DeleteObject(ctx, container, "file.txt")
				Expect(err).NotTo(HaveOccurred())

				_, err = swift.ObjectInfo(ctx, container, "file.txt")
				Expect(err).To(HaveOccurred())
			})
		})
	})
})
