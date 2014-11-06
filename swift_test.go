package swiftclient_test

import (
	"bytes"
	"fmt"
	"github.com/koofr/go-ioutils"
	"github.com/koofr/go-netutils"
	. "github.com/koofr/go-swiftclient"
	"github.com/koofr/go-swiftclient/fakeswift"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"io/ioutil"
	"time"
)

var _ = Describe("Swift", func() {
	var port int
	var fakeSwift *fakeswift.FakeSwift
	var swift *Swift

	container := "test-container"

	BeforeEach(func() {
		var err error

		port, err = netutils.UnusedPort()
		Expect(err).NotTo(HaveOccurred())

		fakeSwift, err = fakeswift.NewFakeSwift(port)
		Expect(err).NotTo(HaveOccurred())

		swift = NewSwift()
	})

	AfterEach(func() {
		if fakeSwift != nil {
			fakeSwift.Close()
		}
	})

	authenticate := func() {
		url := fmt.Sprintf("http://localhost:%d/auth/v1.0", port)
		err := swift.AuthenticateV1(url, "test:tester", "testing")
		Expect(err).NotTo(HaveOccurred())
	}

	Describe("Auth", func() {
		It("should authenticate", func() {
			authenticate()
			Expect("").To(Equal(""))
		})

		It("should reauthenticate", func() {
			authenticate()

			err := swift.PutObject(container, "f1", bytes.NewBufferString("12345"))
			Expect(err).NotTo(HaveOccurred())

			// swift will forget auth token, client should reauthenticate

			fakeSwift.Close()

			fakeSwift, err = fakeswift.NewFakeSwift(port)
			Expect(err).NotTo(HaveOccurred())

			// first PUT must fail because we cannot cache request reader
			err = swift.PutObject(container, "f2", bytes.NewBufferString("12345"))
			Expect(err).To(HaveOccurred())

			// second PUT must not fail, because we reauthenticated
			err = swift.PutObject(container, "f2", bytes.NewBufferString("12345"))
			Expect(err).NotTo(HaveOccurred())
		})

		It("should reauthenticate and retry idempotent requests", func() {
			authenticate()

			_, err := swift.ListObjects(container, "/", true)
			Expect(err).NotTo(HaveOccurred())

			// swift will forget auth token, client should reauthenticate

			fakeSwift.Close()

			fakeSwift, err = fakeswift.NewFakeSwift(port)
			Expect(err).NotTo(HaveOccurred())

			// GET must not fail because it's an idempotent request and can be repeated
			_, err = swift.ListObjects(container, "/", true)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("API", func() {
		BeforeEach(func() {
			authenticate()
		})

		Describe("ObjectInfo", func() {
			It("should get object info", func() {
				info, err := swift.ObjectInfo(container, "file.txt")

				Expect(err).NotTo(HaveOccurred())

				modified := time.Date(2013, time.April, 22, 16, 58, 36, 698000000, time.UTC)

				Expect(info).To(Equal(&SwiftObject{
					Name:         "file.txt",
					Hash:         "827ccb0eea8a706c4c34a16891f84e7b",
					Bytes:        5,
					ContentType:  "text/plain",
					LastModified: &modified,
				}))
			})
		})

		Describe("GetObject", func() {
			It("should get object", func() {
				obj, err := swift.GetObject(container, "file.txt", nil)

				Expect(err).NotTo(HaveOccurred())

				Expect(obj.Reader).NotTo(BeNil())

				reader := obj.Reader
				obj.Reader = nil

				modified := time.Date(2013, time.April, 22, 16, 58, 36, 698000000, time.UTC)

				Expect(obj).To(Equal(&SwiftObject{
					Name:         "file.txt",
					Hash:         "827ccb0eea8a706c4c34a16891f84e7b",
					Bytes:        5,
					ContentType:  "text/plain",
					LastModified: &modified,
				}))

				data, _ := ioutil.ReadAll(reader)
				reader.Close()

				Expect(string(data)).To(Equal("12345"))
			})

			It("should get object with slash in name", func() {
				obj, err := swift.GetObject(container, "dir1/file1.txt", nil)

				Expect(err).NotTo(HaveOccurred())

				Expect(obj.Bytes).To(Equal(int64(10)))

				obj.Reader.Close()
			})

			It("should get object range", func() {
				span := &ioutils.FileSpan{2, 3}
				obj, err := swift.GetObject(container, "file.txt", span)

				Expect(err).NotTo(HaveOccurred())

				Expect(obj.Bytes).To(Equal(int64(2)))

				data, _ := ioutil.ReadAll(obj.Reader)
				obj.Reader.Close()

				Expect(string(data)).To(Equal("34"))
			})
		})

		Describe("PutObject", func() {
			It("should put object", func() {
				body := bytes.NewBufferString("12345")

				err := swift.PutObject(container, "new-file.txt", body)
				Expect(err).NotTo(HaveOccurred())

				info, err := swift.ObjectInfo(container, "new-file.txt")
				Expect(err).NotTo(HaveOccurred())

				Expect(info.Bytes).To(Equal(int64(5)))
			})

			It("should not put object if body is broken", func() {
				body := &ioutils.ErrorReader{fmt.Errorf("Broken body")}

				err := swift.PutObject(container, "error.txt", body)
				Expect(err).To(HaveOccurred())
			})
		})

		Describe("PutObjectManifest", func() {
			It("should put object manifest", func() {
				err := swift.PutObject(container, "segments/new-file/00001", bytes.NewBufferString("01234"))
				Expect(err).NotTo(HaveOccurred())

				err = swift.PutObject(container, "segments/new-file/00002", bytes.NewBufferString("56789"))
				Expect(err).NotTo(HaveOccurred())

				err = swift.PutObjectManifest(container, "new-file.txt", container, "segments/new-file")
				Expect(err).NotTo(HaveOccurred())

				info, err := swift.ObjectInfo(container, "new-file.txt")
				Expect(err).NotTo(HaveOccurred())

				Expect(info.Bytes).To(Equal(int64(10)))
			})
		})

		Describe("DeleteObject", func() {
			It("should delete object", func() {
				err := swift.DeleteObject(container, "file.txt")
				Expect(err).NotTo(HaveOccurred())

				_, err = swift.ObjectInfo(container, "file.txt")
				Expect(err).To(HaveOccurred())
			})
		})
	})
})
