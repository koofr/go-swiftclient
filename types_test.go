package swiftclient_test

import (
	"time"

	. "github.com/koofr/go-swiftclient"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Types", func() {
	It("should parse time from timestamp", func() {
		ts := TimeFromTimestamp("1366649916.698")
		t := time.Date(2013, time.April, 22, 16, 58, 36, 698000000, time.UTC)
		Expect(ts).To(Equal(&t))
	})
})
