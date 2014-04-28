package swiftclient_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestSwiftclient(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Swiftclient Suite")
}
