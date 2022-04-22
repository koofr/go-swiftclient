package swiftclient_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestSwiftclient(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Swiftclient Suite")
}
