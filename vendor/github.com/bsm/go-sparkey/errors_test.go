package sparkey

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Error", func() {

	It("should generate messages", func() {
		Expect(Error(-303).Error()).To(Equal("sparkey: hash too small"))
		Expect(Error(23).Error()).To(Equal("sparkey: unknown error (23)"))
	})

})
