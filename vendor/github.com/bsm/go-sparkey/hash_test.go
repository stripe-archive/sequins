package sparkey

import (
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("WriteHashFile", func() {

	It("should write hash files for existing logs", func() {
		fname, err := writeDefaultTestHash()
		Expect(err).NotTo(HaveOccurred())

		err = WriteHashFile(fname, HashSize(3))
		Expect(err).To(Equal(ERROR_HASH_SIZE_INVALID))

		err = WriteHashFile(fname, HASH_SIZE_64BIT)
		Expect(err).NotTo(HaveOccurred())

		stat, err := os.Stat(fname + ".spi")
		Expect(err).NotTo(HaveOccurred())
		Expect(stat.Size()).To(Equal(int64(148)))

		err = WriteHashFile(fname, HASH_SIZE_32BIT)
		Expect(err).NotTo(HaveOccurred())

		stat, err = os.Stat(fname + ".spi")
		Expect(err).NotTo(HaveOccurred())
		Expect(stat.Size()).To(Equal(int64(136)))
	})

})

var _ = Describe("HashReader", func() {
	var subject *HashReader

	BeforeEach(func() {
		fname, err := writeDefaultTestHash()
		Expect(err).NotTo(HaveOccurred())
		subject, err = Open(fname)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		subject.Close()
	})

	It("should open hash files", func() {
		Expect(subject.hash).NotTo(BeNil())
		Expect(subject.Name()).To(ContainSubstring("test.spi"))
		Expect(subject.LogName()).To(ContainSubstring("test.spl"))
		Expect(subject.NumSlots()).To(Equal(uint64(2)))
		Expect(subject.NumCollisions()).To(Equal(uint64(0)))
		subject.Close()
	})

	It("should provide access to associated log files", func() {
		Expect(subject.Log().Name()).To(Equal(subject.LogName()))
	})

	It("should create log iterators", func() {
		iter, err := subject.Iterator()
		Expect(err).NotTo(HaveOccurred())
		defer iter.Close()

		Expect(iter).To(BeAssignableToTypeOf(&HashIter{}))
	})

	It("should retrieve values", func() {
		// Get missing
		val, err := subject.Get([]byte("missing"))
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeNil())

		// Other missing
		val, err = subject.Get([]byte("x"))
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeNil())

		// Get existing
		val, err = subject.Get([]byte("zk"))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(val)).To(Equal("last"))

		val, err = subject.Get([]byte("xk"))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(val)).To(Equal("short"))

		// Get deleted
		val, err = subject.Get([]byte("yk"))
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeNil())
	})

})
