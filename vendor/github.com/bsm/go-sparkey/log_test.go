package sparkey

import (
	"path/filepath"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("LogWriter", func() {
	var subject *LogWriter
	var fname string

	BeforeEach(func() {
		var err error
		fname = filepath.Join(testDir, "test")
		subject, err = CreateLogWriter(fname, nil)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		subject.Close()
	})

	It("should create/close/reopen instances", func() {
		Expect(subject.log).NotTo(BeNil())
		Expect(subject.Name()).To(Equal(fname + ".spl"))
		Expect(subject.Close()).NotTo(HaveOccurred())

		var err error
		subject, err = OpenLogWriter(fname)
		Expect(err).NotTo(HaveOccurred())
		Expect(subject.log).NotTo(BeNil())
	})

	It("should return errors when tryng something bad", func() {
		_, err := OpenLogWriter(filepath.Join(testDir, "missing"))
		Expect(err).To(Equal(ERROR_FILE_NOT_FOUND))
	})

	It("should add pairs", func() {
		Expect(subject.Put([]byte("k1"), []byte("v1"))).NotTo(HaveOccurred())
		Expect(subject.Put([]byte("k2"), []byte("v2"))).NotTo(HaveOccurred())
		Expect(subject.Put([]byte("k2"), []byte("other"))).NotTo(HaveOccurred())
		Expect(subject.Put([]byte{}, []byte{})).NotTo(HaveOccurred())
	})

	It("should delete keys", func() {
		Expect(subject.Put([]byte("k1"), []byte("v1"))).NotTo(HaveOccurred())
		Expect(subject.Delete([]byte("k1"))).NotTo(HaveOccurred())
		Expect(subject.Put([]byte("k2"), []byte("v2"))).NotTo(HaveOccurred())
	})

	It("should re-write hash-files", func() {
		Expect(subject.WriteHashFile(HASH_SIZE_AUTO)).NotTo(HaveOccurred())
		entries, _ := filepath.Glob(filepath.Join(testDir, "*"))
		Expect(entries).To(ConsistOf([]string{
			filepath.Join(testDir, "test.spi"),
			filepath.Join(testDir, "test.spl"),
		}))
	})

})

var _ = Describe("LogReader", func() {
	var subject *LogReader

	BeforeEach(func() {
		fname, err := writeDefaultTestHash()
		Expect(err).NotTo(HaveOccurred())
		subject, err = OpenLogReader(fname + ".spl")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		subject.Close()
	})

	It("should open log files", func() {
		Expect(subject.log).NotTo(BeNil())
		Expect(subject.Name()).To(ContainSubstring("test.spl"))
		Expect(subject.MaxKeyLen()).To(Equal(uint64(2)))
		Expect(subject.MaxValueLen()).To(Equal(uint64(9)))
		Expect(subject.Compression()).To(Equal(COMPRESSION_NONE))
		Expect(subject.CompressionBlockSize()).To(Equal(0))
		Expect(subject.Close()).NotTo(HaveOccurred())
	})

	It("should open iterators", func() {
		iter, err := subject.Iterator()
		Expect(err).NotTo(HaveOccurred())
		iter.Close()
	})

})
