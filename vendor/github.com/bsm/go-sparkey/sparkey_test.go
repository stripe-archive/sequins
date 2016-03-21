package sparkey

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Constants", func() {

	It("should include compression options", func() {
		Expect(COMPRESSION_NONE).To(Equal(CompressionType(0)))
		Expect(COMPRESSION_SNAPPY).To(Equal(CompressionType(1)))
	})

})

var _ = Describe("File name helpers", func() {

	It("should convert", func() {
		Expect(HashFileName("/tmp/my.spi/test")).To(Equal("/tmp/my.spi/test.spi"))
		Expect(HashFileName("/tmp/my.spi/test.spi")).To(Equal("/tmp/my.spi/test.spi"))
		Expect(HashFileName("/tmp/my.spi/test.spl")).To(Equal("/tmp/my.spi/test.spi"))
		Expect(HashFileName("/tmp/my.spi/test.ext")).To(Equal("/tmp/my.spi/test.ext.spi"))

		Expect(LogFileName("/tmp/my.spi/test")).To(Equal("/tmp/my.spi/test.spl"))
		Expect(LogFileName("/tmp/my.spi/test.spi")).To(Equal("/tmp/my.spi/test.spl"))
		Expect(LogFileName("/tmp/my.spi/test.spl")).To(Equal("/tmp/my.spi/test.spl"))
		Expect(LogFileName("/tmp/my.spi/test.ext")).To(Equal("/tmp/my.spi/test.ext.spl"))
	})

})

/** Test hook **/

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	BeforeEach(func() {
		var err error
		testDir, err = ioutil.TempDir("", "sparkey-tests")
		Expect(err).NotTo(HaveOccurred())
	})
	AfterEach(func() {
		os.RemoveAll(testDir)
	})
	RunSpecs(t, "sparkey")
}

/** Benchmarks **/

const bmSeedKeys = 100000

func benchmarkRun(b *testing.B, cb func(*HashReader)) {
	val := bytes.Repeat([]byte{'x'}, 512)
	dir, err := ioutil.TempDir("", "sparkey-tests")
	if err != nil {
		b.Fatal("error creating dir", err)
	}
	defer os.RemoveAll(dir)

	fname, err := writeTestHash(dir, func(w *LogWriter) error {
		for i := 0; i < bmSeedKeys; i += 2 {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, uint64(i))

			if err := w.Put(key, val); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal("error writing test data", err)
	}

	db, err := Open(fname)
	if err != nil {
		b.Fatal("error opening db", err)
	}
	defer db.Close()

	b.ResetTimer()
	cb(db)
}

func BenchmarkHashReaderGet(b *testing.B) {
	benchmarkRun(b, func(db *HashReader) {
		for i := 0; i < b.N; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, uint64(rand.Intn(bmSeedKeys)))

			if _, err := db.Get(key); err != nil {
				b.Fatal("error retrieving key", err)
			}
		}
	})
}

func BenchmarkHashIterGet(b *testing.B) {
	benchmarkRun(b, func(db *HashReader) {
		iter, err := db.Iterator()
		if err != nil {
			b.Fatal("error opening iterator", err)
		}
		defer iter.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, uint64(rand.Intn(bmSeedKeys)))

			if _, err := iter.Get(key); err != nil {
				b.Fatal("error retrieving key", err)
			}
		}
	})
}

/** Helpers **/

var testDir string

func writeTestHash(dir string, cb func(w *LogWriter) error) (string, error) {
	fname := filepath.Join(dir, "test")
	err := writeTestLog(LogFileName(fname), cb)
	if err != nil {
		return fname, err
	}
	return fname, WriteHashFile(fname, HASH_SIZE_64BIT)
}

func writeTestLog(fname string, cb func(w *LogWriter) error) error {
	w, err := CreateLogWriter(fname, nil)
	if err != nil {
		return err
	}
	defer w.Close()
	return cb(w)
}

func writeDefaultTestHash() (string, error) {
	return writeTestHash(testDir, func(w *LogWriter) (err error) {
		if err = w.Put([]byte("xk"), []byte("short")); err != nil {
			return
		}
		if err = w.Put([]byte("yk"), []byte("longvalue")); err != nil {
			return
		}
		if err = w.Put([]byte("zk"), []byte("last")); err != nil {
			return
		}
		if err = w.Delete([]byte("yk")); err != nil {
			return
		}
		return
	})
}
