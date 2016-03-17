package lockfile_test

import (
	lockfile "."
	"fmt"
	"os"
	"path/filepath"
)

func ExampleLockfile() {
	lock, err := lockfile.New(filepath.Join(os.TempDir(), "lock.me.now.lck"))
	if err != nil {
		fmt.Printf("Cannot init lock. reason: %v\n", err)
		panic(err)
	}
	err = lock.TryLock()

	// Error handling is essential, as we only try to get the lock.
	if err != nil {
		fmt.Printf("Cannot lock \"%v\", reason: %v\n", lock, err)
		panic(err)
	}

	defer lock.Unlock()

	fmt.Println("Do stuff under lock")
	// Output: Do stuff under lock
}
