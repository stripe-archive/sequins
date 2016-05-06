package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const expectTimeout = 10 * time.Second

type testVersion string

const dbName = "db"
const (
	start     testVersion = "START"
	noVersion testVersion = "NONE"
	v1        testVersion = "v1"
	v2        testVersion = "v2"
	v3        testVersion = "v3"
	down      testVersion = "DOWN"
	timeout   testVersion = "TIMEOUT"
)

type testCluster struct {
	*testing.T
	binary     string
	root       string
	sequinses  []*testSequins
	zk         testZK
	testClient *http.Client
}

type testSequins struct {
	*testing.T
	name                string
	binary              string
	storePath           string
	configPath          string
	backendPath         string
	config              sequinsConfig
	testClient          *http.Client
	expectedProgression []testVersion

	process        *exec.Cmd
	bufferedOutput bytes.Buffer
	progression    chan testVersion
}

func newTestCluster(t *testing.T) *testCluster {
	binary, _ := filepath.Abs("sequins")
	if _, err := os.Stat(binary); err != nil {
		t.Skip("Skipping functional cluster tests because no binary is available. Please run the tests with 'make test'.")
	}

	root, err := ioutil.TempDir("", "sequins-cluster-")
	require.NoError(t, err)

	zk := createTestZk(t)

	// Give the zookeeper cluster a chance to start up.
	time.Sleep(1 * time.Second)

	// We have a specific transport to the client, so it doesn't try to reuse
	// connections between tests
	var testClient = &http.Client{
		Timeout:   500 * time.Millisecond,
		Transport: &http.Transport{},
	}

	return &testCluster{
		T:          t,
		binary:     binary,
		root:       root,
		sequinses:  make([]*testSequins, 0),
		zk:         zk,
		testClient: testClient,
	}
}

func (tc *testCluster) addSequins() *testSequins {
	port := randomPort()
	path := filepath.Join(tc.root, fmt.Sprintf("node-%d", port))

	storePath := filepath.Join(path, "store")
	err := os.MkdirAll(storePath, 0755|os.ModeDir)
	require.NoError(tc.T, err, "setup: create store path")

	backendPath := filepath.Join(path, "backend")
	err = os.MkdirAll(backendPath, 0755|os.ModeDir)
	require.NoError(tc.T, err, "setup: create backend path")

	configPath := filepath.Join(path, "sequins.conf")

	config := defaultConfig()
	name := fmt.Sprintf("localhost:%d", port)
	config.Bind = name
	config.Root = backendPath
	config.LocalStore = path
	config.RequireSuccessFile = true
	config.ZK.Servers = []string{tc.zk.addr}
	config.ZK.TimeToConverge = duration{100 * time.Millisecond}
	config.ZK.ProxyTimeout = duration{300 * time.Millisecond}
	config.ZK.AdvertisedHostname = "localhost"
	config.Test.AllowLocalCluster = true

	// Slow everything down to an observable level.
	config.ThrottleLoads = duration{5 * time.Millisecond}
	config.Test.UpgradeDelay = duration{1 * time.Second}

	s := &testSequins{
		T:           tc.T,
		name:        name,
		binary:      tc.binary,
		backendPath: backendPath,
		configPath:  configPath,
		config:      config,
		testClient:  tc.testClient,

		progression: make(chan testVersion, 1024),
	}

	tc.sequinses = append(tc.sequinses, s)
	return s
}

func (tc *testCluster) addSequinses(n int) {
	for i := 0; i < n; i++ {
		tc.addSequins()
	}
}

func (tc *testCluster) expectProgression(versions ...testVersion) {
	for _, ts := range tc.sequinses {
		ts.expectProgression(versions...)
	}
}

func (tc *testCluster) setup() {
	for _, ts := range tc.sequinses {
		ts.setup()
	}
}

func (tc *testCluster) makeVersionAvailable(version testVersion) {
	for _, ts := range tc.sequinses {
		ts.makeVersionAvailable(version)
	}
}

func (tc *testCluster) removeAvailableVersion(version testVersion) {
	for _, ts := range tc.sequinses {
		ts.removeAvailableVersion(version)
	}
}

func (tc *testCluster) startTest() {
	for _, ts := range tc.sequinses {
		ts.startTest()
	}
}

func (tc *testCluster) assertProgression() {
	for _, ts := range tc.sequinses {
		ts.assertProgression()
	}
}

func (tc *testCluster) hup() {
	for _, ts := range tc.sequinses {
		ts.hup()
	}
}

func (tc *testCluster) tearDown() {
	for _, ts := range tc.sequinses {
		ts.process.Process.Kill()
		tc.T.Logf("===== OUTPUT FOR %s\n%s", ts.name, ts.bufferedOutput.String())
	}

	tc.zk.close()
	os.RemoveAll(tc.root)
}

func (ts *testSequins) expectProgression(versions ...testVersion) {
	ts.expectedProgression = versions
}

func (ts *testSequins) setup() {
	f, err := os.Create(ts.configPath)
	require.NoError(ts.T, err, "setup: create config")

	err = toml.NewEncoder(f).Encode(ts.config)
	require.NoError(ts.T, err, "setup: create config")
	f.Close()
}

func (ts *testSequins) makeVersionAvailable(version testVersion) {
	path := filepath.Join(ts.backendPath, dbName, string(version))
	err := directoryCopy(ts.T, path, "test/baby-names/1")
	require.NoError(ts.T, err, "setup: make version available: %s", version)

	f, err := os.Create(filepath.Join(path, "_SUCCESS"))
	require.NoError(ts.T, err, "setup: make version available: %s", version)

	f.Close()
}

func (ts *testSequins) removeAvailableVersion(version testVersion) {
	path := filepath.Join(ts.backendPath, dbName, string(version))
	os.RemoveAll(path)
}

func (ts *testSequins) startTest() {
	versions := make(chan testVersion)

	go func() {
		lastVersion := start
		for {
			now := time.Now()
			key := babyNames[rand.Intn(len(babyNames))].key
			url := fmt.Sprintf("http://%s/%s/%s", ts.name, dbName, key)

			var version testVersion
			resp, err := ts.testClient.Get(url)
			if err == nil {
				v := resp.Header.Get("X-Sequins-Version")
				resp.Body.Close()

				if resp.StatusCode > 404 {
					version = down
				} else if v == "" {
					version = noVersion
				} else {
					version = testVersion(v)
				}
			} else {
				// A number of timeouts are ok - this isn't the friendliest environment,
				// after all. We want to fail fast and frequently so that we don't
				// miss changes to the available version.
				netErr, ok := err.(net.Error)
				if ok && netErr.Timeout() {
					version = lastVersion
				} else {
					if lastVersion != down && lastVersion != start {
						ts.T.Logf("%s (lastVersion: %s)", err, lastVersion)
					}

					version = down
				}
			}

			if version != lastVersion {
				versions <- version
				lastVersion = version
			}

			// Sleep for 250 milliseconds less the time we took to make the last
			// request, such that we make a request roughly every 250 milliseconds.
			time.Sleep((250 * time.Millisecond) - time.Now().Sub(now))
		}
	}()

	if ts.process == nil {
		go func() {
			// Wait for the process to register as down, then start it.
			first := <-versions
			require.Equal(ts.T, down, first, "setup: sequins process should start as down")

			ts.start()
			ts.progression <- first
			for v := range versions {
				ts.progression <- v
			}
		}()
	}
}

func (ts *testSequins) start() {
	ts.process = exec.Command(ts.binary, "--config", ts.configPath)
	ts.process.Stdout = &ts.bufferedOutput
	ts.process.Stderr = &ts.bufferedOutput

	ts.process.Start()
}

func (ts *testSequins) hup() {
	ts.process.Process.Signal(syscall.SIGHUP)
}

func (ts *testSequins) stop() {
	ts.process.Process.Signal(syscall.SIGTERM)
	ts.process.Process.Wait()
}

func (ts *testSequins) assertProgression() {
	var actualProgression []testVersion

Progression:
	for range ts.expectedProgression {
		t := time.NewTimer(expectTimeout)

		select {
		case v := <-ts.progression:
			actualProgression = append(actualProgression, v)
		case <-t.C:
			actualProgression = append(actualProgression, timeout)
			break Progression
		}
	}

	expected := ""
	for _, v := range ts.expectedProgression {
		if expected != "" {
			expected += " -> "
		}

		expected += string(v)
	}

	actual := ""
	for _, v := range actualProgression {
		if actual != "" {
			actual += " -> "
		}

		actual += string(v)
	}

	assert.Equal(ts.T, expected, actual, "unexpected progression for %s", ts.name)
}

// TestClusterEmptySingleNode tests that a node with no preexisting state can start up
// and serve requests.
func TestClusterEmptySingleNode(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping cluster test in short mode.")
	}

	tc := newTestCluster(t)
	defer tc.tearDown()

	tc.addSequinses(1)
	tc.makeVersionAvailable(v3)
	tc.expectProgression(down, noVersion, v3)

	tc.setup()
	tc.startTest()
	tc.assertProgression()
}

// TestClusterUpgradingSingleNode tests that a node can upgrade to one version, and
// then upgrade a second and third time.
func TestClusterUpgradingSingleNode(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping cluster test in short mode.")
	}

	tc := newTestCluster(t)
	defer tc.tearDown()

	tc.addSequinses(1)
	tc.makeVersionAvailable(v1)
	tc.expectProgression(down, noVersion, v1, v2, v3)

	tc.setup()
	tc.startTest()

	time.Sleep(expectTimeout)
	tc.makeVersionAvailable(v2)
	tc.hup()

	time.Sleep(expectTimeout)
	tc.makeVersionAvailable(v3)
	tc.hup()

	tc.assertProgression()
}

// TestClusterEmpty tests that a cluster with no preexisting state can start up
// and serve requests.
func TestClusterEmpty(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping cluster test in short mode.")
	}

	tc := newTestCluster(t)
	defer tc.tearDown()

	tc.addSequinses(3)
	tc.makeVersionAvailable(v3)
	tc.expectProgression(down, noVersion, v3)

	tc.setup()
	tc.startTest()
	tc.assertProgression()
}

// TestClusterEmpty tests that a cluster with many nodes and no preexisting
// state can start up and serve requests.
func TestLargeClusterEmpty(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping cluster test in short mode.")
	}

	tc := newTestCluster(t)
	defer tc.tearDown()

	tc.addSequinses(30)
	tc.makeVersionAvailable(v3)
	tc.expectProgression(down, noVersion, v3)

	tc.setup()
	tc.startTest()
	tc.assertProgression()
}

// TestClusterUpgrading tests that a node can upgrade to one version, and
// then upgrade a second and third time.
func TestClusterUpgrading(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping cluster test in short mode.")
	}

	tc := newTestCluster(t)
	defer tc.tearDown()

	tc.addSequinses(3)
	tc.makeVersionAvailable(v1)
	tc.expectProgression(down, noVersion, v1, v2, v3)

	tc.setup()
	tc.startTest()

	time.Sleep(expectTimeout)
	tc.makeVersionAvailable(v2)
	tc.hup()

	time.Sleep(expectTimeout)
	tc.makeVersionAvailable(v3)
	tc.hup()

	tc.assertProgression()
}

// TestClusterDelayedUpgrade tests that one node can upgrade several seconds earlier
// that the rest of the cluster without losing any reads.
func TestClusterDelayedUpgrade(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping cluster test in short mode.")
	}

	tc := newTestCluster(t)
	defer tc.tearDown()

	tc.addSequinses(3)
	tc.expectProgression(down, noVersion, v1, v2)

	tc.makeVersionAvailable(v1)
	tc.setup()
	tc.startTest()

	time.Sleep(expectTimeout)
	tc.makeVersionAvailable(v2)
	tc.sequinses[0].hup()

	time.Sleep(expectTimeout)
	tc.hup()

	tc.assertProgression()
}

// TestClusterNoDowngrade tests that a cluster will never downgrade to an older
// version, even if the newer one is available.
func TestClusterNoDowngrade(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping cluster test in short mode.")
	}

	tc := newTestCluster(t)
	defer tc.tearDown()

	tc.addSequinses(3)
	tc.expectProgression(down, noVersion, v3)

	tc.makeVersionAvailable(v3)
	tc.setup()
	tc.startTest()

	time.Sleep(expectTimeout)
	tc.makeVersionAvailable(v2)
	tc.removeAvailableVersion(v3)
	tc.hup()

	tc.assertProgression()
}

// TestClusterLateJoin tests if a node can join an existing cluster and
// immediately start serving the version that the rest of the cluster has.
func TestClusterLateJoin(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping cluster test in short mode.")
	}

	tc := newTestCluster(t)
	defer tc.tearDown()

	tc.addSequinses(3)
	tc.expectProgression(down, noVersion, v3)

	tc.makeVersionAvailable(v3)
	tc.setup()
	tc.startTest()
	time.Sleep(expectTimeout)

	s := tc.addSequins()
	s.makeVersionAvailable(v3)
	s.setup()
	s.expectProgression(down, v3)
	s.startTest()

	tc.assertProgression()
}

// TestClusterNodeWithoutData tests if a node can safely stay behind while
// the rest of the cluster upgrades.
func TestClusterNodeWithoutData(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping cluster test in short mode.")
	}

	tc := newTestCluster(t)
	defer tc.tearDown()

	tc.addSequinses(3)

	// By default this is 10 minutes; we're reducing it to confirm that
	// nodes are not removing versions that their peers still have.
	tc.sequinses[0].config.Test.VersionRemoveTimeout = duration{5 * time.Second}
	tc.sequinses[1].config.Test.VersionRemoveTimeout = duration{5 * time.Second}
	tc.sequinses[2].config.Test.VersionRemoveTimeout = duration{5 * time.Second}

	tc.sequinses[0].expectProgression(down, noVersion, v1, v3)
	tc.sequinses[1].expectProgression(down, noVersion, v1, v2, v3)
	tc.sequinses[2].expectProgression(down, noVersion, v1, v2, v3)

	tc.makeVersionAvailable(v1)
	tc.setup()
	tc.startTest()

	time.Sleep(expectTimeout)
	tc.sequinses[1].makeVersionAvailable(v2)
	tc.sequinses[2].makeVersionAvailable(v2)
	tc.hup()

	time.Sleep(expectTimeout)
	tc.makeVersionAvailable(v3)
	tc.hup()

	tc.assertProgression()
}

// TestClusterRollingRestart tests that a cluster can be restarted a node at a
// time and stay on the same version continually.
func TestClusterRollingRestart(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping cluster test in short mode.")
	}

	tc := newTestCluster(t)
	defer tc.tearDown()

	tc.addSequinses(3)
	tc.makeVersionAvailable(v3)
	tc.expectProgression(down, noVersion, v3, down, v3)

	tc.setup()
	tc.startTest()
	time.Sleep(expectTimeout)

	for _, s := range tc.sequinses {
		s.stop()
		time.Sleep(expectTimeout)
		s.start()
		time.Sleep(expectTimeout)
	}

	tc.assertProgression()
}

// TestClusterNodeVacation tests that if a node is down while the rest of a
// cluster upgrades without it, it can rejoin without issue.
func TestClusterNodeVacation(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping cluster test in short mode.")
	}

	tc := newTestCluster(t)
	defer tc.tearDown()

	tc.addSequinses(3)
	tc.makeVersionAvailable(v2)
	tc.sequinses[0].expectProgression(down, noVersion, v2, down, v3)
	tc.sequinses[1].expectProgression(down, noVersion, v2, v3)
	tc.sequinses[2].expectProgression(down, noVersion, v2, v3)

	tc.setup()
	tc.startTest()
	time.Sleep(expectTimeout)

	tc.sequinses[0].stop()
	time.Sleep(expectTimeout)

	tc.makeVersionAvailable(v3)
	tc.sequinses[1].hup()
	tc.sequinses[2].hup()
	time.Sleep(expectTimeout)

	tc.sequinses[0].start()
	tc.assertProgression()
}
