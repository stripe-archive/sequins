package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/sequins/zk/zktest"
)

const expectTimeout = 10 * time.Second

type testVersion string

const dbName = "db"
const (
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
	source     string
	sequinses  []*testSequins
	zk         *zk.TestCluster
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

	cmdError    chan error
	process     *exec.Cmd
	log         *os.File
	progression chan testVersion
}

func newTestCluster(t *testing.T) *testCluster {
	binary, _ := filepath.Abs("sequins")
	if _, err := os.Stat(binary); err != nil {
		t.Skip("Skipping functional cluster tests because no binary is available. Please run the tests with 'make test'.")
	}

	source, err := ioutil.TempDir("", "sequins-cluster-")
	require.NoError(t, err)

	zk := zktest.New(t)

	// Give the zookeeper cluster a chance to start up.
	time.Sleep(1 * time.Second)

	// We have a specific transport to the client, so it doesn't try to reuse
	// connections between tests
	var testClient = &http.Client{
		Timeout:   5000 * time.Millisecond,
		Transport: &http.Transport{},
	}

	return &testCluster{
		T:          t,
		binary:     binary,
		source:     source,
		sequinses:  make([]*testSequins, 0),
		zk:         zk,
		testClient: testClient,
	}
}

type configOption func(*sequinsConfig)

func repl(r int) configOption {
	return func(config *sequinsConfig) {
		config.Sharding.Replication = r
	}
}

func minRepl(r int) configOption {
	return func(config *sequinsConfig) {
		config.Sharding.MinReplication = r
	}
}

func maxRepl(r int) configOption {
	return func(config *sequinsConfig) {
		config.Sharding.MaxReplication = r
	}
}

func noShardID() configOption {
	return func(config *sequinsConfig) {
		config.Sharding.ShardID = ""
	}
}

func shardID(id string) configOption {
	return func(config *sequinsConfig) {
		config.Sharding.ShardID = id
	}
}

func featureFlags(file *os.File) configOption {
	return func(config *sequinsConfig) {
		config.GoforitFlagJsonPath = file.Name()
		config.Sharding.ClusterName = ""
	}
}

func (tc *testCluster) addSequins(opts ...configOption) *testSequins {
	port := zktest.RandomPort()
	path := filepath.Join(tc.source, fmt.Sprintf("node-%d", port))

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
	config.Source = backendPath
	config.LocalStore = path
	config.RequireSuccessFile = true
	config.Sharding.Enabled = true
	config.Sharding.TimeToConverge = duration{100 * time.Millisecond}
	config.Sharding.ProxyTimeout = duration{600 * time.Millisecond}
	config.Sharding.AdvertisedHostname = "localhost"
	config.Sharding.ShardID = strconv.Itoa(len(tc.sequinses) + 1)
	config.ZK.Servers = []string{fmt.Sprintf("localhost:%d", tc.zk.Servers[0].Port)}
	config.Test.AllowLocalCluster = true

	// Slow everything down to an observable level.
	config.ThrottleLoads = duration{5 * time.Millisecond}
	config.Test.UpgradeDelay = duration{1 * time.Second}

	for _, opt := range opts {
		opt(&config)
	}

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

func (tc *testCluster) addSequinses(n int, opts ...configOption) {
	for i := 0; i < n; i++ {
		tc.addSequins(opts...)
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

func (ts *testSequins) getPartitions(version testVersion) []int {
	url := fmt.Sprintf("http://%s/healthz", ts.name)
	res, err := ts.testClient.Get(url)
	require.NoError(ts, err)

	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	require.NoError(ts, err)

	var dbs map[string]map[testVersion]nodeVersionStatus
	err = json.Unmarshal(body, &dbs)
	require.NoError(ts, err)

	partitions := make([]int, 0)
	if status, ok := dbs[dbName][version]; ok {
		partitions = status.Partitions
		sort.Ints(partitions)
	}

	return partitions
}

func (tc *testCluster) startTest() {
	var wg sync.WaitGroup
	wg.Add(len(tc.sequinses))

	for _, ts := range tc.sequinses {
		go func(ts *testSequins) {
			defer wg.Done()
			ts.startTest()
		}(ts)
	}

	wg.Wait()
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
		tc.T.Logf("Output for %s at %s", ts.name, ts.log.Name())
	}

	tc.zk.Stop()
	os.RemoveAll(tc.source)
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
	err := directoryCopy(ts.T, path, "test_databases/healthy/baby-names/1")
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
	ts.start()

	go func() {
		lastVersion := noVersion
		bootingUp := true

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
					if lastVersion != down {
						ts.T.Logf("%s (lastVersion: %s)", err, lastVersion)
					}

					version = down
				}
			}

			if bootingUp && version == down {
				version = lastVersion
			} else {
				bootingUp = false
			}

			if version != lastVersion {
				ts.progression <- version
				lastVersion = version
			}

			// Sleep for 250 milliseconds less the time we took to make the last
			// request, such that we make a request roughly every 250 milliseconds.
			time.Sleep((250 * time.Millisecond) - time.Now().Sub(now))
		}
	}()
}
func (ts *testSequins) start() {
	for i := 0; i < 3; i++ {
		ts.setup()

		log, err := ioutil.TempFile("", "sequins-test-cluster-")
		require.NoError(ts.T, err, "setup: creating log")

		ts.log = log
		ts.process = exec.Command(ts.binary, "--config", ts.configPath)
		ts.process.Stdout = log
		ts.process.Stderr = log

		ts.process.Start()
		ts.cmdError = make(chan error, 1)
		go func(cmdError chan error) {
			cmdError <- ts.process.Wait()
		}(ts.cmdError)
		select {
		case err = <-ts.cmdError:
			ts.config.Bind = fmt.Sprintf("localhost:%d", zktest.RandomPort())
			ts.name = ts.config.Bind
		case <-time.After(time.Second * 1):
			return
		}
	}

	ts.T.Fatal("Sequins did not start up after 3 tries")
}

func (ts *testSequins) hup() {
	ts.process.Process.Signal(syscall.SIGHUP)
}

func (ts *testSequins) stop() {
	ts.process.Process.Signal(syscall.SIGTERM)
	<-ts.cmdError
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
	if testing.Short() {
		t.Skip("skipping cluster test in short mode.")
	}
	t.Parallel()

	tc := newTestCluster(t)
	defer tc.tearDown()

	tc.addSequinses(1)
	tc.makeVersionAvailable(v3)
	tc.expectProgression(v3)

	tc.startTest()
	tc.assertProgression()
}

// TestClusterUpgradingSingleNode tests that a node can upgrade to one version, and
// then upgrade a second and third time.
func TestClusterUpgradingSingleNode(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster test in short mode.")
	}
	t.Parallel()

	tc := newTestCluster(t)
	defer tc.tearDown()

	tc.addSequinses(1)
	tc.makeVersionAvailable(v1)
	tc.expectProgression(v1, v2, v3)
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
	if testing.Short() {
		t.Skip("skipping cluster test in short mode.")
	}
	t.Parallel()

	tc := newTestCluster(t)
	defer tc.tearDown()

	tc.addSequinses(3, minRepl(2))
	tc.makeVersionAvailable(v3)
	tc.expectProgression(v3)
	tc.startTest()
	tc.assertProgression()
}

// TestClusterEmpty tests that a cluster with many nodes and no preexisting
// state can start up and serve requests.
func TestLargeClusterEmpty(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster test in short mode.")
	}
	t.Parallel()

	tc := newTestCluster(t)
	defer tc.tearDown()

	tc.addSequinses(30, minRepl(2))
	tc.makeVersionAvailable(v3)
	tc.expectProgression(v3)
	tc.startTest()
	tc.assertProgression()
}

// TestClusterUpgrading tests that a cluster can upgrade to one version, and
// then upgrade a second and third time.
func TestClusterUpgrading(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster test in short mode.")
	}
	t.Parallel()

	tc := newTestCluster(t)
	defer tc.tearDown()

	tc.addSequinses(3, minRepl(2))
	tc.makeVersionAvailable(v1)
	tc.expectProgression(v1, v2, v3)
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
	if testing.Short() {
		t.Skip("skipping cluster test in short mode.")
	}
	t.Parallel()

	tc := newTestCluster(t)
	defer tc.tearDown()

	tc.addSequinses(3, minRepl(2))
	tc.expectProgression(v1, v2)

	tc.makeVersionAvailable(v1)
	tc.startTest()

	time.Sleep(expectTimeout)
	tc.makeVersionAvailable(v2)
	tc.sequinses[0].hup()

	time.Sleep(expectTimeout)
	tc.hup()

	tc.assertProgression()
}

// TestClusterNoDowngrade tests that a cluster will never downgrade to an older
// version, even if the newer one is unavailable.
func TestClusterNoDowngrade(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster test in short mode.")
	}
	t.Parallel()

	tc := newTestCluster(t)
	defer tc.tearDown()

	tc.addSequinses(3, minRepl(2))
	tc.expectProgression(v3)

	tc.makeVersionAvailable(v3)
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
	if testing.Short() {
		t.Skip("skipping cluster test in short mode.")
	}
	t.Parallel()

	tc := newTestCluster(t)
	defer tc.tearDown()

	tc.addSequinses(3, minRepl(2))
	tc.expectProgression(v3)

	tc.makeVersionAvailable(v3)
	tc.startTest()
	time.Sleep(expectTimeout)

	s := tc.addSequins(minRepl(2))
	s.makeVersionAvailable(v3)
	s.expectProgression(v3)
	s.startTest()

	tc.assertProgression()
}

// TestClusterNodeWithoutData tests if a node can safely stay behind while
// the rest of the cluster upgrades.
func TestClusterNodeWithoutData(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster test in short mode.")
	}
	t.Parallel()

	tc := newTestCluster(t)
	defer tc.tearDown()

	// Needs repl(3) so two nodes can cover two of each partition.
	tc.addSequinses(3, repl(3), minRepl(2))

	// By default this is 10 minutes; we're reducing it to confirm that
	// nodes are not removing versions that their peers still have.
	tc.sequinses[0].config.Test.VersionRemoveTimeout = duration{5 * time.Second}
	tc.sequinses[1].config.Test.VersionRemoveTimeout = duration{5 * time.Second}
	tc.sequinses[2].config.Test.VersionRemoveTimeout = duration{5 * time.Second}

	// Because it's behind, it's expected that the first node will flap when it
	// proxies requests and gets v2 from peers.
	// tc.sequinses[0].expectProgression(v1, v3)
	tc.sequinses[1].expectProgression(v1, v2, v3)
	tc.sequinses[2].expectProgression(v1, v2, v3)

	tc.makeVersionAvailable(v1)
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
	if testing.Short() {
		t.Skip("skipping cluster test in short mode.")
	}
	t.Parallel()

	tc := newTestCluster(t)
	defer tc.tearDown()

	tc.addSequinses(3, minRepl(2))
	tc.makeVersionAvailable(v3)
	tc.expectProgression(v3, down, v3)
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
	if testing.Short() {
		t.Skip("skipping cluster test in short mode.")
	}
	t.Parallel()

	tc := newTestCluster(t)
	defer tc.tearDown()

	tc.addSequinses(3, minRepl(2))
	tc.makeVersionAvailable(v2)
	tc.sequinses[0].expectProgression(v2, down, v3)
	tc.sequinses[1].expectProgression(v2, v3)
	tc.sequinses[2].expectProgression(v2, v3)
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

type crashTest struct {
	MinReplication    int
	VersionAfterCrash string
	MissingData       bool
}

func testCrash(t *testing.T, testCase crashTest) {
	tc := newTestCluster(t)
	defer tc.tearDown()

	tc.addSequinses(3, repl(2), minRepl(testCase.MinReplication))

	// Keep one sequins permanently hanging
	tc.sequinses[1].config.Test.Hang = hangAfterRead{
		Version: "v2",
		File:    "part-00003",
	}

	tc.makeVersionAvailable(v1)
	tc.expectProgression(v1)
	tc.startTest()

	tc.assertProgression()

	tc.makeVersionAvailable(v2)
	tc.hup()
	time.Sleep(expectTimeout)

	// Simulate a crash.
	tc.sequinses[2].stop()
	// Ensure node 2 is down.
	url := fmt.Sprintf("http://%s/%s/%s", tc.sequinses[2].name, dbName, babyNames[0].key)
	_, err := tc.testClient.Get(url)
	assert.Error(t, err)

	testCrashStatus(t, tc, testCase.VersionAfterCrash, testCase.MissingData)

	// When nodes come back up, we should be fine.
	tc.sequinses[1].stop()
	tc.sequinses[1].config.Test.Hang = hangAfterRead{}
	tc.sequinses[1].start()
	tc.sequinses[2].start()

	time.Sleep(expectTimeout)
	testCrashStatus(t, tc, "v2", false)
}

func testCrashStatus(t *testing.T, tc *testCluster, versionAfterCrash string, missingData bool) {
	// Check for missing data.
	numErrors := 0
	numSuccess := 0
	for _, name := range babyNames {
		url := fmt.Sprintf("http://%s/%s/%s", tc.sequinses[0].name, dbName, name.key)
		resp, err := tc.testClient.Get(url)
		require.NoError(t, err)
		resp.Body.Close()
		if resp.StatusCode >= 500 && resp.StatusCode < 600 {
			numErrors++
		} else {
			require.Equal(t, 200, resp.StatusCode)
			numSuccess++
			v := resp.Header.Get("X-Sequins-Version")
			assert.Equal(t, versionAfterCrash, v)
		}
	}
	assert.NotZero(t, numSuccess)

	// If data should be missing, node 0 returns error for at least some key.
	if missingData {
		assert.NotZero(t, numErrors)
	} else {
		assert.Zero(t, numErrors)
	}
}

// TestClusterCrashMin tests that min_replication allows robustness in case
// of a crash just after a version is complete.
func TestClusterCrashMin(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster test in short mode.")
	}
	t.Parallel()

	testCases := []crashTest{
		// With min_replication = 1, we prematurely go to v2, and then data is missing
		// after a crash.
		{1, "v2", true},
		// With min_replication = 2, we hold off on v2, and still can serve v1.
		{2, "v1", false},
	}
	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("MinRepl-%d", testCase.MinReplication), func(t *testing.T) {
			t.Parallel()
			testCrash(t, testCase)
		})
	}
}

func testMinReplication(t *testing.T, mrepl int, progression testVersion) {
	tc := newTestCluster(t)
	defer tc.tearDown()

	tc.addSequinses(4, repl(3), minRepl(mrepl))
	tc.makeVersionAvailable(v1)
	tc.expectProgression(v1)
	tc.startTest()
	tc.assertProgression()

	tc.sequinses[3].stop()
	tc.makeVersionAvailable(v2)
	tc.hup()
	tc.sequinses[3].expectProgression(down)
	for i := 0; i <= 2; i++ {
		tc.sequinses[i].expectProgression(progression)
	}
	tc.assertProgression()
}

// TestClusterMin tests that 1 < min_replication < replication < count(shard_id)
// allows upgrades even if a node goes down.
func TestClusterMin(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster test in short mode.")
	}
	t.Parallel()

	testCases := []struct {
		MinReplication int
		Progression    testVersion
	}{
		{3, timeout},
		{2, v2},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("MinRepl-%d", testCase.MinReplication), func(t *testing.T) {
			t.Parallel()
			testMinReplication(t, testCase.MinReplication, testCase.Progression)
		})
	}
}

// TestMaxReplication tests that nodes within the cluster will respect
// partition replication limits.
func TestMaxReplication(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster test in short mode.")
	}
	t.Parallel()

	tc := newTestCluster(t)
	defer tc.tearDown()

	// Create a partially full cluster. Both nodes should grab all the partitions.
	s := tc.addSequins(repl(2), maxRepl(2), shardID("1"))
	s.makeVersionAvailable(v1)
	s.expectProgression(v1)
	s.startTest()
	time.Sleep(expectTimeout)

	s = tc.addSequins(repl(2), maxRepl(2), shardID("2"))
	s.makeVersionAvailable(v1)
	s.expectProgression(v1)
	s.startTest()
	time.Sleep(expectTimeout)

	expectedAssignments := [][]int{{0, 1, 2, 3, 4}, {0, 1, 2, 3, 4}}
	for i, host := range tc.sequinses {
		id := host.getShardID(t)
		require.Equal(t, expectedAssignments[i], host.getPartitions(v1), "partition assignment for node (%s) should be correct", id)
	}

	// Add a node that will respect the maximum replication and thus fetch
	// no nodes.
	s = tc.addSequins(repl(2), maxRepl(2), shardID("3"))
	s.makeVersionAvailable(v1)
	s.expectProgression(v1)
	s.startTest()
	time.Sleep(expectTimeout)

	expectedAssignments = [][]int{{0, 1, 2, 3, 4}, {0, 1, 2, 3, 4}, {}}
	for i, host := range tc.sequinses {
		id := host.getShardID(t)
		require.Equal(t, expectedAssignments[i], host.getPartitions(v1), "partition assignment for node (%s) should be correct", id)
	}

	// Add a node that will have the maximum replication limit disabled and
	// thus fetch partitions as normal.
	s = tc.addSequins(repl(2), maxRepl(0), shardID("4"))
	s.makeVersionAvailable(v1)
	s.expectProgression(v1)
	s.startTest()
	time.Sleep(expectTimeout)

	expectedAssignments = [][]int{{0, 1, 2, 3, 4}, {0, 1, 2, 3, 4}, {}, {1, 3}}
	for i, host := range tc.sequinses {
		id := host.getShardID(t)
		require.Equal(t, expectedAssignments[i], host.getPartitions(v1), "partition assignment for node (%s) should be correct", id)
	}
}

func (ts *testSequins) getShardID(t *testing.T) string {
	url := fmt.Sprintf("http://%s/healthz", ts.name)

	resp, err := ts.testClient.Get(url)
	assert.NoError(t, err)
	defer resp.Body.Close()

	return resp.Header.Get("X-Sequins-Shard-ID")
}

func TestClusterAutoAssignSingleID(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster test in short mode.")
	}
	t.Parallel()

	tc := newTestCluster(t)
	defer tc.tearDown()

	s1 := tc.addSequins(noShardID())
	tc.makeVersionAvailable(v3)
	tc.startTest()
	time.Sleep(expectTimeout)

	shardID := s1.getShardID(t)
	assert.Equal(t, "1", shardID)
}

// Test shardID auto-assignment when spinning up nodes one at a time.
func TestClusterAutoAssignMultipleIDs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster test in short mode.")
	}
	t.Parallel()

	tc := newTestCluster(t)
	defer tc.tearDown()

	for i := 1; i <= 3; i++ {
		s := tc.addSequins(noShardID())
		s.makeVersionAvailable(v3)
		s.startTest()
		time.Sleep(expectTimeout)
	}

	for i, s := range tc.sequinses {
		shardID := fmt.Sprintf("%d", i+1)
		assert.Equal(t, shardID, s.getShardID(t))
	}
}

// Test shardID auto-assignment when the nodes start at the same time.
func TestClusterAutoAssignParallelIDs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster test in short mode.")
	}
	t.Parallel()

	tc := newTestCluster(t)
	defer tc.tearDown()

	tc.addSequinses(3, noShardID())
	tc.makeVersionAvailable(v3)
	tc.startTest()
	time.Sleep(expectTimeout * 3)

	for i := 1; i <= 3; i++ {
		shardID := fmt.Sprintf("%d", i)

		found := false
		for _, s := range tc.sequinses {
			if s.getShardID(t) == shardID {
				found = true
				break
			}
		}
		assert.True(t, found, "None of the nodes had shardID %q", shardID)
	}
}

// Down each node in a cluster one at a time and then ensure shardID auto-assignment will use
// the newly unused shardID.
func TestClusterAutoAssignUnusedID(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster test in short mode.")
	}
	t.Parallel()

	tc := newTestCluster(t)
	defer tc.tearDown()

	tc.addSequinses(3)

	tc.makeVersionAvailable(v3)
	tc.startTest()
	time.Sleep(expectTimeout)

	// This doesn't actual test anything valuable. If these fail then that means we've probably changed from
	// 1-indexed to 0-indexed Sequins IDs.
	for i, s := range tc.sequinses {
		shardID := fmt.Sprintf("%d", i+1)
		assert.Equal(t, shardID, s.getShardID(t), "Sanity check for pre-configured ID failed")
	}

	for i, s := range tc.sequinses {
		shardID := fmt.Sprintf("%d", i+1)
		assert.Equal(t, shardID, s.getShardID(t))

		s.stop()

		newS := tc.addSequins(noShardID())
		newS.makeVersionAvailable(v3)
		newS.startTest()
		time.Sleep(expectTimeout)
		assert.Equal(t, shardID, newS.getShardID(t))
	}
}

// TestClusterPartitionAssignment tests that the partitions are assigned to
// nodes as expected since the assignments should be deterministic.
func TestClusterPartitionAssignment(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster test in short mode.")
	}
	t.Parallel()

	tc := newTestCluster(t)
	defer tc.tearDown()

	tc.addSequinses(5, repl(3))
	tc.makeVersionAvailable(v1)
	tc.expectProgression(v1)
	tc.startTest()
	time.Sleep(expectTimeout)

	hosts := tc.sequinses
	cmpHosts := func(i, j int) bool {
		return hosts[i].getShardID(t) < hosts[j].getShardID(t)
	}
	sort.Slice(hosts, cmpHosts)

	expectedAssignments := [][]int{{0, 1, 3}, {0, 2, 3}, {0, 2, 4}, {1, 2, 4}, {1, 3, 4}}
	oldAssignments := make(map[string][]int)
	for i, host := range hosts {
		id := host.getShardID(t)
		oldAssignments[id] = host.getPartitions(v1)
		require.Equal(t, expectedAssignments[i], oldAssignments[id], "partition assignment for node (%s) should be correct", id)
	}

	// Test that adding a node results in the correct assignment for that
	// node. Note that the current nodes should not change which partitions
	// they own.
	s := tc.addSequins(repl(3))
	s.makeVersionAvailable(v1)
	s.expectProgression(v1)
	s.startTest()
	time.Sleep(expectTimeout)

	hosts = tc.sequinses
	sort.Slice(hosts, cmpHosts)

	expectedAssignments = [][]int{{0, 2, 4}, {0, 2, 4}, {0, 2, 4}, {1, 3}, {1, 3}, {1, 3}}
	for i, host := range hosts {
		id := host.getShardID(t)
		if partitions, ok := oldAssignments[id]; ok {
			require.Equal(t, partitions, host.getPartitions(v1), "partition assignment for existing node (%s) should be unaffected", id)
		} else {
			require.Equal(t, expectedAssignments[i], host.getPartitions(v1), "partition assignment for new node (%s) should be correct", id)
		}
	}

	// Test that when making a new version available, the partition
	// assignments for all nodes get updated correctly.
	tc.makeVersionAvailable(v2)
	tc.hup()
	tc.expectProgression(v2)
	time.Sleep(expectTimeout)

	hosts = tc.sequinses
	sort.Slice(hosts, cmpHosts)

	for i, host := range hosts {
		id := host.getShardID(t)
		require.Equal(t, expectedAssignments[i], host.getPartitions(v2), "partitions assignment for node (%s) should be correct", id)
	}
}

func TestRestartingNodeWhileRemoteRefreshDisabled(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster test in short mode.")
	}
	t.Parallel()

	// Create a feature flags file where remote refreshes are allowed
	flagsFile, err := ioutil.TempFile("", "sequins-test-cluster-flags-")
	require.NoError(t, err)
	defer os.Remove(flagsFile.Name())
	writeFlag(t, flagsFile, "sequins.prevent_download", false)

	tc := newTestCluster(t)
	defer tc.tearDown()

	tc.addSequinses(1, featureFlags(flagsFile))
	tc.makeVersionAvailable(v1)
	tc.expectProgression(v1, down, v1)
	tc.startTest()

	// download initial dataset
	time.Sleep(expectTimeout)

	// stop node
	tc.sequinses[0].stop()

	// prevent download
	writeFlag(t, flagsFile, "sequins.prevent_download", true)

	// make new version available
	tc.makeVersionAvailable(v2)

	// restart node while downloads prevented
	tc.sequinses[0].start()

	tc.assertProgression()
}
