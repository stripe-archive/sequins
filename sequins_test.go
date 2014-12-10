package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stripe/sequins/backend"
)

func getSequins(t *testing.T, opts sequinsOptions) *sequins {
	backend := backend.NewLocalBackend("test_data")
	s := newSequins(backend, opts)

	go func() {
		err := s.start("localhost:0")
		if err != nil {
			t.Fatal(err)
		}
	}()

	time.Sleep(100 * time.Millisecond)
	return s
}

func TestSequins(t *testing.T) {
	ts := getSequins(t, sequinsOptions{"test_data", false})

	req, _ := http.NewRequest("GET", "/Alice", nil)
	w := httptest.NewRecorder()
	ts.ServeHTTP(w, req)

	assert.Equal(t, 200, w.Code)
	assert.Equal(t, "Practice", w.Body.String(), "Practice")

	req, _ = http.NewRequest("GET", "/foo", nil)
	w = httptest.NewRecorder()
	ts.ServeHTTP(w, req)

	assert.Equal(t, 404, w.Code)
	assert.Equal(t, "", w.Body.String())

	req, _ = http.NewRequest("GET", "/", nil)
	w = httptest.NewRecorder()
	ts.ServeHTTP(w, req)

	now := time.Now().Unix() - 1
	status := &status{}
	err := json.Unmarshal(w.Body.Bytes(), status)
	assert.Nil(t, err)
	assert.Equal(t, 200, w.Code, 200)
	assert.Equal(t, "test_data/1", status.Path)
	assert.True(t, status.Started >= now)
	assert.Equal(t, 3, status.Count)
}

func TestSequinsNoValidDirectories(t *testing.T) {
	backend := backend.NewLocalBackend("test_data/0")
	s := newSequins(backend, sequinsOptions{"test_data/0", false})
	err := s.start("localhost:0")
	assert.Error(t, err)
}

func TestSequinsCors(t *testing.T) {
	ts := getSequins(t, sequinsOptions{"test_data", false})

	req, _ := http.NewRequest("GET", "/Alice", nil)
	req.Header.Add("Origin", "something.org")
	w := httptest.NewRecorder()
	ts.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	assert.Equal(t, []string{"something.org"}, w.HeaderMap["Access-Control-Allow-Origin"])
}
