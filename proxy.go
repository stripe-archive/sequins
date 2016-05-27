package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

const proxyHeader = "X-Sequins-Proxied-To"

type proxyResponse struct {
	resp *http.Response
	err  error
}

var errProxyTimeout = errors.New("all peers timed out")

// proxy proxies the request, trying each peer that should have the key
// in turn. The total logical attempt will be capped at the configured proxy
// timeout, but individual peers will be tried using the following algorithm:
//   - Each interval of 'proxy_stage_timeout', starting immediately, a request
//     is kicked off to one random not-yet-tried peer. All requests after
//     the first run concurrently.
//   - If a request finishes successfully to any peer, the result is returned
//     immediately and the others are canceled, as long as the result was not
//     an error.
//   - If a request finishes, but resulted in an error, another is kicked off
//     immediately to another random not-yet-tried peer.
//   - This process continues until either all peers are being tried, in which
//     case the code just waits for one to finish. If the total 'proxy_timeout'
//     is hit at any point, the method returns immediately with an error and
//     cancels any running requests.
func (vs *version) proxy(w http.ResponseWriter, r *http.Request, peers []string) ([]byte, error) {
	responses := make(chan proxyResponse, len(peers))
	cancel := make(chan struct{})
	defer close(cancel)

	peerIndex := 0
	outstanding := 0
	totalTimeout := time.NewTimer(vs.sequins.config.Sharding.ProxyTimeout.Duration)
	for {
		stageTimeout := time.NewTimer(vs.sequins.config.Sharding.ProxyStageTimeout.Duration)
		if peerIndex < len(peers) {
			host := peers[peerIndex]
			// Adding a header to the response to track proxied requests.
			w.Header().Add(proxyHeader, host)
			url := fmt.Sprintf("http://%s%s?proxy=%s", host, r.URL.Path, vs.name)
			go vs.proxyAttempt(url, responses, cancel)
			peerIndex += 1
			outstanding += 1
		} else if outstanding == 0 {
			return nil, errNoAvailablePeers
		}

		select {
		case res := <-responses:
			if res.err != nil {
				log.Printf("Error proxying request to peer: %s", res.err)
				outstanding -= 1
				continue
			} else {
				return readResponse(res)
			}
		case <-totalTimeout.C:
			return nil, errProxyTimeout
		case <-stageTimeout.C:
		}
	}

	return nil, errNoAvailablePeers
}

func (vs *version) proxyAttempt(url string, res chan proxyResponse, cancel chan struct{}) {
	// Create a fresh request, so we don't pass on any baggage like
	// 'Connection: close' headers.
	proxyRequest, err := http.NewRequest("GET", url, nil)
	if err != nil {
		res <- proxyResponse{nil, err}
		return
	}

	proxyRequest.Cancel = cancel
	resp, err := http.DefaultClient.Do(proxyRequest)
	if err != nil {
		res <- proxyResponse{nil, err}
		return
	}

	if resp.StatusCode != 200 && resp.StatusCode != 404 {
		resp.Body.Close()
		res <- proxyResponse{nil, fmt.Errorf("got %d", resp.StatusCode)}
		return
	}

	res <- proxyResponse{resp, nil}
}

func readResponse(res proxyResponse) ([]byte, error) {
	resp := res.resp

	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		return nil, nil
	} else if resp.StatusCode != 200 {
		return nil, fmt.Errorf("got %d", resp.StatusCode)
	}

	return ioutil.ReadAll(resp.Body)
}
