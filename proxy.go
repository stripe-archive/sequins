package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"
)

const proxyHeader = "X-Sequins-Proxied-To"

type proxyResponse struct {
	resp *http.Response
	peer string
	err  error
}

var errProxyTimeout = errors.New("all peers timed out")

// proxy proxies the request, trying each peer that should have the key
// in turn. The total logical attempt will be capped at the configured proxy
// timeout, but individual peers will be tried in the order they are passed in,
// using the following algorithm:
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
func (vs *version) proxy(r *http.Request, peers []string) (*http.Response, string, error) {
	responses := make(chan proxyResponse, len(peers))
	ctx, _ := context.WithDeadline(r.Context(),
		time.Now().Add(vs.sequins.config.Sharding.ProxyTimeout.Duration))

	peerIndex := 0
	outstanding := 0
	for {
		stageTimeout := time.NewTimer(vs.sequins.config.Sharding.ProxyStageTimeout.Duration)
		if peerIndex < len(peers) {
			peer := peers[peerIndex]
			go vs.proxyAttempt(ctx, r.URL.Path, peer, responses)
			peerIndex += 1
			outstanding += 1
		} else if outstanding == 0 {
			return nil, "", errNoAvailablePeers
		}

		select {
		case res := <-responses:
			if res.err != nil {
				log.Printf("Error proxying request to peer: %s", res.err)
				outstanding -= 1
				continue
			} else {
				return res.resp, res.peer, nil
			}
		case <-ctx.Done():
			return nil, "", errProxyTimeout
		case <-stageTimeout.C:
		}
	}

	return nil, "", errNoAvailablePeers
}

func (vs *version) proxyAttempt(ctx context.Context, path, peer string, res chan proxyResponse) {
	// Create a fresh request, so we don't pass on any baggage like
	// 'Connection: close' headers.
	url := &url.URL{
		Scheme:   "http",
		Host:     peer,
		Path:     path,
		RawQuery: fmt.Sprintf("proxy=%s", vs.name),
	}

	proxyRequest, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		res <- proxyResponse{nil, "", err}
		return
	}

	resp, err := http.DefaultClient.Do(proxyRequest.WithContext(ctx))
	if err != nil {
		res <- proxyResponse{nil, "", err}
		return
	}

	if resp.StatusCode != 200 && resp.StatusCode != 404 {
		resp.Body.Close()
		res <- proxyResponse{nil, "", fmt.Errorf("got %d", resp.StatusCode)}
		return
	}

	res <- proxyResponse{resp, peer, nil}
}
