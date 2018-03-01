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

var (
	errProxyTimeout    = errors.New("all peers timed out")
	errRequestCanceled = errors.New("client-side request canceled")
)

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
	totalTimeout := time.NewTimer(vs.sequins.config.Sharding.ProxyTimeout.Duration)

	// Per the documentation for `http.Client.Do`, if that returns successfully,
	// callers are required to the close the response Body, even if the request
	// is cancelled mid-flight.
	outstanding := 0
	defer func() {
		go func() {
			for ; outstanding > 0; outstanding-- {
				res := <-responses
				if res.err == nil {
					res.resp.Body.Close()
				}
			}
			close(responses)
		}()
	}()

	cancels := make(map[string]context.CancelFunc, len(peers))
	for peerIndex := 0; ; peerIndex++ {
		stageTimeout := time.NewTimer(vs.sequins.config.Sharding.ProxyStageTimeout.Duration)

		if peerIndex < len(peers) {
			peer := peers[peerIndex]

			attemptCtx, cancelAttempt := context.WithCancel(r.Context())
			req, err := vs.newProxyRequest(attemptCtx, r.URL.Path, peer)
			if err != nil {
				cancelAttempt()
				log.Printf("Error initializing request to peer %s: %s", peer, err)
			} else {
				cancels[peer] = cancelAttempt
				outstanding += 1
				go vs.proxyAttempt(req, peer, responses)
			}
		} else if outstanding == 0 {
			return nil, "", errNoAvailablePeers
		}

		select {
		case res := <-responses:
			outstanding -= 1
			if res.err != nil {
				log.Printf("Error proxying request to peer %s: %s", res.peer, res.err)
				cancels[res.peer]()
			} else {
				// Cancel any other outstanding attempts.
				for peer, cancelAttempt := range cancels {
					if peer != res.peer {
						cancelAttempt()
					}
				}

				return res.resp, res.peer, nil
			}
		case <-totalTimeout.C:
			for _, cancelAttempt := range cancels {
				cancelAttempt()
			}
			return nil, "", errProxyTimeout
		case <-r.Context().Done():
			for _, cancelAttempt := range cancels {
				cancelAttempt()
			}
			return nil, "", errRequestCanceled
		case <-stageTimeout.C:
		}
	}
}

func (vs *version) proxyAttempt(proxyRequest *http.Request, peer string, res chan proxyResponse) {
	resp, err := http.DefaultClient.Do(proxyRequest)
	if err != nil {
		res <- proxyResponse{nil, peer, err}
		return
	}

	if resp.StatusCode != 200 && resp.StatusCode != 404 {
		resp.Body.Close()
		res <- proxyResponse{nil, peer, fmt.Errorf("got %d", resp.StatusCode)}
		return
	}

	res <- proxyResponse{resp, peer, nil}
}

// newProxyRequest creates a fresh request, to avoid passing on baggage like
// 'Connection: close' headers.
func (vs *version) newProxyRequest(ctx context.Context, path, peer string) (*http.Request, error) {
	url := &url.URL{
		Scheme:   "http",
		Host:     peer,
		Path:     path,
		RawQuery: fmt.Sprintf("proxy=%s", vs.name),
	}

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return req, err
	}

	return req.WithContext(ctx), nil
}
