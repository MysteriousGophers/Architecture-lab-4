package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/mysteriousgophers/architecture-lab-4/httptools"
	"github.com/mysteriousgophers/architecture-lab-4/signal"
)

var (
	port       = flag.Int("port", 8090, "load balancer port")
	timeoutSec = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https      = flag.Bool("https", false, "whether backends support HTTPs")

	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

var (
	timeout     = time.Duration(*timeoutSec) * time.Second
	serversPool = []string{
		"server1:8080",
		"server2:8080",
		"server3:8080",
	}
	poolOfHealthyServers = make([]string, len(serversPool))
	poolLock             sync.Mutex
)

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

func health(dst string) bool {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", scheme(), dst), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	if resp.StatusCode != http.StatusOK {
		return false
	}
	return true
}

func forward(dst string, rw http.ResponseWriter, r *http.Request) error {
	ctx, _ := context.WithTimeout(r.Context(), timeout)
	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = dst
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = dst

	resp, err := http.DefaultClient.Do(fwdRequest)
	if err == nil {
		for k, values := range resp.Header {
			for _, value := range values {
				rw.Header().Add(k, value)
			}
		}
		if *traceEnabled {
			rw.Header().Set("lb-from", dst)
		}
		log.Println("fwd", resp.StatusCode, resp.Request.URL)
		rw.WriteHeader(resp.StatusCode)
		defer resp.Body.Close()
		_, err := io.Copy(rw, resp.Body)
		if err != nil {
			log.Printf("Failed to write response: %s", err)
		}
		return nil
	} else {
		log.Printf("Failed to get response from %s: %s", dst, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
}

func healthCheck(servers []string) {
	healthStatus := make(map[string]bool)
	for _, server := range servers {
		healthStatus[server] = true
	}

	for i, server := range servers {
		go func(server string) {
			for range time.Tick(10 * time.Second) {
				isHealthy := health(server)
				poolLock.Lock()

				if isHealthy {
					healthStatus[server] = true
					poolOfHealthyServers[i] = server
				} else {
					healthStatus[server] = false
					poolOfHealthyServers[i] = ""
				}

				poolOfHealthyServers = nil

				for _, server := range servers {
					if healthStatus[server] {
						poolOfHealthyServers = append(poolOfHealthyServers, server)
					}
				}

				poolLock.Unlock()
				log.Println(server, isHealthy)
			}
		}(server)
	}
}

func main() {
	flag.Parse()

	healthCheck(serversPool)

	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		// TODO: Рееалізуйте свій алгоритм балансувальника.
		forward(serversPool[0], rw, r)
	}))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}
