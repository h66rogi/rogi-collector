package main

import (
	"net/http"
	"os"
	"time"
)

func main() {
	url := "http://127.0.0.1:8080/readyz"
	if len(os.Args) > 1 {
		url = os.Args[1]
	}
	client := http.Client{Timeout: 2 * time.Second}
	r, e := client.Get(url)
	if e != nil {
		os.Exit(1)
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		os.Exit(1)
	}
}
