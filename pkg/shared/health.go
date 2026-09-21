package shared

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"
)

type Health struct {
	Role               string `json:"role"`
	ProcessReady       bool   `json:"processReady"`
	ConfiguredChannels int    `json:"configuredChannels"`
	CollectionActive   bool   `json:"collectionActive"`
	ContractVersion    string `json:"contractVersion"`
	RPCImplementation  string `json:"rpcImplementation"`
}

func Run(role string) {
	listen := flag.String("listen", ":8080", "health listen address")
	check := flag.String("healthcheck", "", "check a health URL and exit")
	flag.Parse()
	if *check != "" {
		client := http.Client{Timeout: 2 * time.Second}
		response, err := client.Get(*check)
		if err != nil || response.StatusCode != http.StatusOK {
			os.Exit(1)
		}
		_ = response.Body.Close()
		return
	}
	health := Health{Role: role, ProcessReady: true, ConfiguredChannels: 0, CollectionActive: false, ContractVersion: "v1", RPCImplementation: "unimplemented"}
	http.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(health)
	})
	fmt.Printf("%s process health listening on %s; no channels configured\n", role, *listen)
	if err := http.ListenAndServe(*listen, nil); err != nil {
		panic(err)
	}
}
