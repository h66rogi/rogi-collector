package connector

import (
	"fmt"

	"github.com/h66rogi/rogi-collector/shared/model"
)

// ConnectorConfig holds optional configuration for platform connectors.
type ConnectorConfig struct {
	SoopAnonymousTest bool
	SoopCookieFile    string // JSON snapshot from cookie-auth; credentials stay in that component.
	SoopRelayAddr     string // optional relay address (for example, "relay.example.invalid:443")
	RelayMetrics      *RelayMetrics
	OnMessageDropped  func(platform string) // called when a message is dropped due to full buffer
	CimeWSURL         string                // required to connect to ci.me chat
	// SoopChatHostSuffixes restricts platform-provided websocket hosts. At
	// least one suffix is required before a SOOP connection can be opened.
	SoopChatHostSuffixes []string
	// ChzzkDCProxyURL routes chzzk's HTTP API calls (live-detail, access-token)
	// through an operator-supplied HTTP proxy. WebSocket connections remain direct.
	// An empty string uses direct HTTP.
	ChzzkDCProxyURL string
}

// globalConfig is set once at startup via SetConnectorConfig.
var globalConfig ConnectorConfig

// SetConnectorConfig sets the global connector configuration.
func SetConnectorConfig(cfg ConnectorConfig) {
	globalConfig = cfg
}

// NewConnector returns the appropriate PlatformConnector for the given platform.
func NewConnector(platform model.Platform) (PlatformConnector, error) {
	switch platform {
	case model.PlatformChzzk:
		return NewChzzkConnector(globalConfig.ChzzkDCProxyURL), nil
	case model.PlatformSoop:
		connector := NewSoopConnector(globalConfig.SoopRelayAddr, globalConfig.RelayMetrics, globalConfig.SoopChatHostSuffixes...)
		if !globalConfig.SoopAnonymousTest {
			connector.SetCookieFile(globalConfig.SoopCookieFile)
		}
		return connector, nil
	case model.PlatformCime:
		return NewCimeConnector(globalConfig.CimeWSURL), nil
	default:
		return nil, fmt.Errorf("unknown platform: %s", platform)
	}
}
