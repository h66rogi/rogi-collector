package discovery

import (
	"github.com/h66rogi/rogi-collector/shared/model"
	"testing"
)

func TestDiscoveriesImplementInterface(t *testing.T) {
	var _ PlatformDiscovery = (*ChzzkDiscovery)(nil)
	var _ PlatformDiscovery = (*SoopDiscovery)(nil)
	var _ PlatformDiscovery = (*CimeDiscovery)(nil)
}

func TestDiscoveryPlatforms(t *testing.T) {
	tests := []struct {
		name     string
		platform model.Platform
		got      model.Platform
	}{
		{name: "chzzk", platform: model.PlatformChzzk, got: func() model.Platform { d, _ := NewChzzkDiscovery(nil, "", "", 1.0); return d.Platform() }()},
		{name: "soop", platform: model.PlatformSoop, got: NewSoopDiscovery(nil).Platform()},
		{name: "cime", platform: model.PlatformCime, got: NewCimeDiscovery(nil).Platform()},
	}

	for _, tt := range tests {
		if tt.got != tt.platform {
			t.Fatalf("%s platform = %s, want %s", tt.name, tt.got, tt.platform)
		}
	}
}
