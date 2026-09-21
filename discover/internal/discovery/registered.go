package discovery

import (
	"context"
	"errors"
	"sort"

	"github.com/h66rogi/rogi-collector/shared/model"
)

// RegisteredDiscovery narrows the existing platform adapter to approved IDs.
// It reuses IsChannelLive; FetchLiveChannels on the underlying adapter is never
// called. An empty set performs zero platform requests.
type RegisteredDiscovery struct {
	base     PlatformDiscovery
	ids      []string
	allowed  map[string]bool
	Enabled  func(context.Context, string) (bool, error)
	Observed func(context.Context, string, bool, error)
}

func NewRegisteredDiscovery(base PlatformDiscovery, approved map[string]bool) *RegisteredDiscovery {
	d := &RegisteredDiscovery{base: base, allowed: make(map[string]bool, len(approved))}
	for id, enabled := range approved {
		if id != "" && enabled {
			d.allowed[id] = true
			d.ids = append(d.ids, id)
		}
	}
	sort.Strings(d.ids)
	return d
}
func (d *RegisteredDiscovery) Platform() model.Platform { return d.base.Platform() }
func (d *RegisteredDiscovery) IsChannelLive(ctx context.Context, id string) (bool, error) {
	if !d.allowed[id] {
		return false, nil
	}
	if d.Enabled != nil {
		enabled, err := d.Enabled(ctx, id)
		if err != nil || !enabled {
			return false, err
		}
	}
	live, err := d.base.IsChannelLive(ctx, id)
	if d.Observed != nil {
		d.Observed(ctx, id, live, err)
	}
	return live, err
}
func (d *RegisteredDiscovery) FetchLiveChannels(ctx context.Context) (DiscoveryResult, error) {
	result := DiscoveryResult{}
	var failures []error
	checked := 0
	for _, id := range d.ids {
		if err := ctx.Err(); err != nil {
			return DiscoveryResult{}, err
		}
		live, err := d.IsChannelLive(ctx, id)
		if err != nil {
			failures = append(failures, err)
			result.Partial = true
			continue
		}
		checked++
		if live {
			result.Channels = append(result.Channels, DiscoveredChannel{ChannelID: id})
		}
	}
	if checked == 0 && len(failures) > 0 {
		return DiscoveryResult{}, errors.Join(failures...)
	}
	return result, nil
}
