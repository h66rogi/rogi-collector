package discovery

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/h66rogi/rogi-collector/shared/model"
)

type registeredFake struct {
	calls      []string
	broadCalls int
	live       map[string]bool
	failures   map[string]error
}

func (f *registeredFake) Platform() model.Platform { return model.PlatformSoop }
func (f *registeredFake) FetchLiveChannels(context.Context) (DiscoveryResult, error) {
	f.broadCalls++
	return DiscoveryResult{}, errors.New("broad enumeration forbidden")
}
func (f *registeredFake) IsChannelLive(_ context.Context, id string) (bool, error) {
	f.calls = append(f.calls, id)
	return f.live[id], f.failures[id]
}
func TestRegisteredDiscoveryEmptyMakesNoPlatformRequests(t *testing.T) {
	f := &registeredFake{}
	d := NewRegisteredDiscovery(f, nil)
	result, err := d.FetchLiveChannels(context.Background())
	if err != nil || len(result.Channels) != 0 || len(f.calls) != 0 || f.broadCalls != 0 {
		t.Fatal(result, f, err)
	}
	if live, err := d.IsChannelLive(context.Background(), "unregistered"); err != nil || live || len(f.calls) != 0 {
		t.Fatal("unregistered probe allowed")
	}
}
func TestRegisteredDiscoveryUsesExistingSingleChannelProbe(t *testing.T) {
	f := &registeredFake{live: map[string]bool{"approved-a": true, "approved-b": false}}
	approved := map[string]bool{"approved-b": true, "approved-a": true, "disabled": false}
	d := NewRegisteredDiscovery(f, approved)
	approved["late-injection"] = true
	result, err := d.FetchLiveChannels(context.Background())
	if err != nil || len(result.Channels) != 1 || result.Channels[0].ChannelID != "approved-a" || result.Partial {
		t.Fatal(result, err)
	}
	if !reflect.DeepEqual(f.calls, []string{"approved-a", "approved-b"}) || f.broadCalls != 0 {
		t.Fatal(f)
	}
}
func TestRegisteredDiscoveryFailureDoesNotPretendBroadcastEnded(t *testing.T) {
	f := &registeredFake{live: map[string]bool{"approved-a": true}, failures: map[string]error{"approved-b": errors.New("synthetic timeout")}}
	d := NewRegisteredDiscovery(f, map[string]bool{"approved-a": true, "approved-b": true})
	result, err := d.FetchLiveChannels(context.Background())
	if err != nil || !result.Partial || len(result.Channels) != 1 {
		t.Fatal(result, err)
	}
	f.failures["approved-a"] = errors.New("synthetic timeout")
	if _, err = d.FetchLiveChannels(context.Background()); err == nil {
		t.Fatal("complete failure appeared as an empty live set")
	}
}
