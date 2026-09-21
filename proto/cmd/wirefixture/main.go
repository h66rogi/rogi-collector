package main

import (
	"os"
	"time"
	collectorv1 "github.com/h66rogi/rogi-collector/proto/gen/collector/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func main(){
	v:=&collectorv1.DonationEvent{SchemaVersion:"v1",EventType:"donation",EventId:"evt-cross-1",ChannelId:"channel-1",Platform:collectorv1.Platform_PLATFORM_SOOP,PlatformChannelId:"soop-channel-1",NativeBalloonCount:33,NativeCurrency:collectorv1.NativeCurrency_NATIVE_CURRENCY_SOOP_BALLOON,DonationKind:"unknown_future_kind",DonorId:"donor-1",DonorDisplayName:"Synthetic donor",Message:"synthetic message",ConnectionEpoch:"epoch-1",ConnectionSequence:^uint64(0),Cursor:&collectorv1.Cursor{JournalGeneration:"generation-1",ChannelOffset:^uint64(0)},IdentityStatus:collectorv1.IdentityStatus_IDENTITY_STATUS_OBSERVATION_ONLY,QualityReasons:[]string{"occurred_at_missing"},RelatedObservationIds:[]string{"observation-1"},ObservedAt:timestamppb.New(time.Unix(123,456000000))}
	b,err:=proto.MarshalOptions{Deterministic:true}.Marshal(v);if err!=nil{panic(err)}
	if err=os.WriteFile("contracts/fixtures/go-donation.bin",b,0o644);err!=nil{panic(err)}
}
