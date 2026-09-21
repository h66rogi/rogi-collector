import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile, writeFile} from 'node:fs/promises';
import {create, fromBinary, toBinary} from '@bufbuild/protobuf';
import {timestampFromDate} from '@bufbuild/protobuf/wkt';
import {DonationEventSchema, IdentityStatus, NativeCurrency, Platform, ResolveConsumerRecoveryResponseSchema} from '../dist/collector/v1/collector_pb.js';
import {validateDonation} from '../dist/validate.js';

const fixture = () => create(DonationEventSchema, {
  schemaVersion:'v1', eventType:'donation', eventId:'evt-cross-1', channelId:'channel-1',
  platform:Platform.SOOP, platformChannelId:'soop-channel-1', nativeBalloonCount:33n,
  nativeCurrency:NativeCurrency.SOOP_BALLOON, donationKind:'unknown_future_kind', donorId:'donor-1',
  donorDisplayName:'Synthetic donor', message:'synthetic message', connectionEpoch:'epoch-1',
  connectionSequence:18446744073709551615n, cursor:{journalGeneration:'generation-1',channelOffset:18446744073709551615n},
  identityStatus:IdentityStatus.OBSERVATION_ONLY, qualityReasons:['occurred_at_missing'],
  relatedObservationIds:['observation-1'], observedAt:timestampFromDate(new Date(123456)),
});

test('TypeScript and Go share deterministic protobuf wire bytes', async () => {
  const tsWire=toBinary(DonationEventSchema,fixture());
  await writeFile('contracts/fixtures/ts-donation.bin',tsWire);
  const goWire=await readFile('contracts/fixtures/go-donation.bin');
  assert.deepEqual(tsWire,new Uint8Array(goWire));
  const decoded=fromBinary(DonationEventSchema,goWire);
  assert.equal(decoded.cursor.channelOffset,18446744073709551615n);
  assert.equal(decoded.connectionSequence,18446744073709551615n);
  assert.equal(decoded.donationKind,'unknown_future_kind');
  assert.equal(decoded.occurredAt,undefined);
  assert.equal(decoded.sourceEventId,undefined);
  assert.ok(decoded.observedAt);
});

test('unknown additive fields survive a TypeScript decode and encode', async () => {
  const base=toBinary(DonationEventSchema,fixture());
  const extended=new Uint8Array([...base,0xfa,0x07,0x01,0x78]);
  const encoded=toBinary(DonationEventSchema,fromBinary(DonationEventSchema,extended));
  assert.deepEqual(encoded,extended);
  await writeFile('contracts/fixtures/ts-additive.bin',extended);
});

test('native count is a positive JS safe integer and observedAt is required', () => {
  validateDonation(fixture());
  assert.throws(()=>validateDonation(create(DonationEventSchema,{...fixture(),nativeBalloonCount:9007199254740992n})));
  assert.throws(()=>validateDonation(create(DonationEventSchema,{...fixture(),observedAt:undefined})));
  for(const patch of [{eventId:''},{channelId:''},{platform:Platform.UNSPECIFIED},{platformChannelId:''},{nativeCurrency:NativeCurrency.UNSPECIFIED},{donorId:''},{connectionEpoch:''},{connectionSequence:0n},{cursor:{journalGeneration:'',channelOffset:1n}},{identityStatus:IdentityStatus.UNSPECIFIED}]) assert.throws(()=>validateDonation(create(DonationEventSchema,{...fixture(),...patch})));
});

test('recovery revision and both generation cursors retain uint64 maximum', async () => {
  const value=create(ResolveConsumerRecoveryResponseSchema,{previousGeneration:'old',newGeneration:'new',previousCursor:{journalGeneration:'old',channelOffset:18446744073709551615n},resumeFrom:{journalGeneration:'new',channelOffset:18446744073709551615n},recoveryRevision:18446744073709551615n,idempotencyKey:'synthetic-key'});
  const wire=toBinary(ResolveConsumerRecoveryResponseSchema,value);
  const decoded=fromBinary(ResolveConsumerRecoveryResponseSchema,wire);
  assert.equal(decoded.recoveryRevision,18446744073709551615n);
  assert.equal(decoded.previousCursor.channelOffset,18446744073709551615n);
  await writeFile('contracts/fixtures/ts-recovery.bin',wire);
});
