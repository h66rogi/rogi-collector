import { IdentityStatus, NativeCurrency, Platform, type DonationEvent } from './collector/v1/collector_pb.js';
export function validateDonation(value: DonationEvent): void {
  if (value.nativeBalloonCount < 1n || value.nativeBalloonCount > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error('nativeBalloonCount outside positive JS safe integer range');
  if (!value.observedAt || value.observedAt.seconds < -62135596800n || value.observedAt.seconds > 253402300799n || value.observedAt.nanos < 0 || value.observedAt.nanos > 999999999) throw new Error('valid observedAt required');
  if (!value.schemaVersion.trim() || value.eventType !== 'donation' || !value.eventId.trim() || !value.channelId.trim() || value.platform !== Platform.SOOP || !value.platformChannelId.trim() || value.nativeCurrency !== NativeCurrency.SOOP_BALLOON || !value.donationKind.trim() || !value.donorId.trim() || !value.connectionEpoch.trim() || value.connectionSequence === 0n || !value.cursor?.journalGeneration.trim() || value.cursor.channelOffset === 0n || value.identityStatus === IdentityStatus.UNSPECIFIED) throw new Error('required donation identity/domain field missing');
}
