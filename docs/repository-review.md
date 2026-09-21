# 기존 저장소 조사와 초기 구상 기록

이 문서는 조사 근거를 보존한 기록이다. 현재 실행 순서와 완료 기준은 [구현 계획](implementation-plan.md)을 따른다.

작성: 2026-09-21. Go 서비스 이식 전 단계.

최종 배포·소비 계약은 [배포 설계 v1](deployment-design.md)을 따른다.
제품별 EC2 1대·Compose, 독립 PostgreSQL/Redis, 인증된 private gRPC를 기준으로 한다.

## 1. 코드베이스 선택

`dylabs/meloming-chat-collector`의 공개 시점 HEAD
`85b8aa1dd9284ded8717c97dc6aee556b5bcf70a`를 일차 기준으로 삼는다.
Go 1.27 workspace이며 discover/coordinator/worker/query/shared/proto가 분리되어 있다.
이 버전의 요구 런타임을 기록한 것이며 최신 Go 버전 선택을 확정한 것은 아니다.

기존 private `meloming-chat-service`와 모델·publisher·SOOP·할당/복구 구현을 비교했다.
공개본은 자체 proto를 포함하여 private proto 의존성을 줄였고 gRPC 인증,
SOOP 접속 host 검증, HTTP 응답 크기 제한이 추가되어 있다. 따라서 private service를
통째로 개명하기보다 공개본의 필요한 파일을 선별 이식하는 쪽이 낫다.

## 2. 남길 기능과 뒤로 미룰 기능

| 첫 연결 경로 | 후속 확장 |
| --- | --- |
| 등록된 SOOP 채널의 방송 인식 | CHZZK/CI.ME 등 추가 플랫폼 |
| SOOP connector + packet 정규화 | 전체 방송 탐색/랭킹 API |
| 워커 소유권·heartbeat·재연결 | 다중 워커 handoff 최적화 |
| 일반 채팅의 제한 길이 Redis stream | 일반 채팅 ClickHouse 아카이브/검색 |
| 후원 durable journal/outbox/replay | 대용량 객체 저장 export |
| 상태 조회·수집 중지·opt-out, 단일 EC2 Compose·IaC | 다중 호스트 확장 |

기존 역할별 Go module 경계를 최대한 유지한다. 처음에는 각 역할 단일 인스턴스로
검증하고, 실행 프로세스를 합치는 새로운 구조 변경은 필수로 삼지 않는다.
원본의 실제 배포/운영 값은 가져오지 않는다. rogichat/rogichat-ops의 IaC·운영 패턴은 선별 활용한다.

## 3. 파일별 첫 이식 후보

아래는 검토할 후보다. 각 행의 관련 helper/import/test를 추적해 개별 파일 목록을
완성한 뒤 반입한다. 폴더 전체를 자동 허용하지 않는다.

| 원본 경로 | 활용 | 필요한 변경 |
| --- | --- | --- |
| worker/internal/connector/soop.go 및 관련 테스트 | frame/parser, handshake, keepalive, timeout | 안정적 event 식별, 종류별 후원, 일반 chat/drop과 분리 |
| worker/internal/connector/connector.go, retry.go | 어댑터 경계와 backoff | 로기챗 module path |
| worker/internal/connector/http_body.go, security_test.go | 응답 제한과 접속 검증 | 필요한 host 설정 명시, 합성 fixture |
| discover/internal/discovery/soop.go 및 관련 테스트 | 목록/live-check | 등록 채널 위주 경로, 설정 없는 전체 수집 금지 |
| worker/internal/manager.go 및 lifecycle 테스트 | 연결 관리·오류 복구 | 후원 저장 우선, publish 실패 재시도 |
| coordinator/internal/leader.go, assigner.go 및 관련 테스트 | lease·할당 | 후원 단일 소유권/fencing 검증 |
| shared/model/message.go, channel.go | 내부 모델 | source ID, donationKind, 원천/관측 시각 구분 |
| shared/store/redis.go, postgres.go | 상태·연결 저장소 패턴 | 신규 최소 스키마에 맞는 메서드만 유지 |
| worker/internal/pipeline/publisher.go | 일반 채팅 최근 stream | Redis namespace 설정, 후원 durable publisher 분리 |
| shared/grpcauth/interceptor.go | 내부 API 인증 | 조회/관리 권한 분리 |
| proto 및 query에서 필요한 RPC | 방송/워커 상태·최근 채팅 | 탐색/랭킹 제거, 자체 package 이름, 후원 replay 추가 |

`cleanup`, `chat-exporter`, 분석용 migration은 기능 도입 시 가져온다.
초기부터 모두 가져오고 실행만 끄는 방식은 사용하지 않는다.

## 4. 일반 채팅과 후원 경로

일반 채팅은 기존 채널별 bounded Redis stream을 재사용할 수 있다.
초기 이름은 `rogi:chat:{platform}:{channelId}`를 제안한다.
외부 제품은 query의 gRPC를 통해 읽으며 서로 다른 cursor를 가진다. Redis 직접 접근은 내부 역할에만 허용한다.
최근 stream은 영구 archive가 아니며 보존 길이·기간을 명시한다.

후원은 다음 흐름을 새로 보완한다.

```text
SOOP packet
  → 정규화/원천 식별
  → durable donation journal + outbox
  → query의 인증된 gRPC journal 구독/replay
  → 각 소비자의 DB inbox + cursor
  → consumer별 RPC ACK
```

journal과 outbox는 같은 DB 트랜잭션으로 저장한다. 재발행에도 eventId를 유지한다.
RPC ACK는 해당 소비자가 durable inbox에 보관한 것을 뜻한다. downstream 제품의
게임 결과나 화면 표시를 뜻하지 않는다.
journal 알림 유실이나 장기 장애 후에는 같은 DB cursor 기반 replay로 복구한다.
소비자별 replay 진행도와 보존 기간을 운영 설정으로 둔다. journal을 먼저 삭제하면
replay가 불가능해지므로 소비 지연과 삭제 정책을 함께 검증한다.

현재 원본의 한계는 다음과 같다.

- SOOP `soop-채널-순번` ID는 새로운 connector에서 중복될 수 있다.
- connector 버퍼가 차면 메시지를 버린다. donation도 같은 경로다.
- Redis 발행 실패 시 manager가 로그를 남기고 다음 메시지를 처리한다.
- handoff의 raw hash/10초 TTL은 원천 거래 ID가 아니며 정상 동일 후원을 합칠 수 있다.
- 최근 stream은 채널별 약 1,000건, firehose 약 100,000건으로 제한되어 있다.
- optional ClickHouse writer도 큐/재시도 한계가 있어 후원 처리 보장의 대체물이 아니다.
- 기존 gRPC query는 방송/최근 메시지 조회와 관리 기능이다. durable 후원 구독/replay는 새 계약이다.

초기 후원 연결은 단일 owner와 fencing을 사용한다. 원천 ID가 있으면 native key로
동일성을 판단한다. 없으면 수집 관측 ID를 고유하게 유지하고 reconnect 사이의 중복
확정 한계를 별도로 기록한다. raw hash 단독으로 정상 연속 후원을 삭제하지 않는다.

DB 저장 전에 죽거나 플랫폼 연결 자체가 없었던 구간은 수집만으로 무손실 보장할 수 없다.
후원 전용 대기열/지속 spool의 최대 크기와 장애 동작을 정하고, 포화 시 gap과 degraded
상태를 표시한다. 무한 메모리 큐나 성공으로 위장한 drop을 사용하지 않는다.

## 5. 외부 이벤트 계약

`schemaVersion`, `eventId`, `type`, `platform`, `platformChannelId`,
`broadcastId`(알 수 없으면 null), `sourceEventId`(없으면 null), `observedAt`,
`occurredAt`(없으면 null), `connectionEpoch`, `sourceSequence`, `donationKind`,
`amount`, `currency`, `donorId`, `donorName`, `message`를 정의한다.

일반 채팅 이벤트에는 플랫폼이 제공한 `userId`를 정규화해 보존하고, 후원의 `donorId`와
같은 식별 규칙을 사용한다. 주루마블이 후원자 채팅의 칸 선택 요청을 검증할 때 필요하다.
닉네임을 ID 대신 사용하거나 플랫폼 userId를 알 수 없는 경우 같은 사람이라고 추정하지 않는다.
채팅 명령어 해석·미션·게임 요청 소유권 검증은 소비 제품에서 처리한다.

`amount`는 `SOOP_BALLOON`의 정수 개수다. 임의 KRW 환산값으로 바꾸지 않는다.
현재 같은 donation으로 정규화되는 일반 별풍선/미션/다른 아이템을 구분해야 한다.
검증되지 않은 종류는 소비 제품이 자동 실행 대상으로 삼지 않도록 enum을 명시한다.
raw에는 내부 프로토콜/개인정보가 있을 수 있으므로 외부 기본 계약에서 제외한다.

JSON schema와 합성 fixture를 이 저장소의 버전 있는 계약으로 관리한다.
주루마블은 고정 버전을 사용하고 Go/TS의 nullable·정수·시간 표현을 서로 검증한다.
주루마블의 `sessionId`, 주사위 횟수, 보드 위치는 collector 이벤트에 넣지 않는다.

## 6. 구현 순서와 검증

1. 파일 허용 목록·고지·module path·최소 설정을 작성한다.
2. SOOP parser/connector 테스트와 합성 데이터로 독립 동작을 검증한다.
3. 등록 채널 live-check, heartbeat, 단일 worker 재연결을 연결한다.
4. 안정적 event ID, 후원 종류, journal/outbox, gRPC 구독/replay를 추가한다.
5. 주루마블 합성 consumer와 연결해 반복 전송/재시작/Redis 중단을 시험한다.
6. 승인된 테스트 채널에서 원천 패킷·방송 전환·미션 중복 의미를 확인한다.
7. 일반 채팅 archive와 다중 worker는 이 경로가 검증된 후 확장한다.

테스트는 기존 SOOP 회귀 사례, malformed/multi packet, host 검증, 연결 timeout,
중복 재발행, 정상 동일 후원 2건, event ID 충돌, 버퍼 포화, DB/Redis 장애,
worker 재시작, channel opt-out, 서비스별 권한을 포함한다.
실제 connector 접속 테스트는 `SOOP_CHAT_HOST_SUFFIXES` 등 설정이 필요하다.
기존 코드 존재와 현재 플랫폼 호환성을 같은 것으로 보지 않는다.

## 7. 현재 완료 범위

원본 clone·구조 비교·이식 후보 정리·후원 신뢰성 설계를 마쳤다.
이 저장소에 Go 구현을 반입하지 않았으므로 collector test/build 성공을 주장하지 않는다.
실방송 연결이나 운영 DB 변경도 수행하지 않았다.
