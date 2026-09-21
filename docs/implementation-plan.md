# 로기챗 공통 수집기 구현 계획 v1

작성: 2026-09-21. 상태: P01 기록과 P02 실행 기반 일부, P03 proto v1 생성·교차 검증 계약이 구현되었다.
세부 통과 여부와 미구현 범위는 [구현 상태](implementation-status.md)를 따른다.
[배포·소비 계약](deployment-design.md), [소스 이식 정책](source-import-policy.md)을 따른다.
원본 파일 후보는 [조사 기록](repository-review.md)에 보존했다.
전체 작업 ID/의존성/출시 기준의 원본은 rogimarble의 `docs/implementation-plan.md`다.
두 문서는 현재 로컬 작성 상태다. 상대 레포 checkout은 런타임/빌드 의존성이 아니다.

## 1. 범위와 순서

SOOP의 등록 채널에서 방송/채팅/후원을 수집하는 공통 제품을 만든다.
주사위·보드·미션·`!이동`·주루마블 sessionId는 소비 제품의 책임이다.
역할별 discover/coordinator/worker/query/shared/proto를 유지하며 EC2 1대에서 각 역할 1개와 PG/Redis를 실행한다.
일반 채팅 bounded stream과 후원 journal/spool을 분리한다.

순서: P01 선별 → P02 기반 → P03 계약 → C01 수집/소유권 → C02 내구성 → C03 RPC/보존 → I02 제품 통합.
운영 O01~O03은 파일/정적 검증을 병행하고 실제 앱 검증 뒤 통과한다. R01은 승인된 채널의 live 관측이다.
주루마블 M 계열과는 향후 P03에서 고정할 계약을 통해 병행하며 상대 레포 내부 DB/Redis를 공유하지 않는다.

## 2. 기반 작업

| ID | 산출물 | 통과 조건 |
| --- | --- | --- |
| P01 | 고정 원본 SHA·개별 파일 원본/대상/helper/test/변경/고지 목록, source-imports 문서 | 전체 복사/이력 연결 없음, LICENSE/NOTICE·허락 근거·공개 ref/비밀 검사 |
| P02 | 역할별 Go module, toolchain/go.sum, 개발 Compose/환경 예시/검증 명령 | clean checkout 독립 빌드·PG/Redis/migration, private proto/sibling 의존 없음 |
| P03 | proto/contracts v1·합성 fixture·Go/TS 생성 계약 묶음 | null/64비트 offset/알 수 없는 종류/호환 필드 round-trip, version/SHA/checksum 고정 |

P01은 공개 meloming-chat-collector의 SOOP/manager/lease/store/auth/query 후보를 개별 선별한다.
private chat-service는 구현 차이 확인 자료로 쓰고 전체 코드/설정을 반입하지 않는다.
라이선스/허락 근거가 없는 직접 이식은 해당 후보를 보류하며 나머지 신규 계약·기반 작업은 진행할 수 있다.
첫 커밋 전에 공개 가능한 파일만 남기고 공개 PR에 올린 뒤 정리하는 절차를 쓰지 않는다.

P03의 필수 필드와 RPC는 배포 설계 §3~4를 따른다. 원천 미제공 시각을 생성 시각으로 대체하지 않는다.
이벤트/fixture에는 native 별풍선 개수·donationKind·sourceEventId?·broadcastId?·connectionEpoch/sequence,
동일 규칙으로 정규화한 donorId/userId·journalGeneration/channelOffset을 포함한다.
identityStatus(source_id/observation_only/reconnect_ambiguous)·qualityReasons·관련 관측 ID도 전달한다.
원천 ID 없음 자체를 재접속 의심과 구분하고 불확실성 근거에는 raw packet/인증정보를 포함하지 않는다.
관리 RPC ResolveConsumerRecovery는 이전/새 generation·기준점·미복구 범위·멱등 키·사유/작업자와 revision을 다룬다.
계약 아티팩트에 생성 도구/버전/checksum을 남긴다. 소비자는 이 묶음의 고정 버전을 사용한다.

## 3. C01 · 등록 채널·연결·소유권

선행: P03. 초기 migration은 consumer/channel subscription·opt-out·owner/lease epoch를 정의한다.

| 모듈 | 구현 | 필요한 검증 |
| --- | --- | --- |
| discover | 명시적 등록 채널 live-check, 시작/종료/재접속 backoff | 등록 없음 0채널, 방송 전환, timeout/응답 크기 제한 |
| coordinator | consumer별 구독, 마지막 구독 해제 후 수집 종료, opt-out 우선 | consumer A 해제에도 B 구독 유지, opt-out 이후 재시작 금지 |
| coordinator/worker | lease 갱신·fencing token, stale owner 중단 | lease 만료/두 worker 경쟁/Redis 단절 중 옛 owner 쓰기 거부 |
| worker connector | SOOP frame/멀티 packet/parser/handshake/keepalive/reconnect | 합성 malformed packet·host 검증·timeout·종료 자원 정리 |
| shared/pipeline | 이벤트 분류·채팅 bounded stream·후원 sink 인터페이스 | 일반 채팅 포화가 후원 성공으로 기록되지 않음 |

fencing은 owner 화면에 token을 표시하는 데 그치지 않고 후원 영속 수락 경계에서 검증한다.
실제 검증은 C02와 함께 수행한다. 등록 미설정 시 전체 플랫폼 탐색으로 확대하지 않는다.
연결 프로세스 health와 마지막 수신/owner/재연결/gap을 별도로 관측한다.

통과: 실제 역할 프로세스+합성 SOOP 전송으로 등록/구독/수집/중지/재시작 검증.
이 시점의 후원 sink가 아직 durable하지 않으면 실방송 사용을 금지하고 구현 상태를 명시한다.

## 4. C02 · journal·outbox·spool

선행: C01. shared/store migration과 worker 후원 수락 경로를 구현한다.

- 검증한 원천 ID+종류의 중복 규칙, 없을 때 관측 eventId를 생성한다. displayName/raw hash를 거래 ID로 사용하지 않는다.
- 같은 내용의 정상 후원 두 건을 보존한다. reconnect 의미적 중복의 불확실성은 이벤트별 identityStatus/qualityReasons로
  journal과 spool에 함께 저장해 replay에도 유지한다. 소비자는 이 근거를 받아 게임 자동 적용을 검토 대상으로 보류한다.
- 채널 counter 행 잠금 아래 offset과 journal/outbox를 같은 PG 트랜잭션으로 커밋한다.
- 단순 sequence 선할당 때문에 낮은 offset의 늦은 commit이 skip되지 않게 동시 writer 테스트를 둔다.
- DB 장애 시 bounded disk spool에 내구 저장하고 재시도한다. fsync/원자 기록·손상 레코드 처리·파일/용량 제한을 명시한다.
- spool 최초 수락 ID를 journal로 옮길 때 유지한다. journal commit 뒤 spool 삭제 전 crash는 unique로 복구한다.
- owner fencing과 spool drain 경계를 정의한다. 예전 owner의 신규 관측은 차단하고 이미 적법하게 수락한 spool은 출처/epoch를 검증해 복구한다.
- outbox Redis 알림 재시도, spool 포화/손상·DB/디스크/플랫폼 단절의 degraded/gap과 지표를 구현한다.

통과: PG commit 전/후, spool fsync/이관/삭제 전후 강제 종료에 ID/수량/원장 일치.
일반 채팅 폭주·DB/Redis 중단·디스크 full 상황에서도 drop을 성공으로 위장하지 않는다.
영속 저장 전 호스트/디스크 손실이나 플랫폼 미수신 구간은 복구 보장으로 표시하지 않는다.

## 5. C03 · 인증 query·재생·보존

선행: C02. query와 consumer ACK/보존 migration을 연결한다.

- mTLS SAN/CA 검증, consumer identity→channel/read/manage scope, 인증서 회전/만료 관측을 구현한다.
- GetCollectionStatus, SetChannelSubscription, ListDonations, WatchDonations, AckDonations, WatchChat을 제공한다.
- List/Watch는 같은 journal cursor를 쓴다. replay/live 전환 사이 레코드를 놓치지 않고 Redis 알림이 없어도 주기 조회한다.
- ACK는 해당 consumer/channel/generation의 연속 영속 수락 cursor다. 소비자는 수락 직렬화 또는 연속 watermark로
  미수락 구간을 건너뛰지 않아야 한다. collector는 단조성·전달 범위·권한·recovery revision을 검사한다.
  consumer의 DB commit 자체를 원격 검증한다고 주장하지 않는다. 다른 consumer cursor는 갱신하지 않는다.
- 초기 소비 cursor 선택은 명시적으로 기록한다. 기존 cursor는 자동으로 최신으로 점프하지 않는다.
- ResolveConsumerRecovery는 관리 scope로만 재개 기준점을 변경한다. 복구 필요→대조→운영자 확정→재개의
  이전/새 generation·기준점·미복구 범위·사유/작업자·멱등 키/expectedRevision을 저장한다.
  기준점 변경은 실제 수락 ACK로 처리하지 않는다. 같은 키 재시도는 같은 결과, 다른 본문은 거부한다.
  옛 stream/recovery revision의 ACK를 차단하고 새 기준점 뒤 실제 연속 수락부터 ACK를 진행한다.
- CURSOR_EXPIRED와 earliest/current, 복원 generation 불일치, 일반 채팅 trim gap을 계약대로 반환한다.
- 최소 후원 보존+활성 ACK와 최대 기간/용량을 함께 적용한다. 느린 consumer의 무제한 보관과 조용한 삭제를 막는다.
- 초기 후원 30일, 채팅 24시간 및 채널 10,000건, spool 1GiB를 운영 입력으로 두고 보존 축소 영향을 표시한다.
- query/worker 진행 heartbeat, journal/outbox/ACK/spool 지연·용량을 status/metrics로 제공한다.

통과: 두 consumer의 구독/읽기/관리 격리, ACK 유실 재전송, List/Watch 전환/동시 commit,
Redis 알림 유실·느린 consumer·cursor 만료·인증서 교체·복원 세대 변경을 자동 검증한다.
ACK를 게임 완료/OBS 표시 성공으로 해석하지 않는다. DB/Redis는 외부 제품에 직접 열지 않는다.

## 6. I02 · 주루마블과 통합

선행: C03, marble I01/M07/M04. 두 독립 Compose와 mTLS로 실제 query→marble worker를 연결한다.
실제 서비스의 입력만 합성 adapter로 제공하고 mock RPC 통과로 통합 완료를 표시하지 않는다.

| 장애/시나리오 | 확인 |
| --- | --- |
| marble 종료 중 후원 저장 | journal 유지·재시작 후 보존 범위 재생 |
| collector 종료/재기동 | marble 수동 운영 유지, 수집 상태 단절 표시 |
| inbox commit 뒤 ACK 전 crash | 재전송되지만 같은 게임 효과 한 번 |
| 낮은 offset 수락 실패·높은 offset 선완료·겹친 stream | 소비 cursor/ACK가 빈 구간을 넘지 않음 |
| 재접속 불확실 관측 | 근거 전달·영속 수락/ACK 후 게임 보류, 운영 승인/무동작 마감 전 미실행 |
| 같은 native 내용 두 후원 | 서로 다른 정상 후원 두 행과 두 요청 |
| 후원/채팅 역순·채팅 gap | donorId 정규화 일치; 선택 판정은 marble 책임 |
| cursor 만료/세대 불일치 | 자동 점프/재적용 없음; 웹 범위 대조·기준점 확정·새 구간 정상 수신까지 |

journal eventId→inbox→action/result의 추적 증거를 양쪽에서 남긴다.
복구 결정은 marble의 영속 요청→collector 관리 RPC 반영→marble 기준점 반영으로 추적한다.
응답 유실은 같은 키로 조회/재시도하고 양쪽 적용 확인 전 수신을 멈춘다. 기존 inbox/결과는 삭제하지 않는다.
미복구 범위 포기는 감사 기록을 남기는 명시적 결정이며 개별 후원 검토나 수락 ACK와 섞지 않는다.
공통 계약 버전의 호환 추가는 collector 제공→marble 갱신 순으로 배포하고 breaking change는 새 major로 분리한다.

## 7. O01~O03 · 단일 EC2 운영

| ID / 선행 | collector 작업 | 완료 증거 |
| --- | --- | --- |
| O01 / P03, 실제 앱 완성 | 역할별 Dockerfile/digest, 운영 Compose·one-shot migration, CPU/RAM/PID/로그 제한 | compose config·실제 이미지 smoke·manifest/digest/migration checksum 검증 |
| O02 / O01 | collector EC2/EBS/SG/IAM root, A SG 입력·private 7443·mTLS, systemd/mount/secret 순서, host lock 배포 | EC2 1대·cold boot·mount 부재 차단·동시 배포·migration 실패·호환 rollback |
| O03 / O02,C03,I02 | 보존/관측 통합, pgBackRest+WAL, spool/manifest/복구 메타데이터, generation 재조정 runbook | 단독/시차 DB 복구·cursor/ID 대조·RPO/RTO 실측 |

network는 marble의 별도 root가 소유한다. 이 레포는 같은 VPC 자원을 중복 선언하지 않는다.
DB/Redis host port는 미공개, public SSH 없음, 운영은 Tailscale/OpenSSH와 SSM 복구를 사용한다.
데이터 EBS·tmpfs secret·migration 완료 전 서비스 시작을 막는다. 역할별 systemd unit이 담당 프로세스의
종료를 감지/재시작하고 Compose restart=no를 쓴다. detached Compose 성공을 지속 감독으로 취급하지 않는다.
각 장기 실행 프로세스 강제 종료 후 mount/secret 순서와 작업 진행을 복구하는지 검사한다.
EC2 교체와 data EBS lifecycle을 분리하고 삭제/교체 보호 및 명시적 유지보수 해제 절차를 둔다.
호스트 교체 후 기존 EBS의 DB/spool을 보존하며 data EBS 삭제/교체 plan을 차단하는 시험을 수행한다.
역할별 secret/DB 계정 mount 표를 만들고 init/migration 자격증명은 해당 one-shot에만 제공한다.
Docker forwarding에서 앱의 IMDS 접근을 차단하고 실제 앱 안에서 metadata·타 역할 secret·migration 계정 접근 실패를 검증한다.
`tools/ops/deploy.sh --manifest ...` 하나가 검증/lock/readiness/migration/기동/smoke를 수행한다.
DB down migration·down -v·원본 운영 설정/키 복사는 하지 않는다.
실제 backend/plan/secret/manifest는 Git 밖, public CI에는 cloud/SSH 권한이 없다.

복원 후 journalGeneration 변경을 통지하며 기존 eventId는 유지한다. consumer와 spool 대조 전 수집/전달을
자동 재개하지 않는다. 월간 복구 목표 RPO 5분/RTO 2시간은 실측 전 보증이 아니다.
기존 rogichat 리소스/ACL 변경이나 상시 세 번째 EC2는 이 계획의 범위가 아니다.

## 8. R01 · 실제 플랫폼 확인과 출시

선행: 전체 계약/내구성/제품 통합/운영 검증. 승인된 테스트 채널에서 SOOP 종류·원천 ID·방송 전환·재접속을 관측한다.
실제 금전 후원을 자동 발생시키지 않는다. 민감 정보 없는 fixture만 테스트에 반영한다.
실제 관측이 안 된 프로토콜/후원 종류를 지원 완료로 표시하지 않는다.
최종 source/digest/계약·환경·검증 증거·남은 제한·rollback 경로를 기록한다.

검증 명령은 P02에서 root entrypoint로 제공한다. 역할별 unit, race 검사, 실제 PG/Redis 통합,
합성 SOOP 연결·gRPC contract, Compose smoke를 구분한다. 외부 live 테스트는 명시적 별도 명령이다.
todo→doing→review→verified를 적용하고 관련 증거가 있어야 verified로 올린다.
