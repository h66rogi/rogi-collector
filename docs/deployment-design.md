# rogimarble 방송 입력 수집기 · 배포와 소비 계약 v1

수정: 2026-09-21. **제품 경계와 목표 계약을 정의한다. 쿠키 인증·후원 내구성·collector v1 RPC와 정식 배포를 구현했으며, 실증 결과와 남은 게임 연동·복구 검증은 [구현 상태](implementation-status.md)를 따른다.**
전체 제품 설계는 [rogimarble 저장소](https://github.com/h66rogi/rogimarble)의
`docs/final-design.md`에서 관리한다.
이 문서는 collector가 독립적으로 구현할 경계를 정한다. 실행 작업은 [구현 계획](implementation-plan.md),
초기 조사 근거는 [조사 기록](repository-review.md)에 분리했다.

## 첫 출시 대상 · 사용자 확정 변경 (2026-09-21)

이 도구는 rogimarble의 후원 기반 게임 진행과 후원자 채팅 선택에 필요한 입력을 제공한다.
chat-collector는 기본 코드베이스이며, 원본의 범용 기능 목록이 이 제품의 구현 목표는 아니다.
첫 제품은 후로기(`h66rogi`) 한 채널의 주루마블 방송을 위한 수집기다.
연령제한 방송이므로 주입한 로그인 쿠키를 사용하는 방송 감지·채팅 접속이 필수다.
후로기 방의 전체 채팅과 필요한 별풍선 후원을 수집하며 첫 소비자는 주루마블 하나다.

기존 구조와 아래 후원 전달 의미는 유지하되, 다채널 승인/등록 및 여러 소비자 관리의 일반화는
첫 출시 선행 조건에서 제외한다. 한 채널·한 소비자의 설정으로 시작한다.
실행 순서는 [제품 구현 계획 v3](implementation-plan.md)를 따른다. 쿠키를 이용한 실제 접속 확인을
초기에 수행하고 후원 전달·저장·방송 운영 검증을 이어간다. 이 범위 변경이 아래 일반적인 확장 설명보다 우선한다.

쿠키 입력은 `SOOP_COOKIE_FILE`의 JSON snapshot으로 주입한다.
[쿠키 획득 컴포넌트](../cookie-auth/README.md)가 `SOOP_ID`/`SOOP_PW`로 로그인하고 24시간마다 또는 갱신 요청 시 저장한다. discover와 worker에 필요하며
쿠키 값은 코드/로그/계약/브라우저에 넣지 않는다. 인증 실패와 방송 종료를 구분하고
교체 후 재접속한다. 실제 SOOP 인증 흐름과 다른 연령제한 방송 입장을 EC2에서 확인했다. 후로기 본인 방송의 수신은 방송이 켜질 때 확인해야 한다.

## 1. 단일 EC2 구성

제품별 EC2 1대라는 요구에 따라 collector 한 호스트에서 Docker Compose로 다음을 실행한다.
주루마블은 별도 EC2이며 이 호스트의 DB·Redis를 직접 사용하지 않는다.

| 서비스 | 역할 |
| --- | --- |
| discover | 명시적으로 등록한 채널의 live-check |
| coordinator | 채널 소유권·할당·lease/fencing |
| worker | SOOP 연결·정규화·후원 영속 수락·outbox/spool |
| query | 인증된 내부 gRPC 상태·수집 관리·후원 replay·최근 채팅 |
| cookie-auth | SOOP 브라우저 로그인·일일/요청 시 쿠키 갱신; 동일 EC2의 보조 컴포넌트 |
| PostgreSQL | 등록/소유권에 필요한 영속 상태·후원 journal/outbox·consumer ACK |
| Redis | coordination·최근 채팅·journal 갱신 알림 |
| 일회성 profile | 초기화/migration·backup/restore |

Go 역할별 모듈 경계와 프로세스를 유지한다. 초기 각 역할 replica는 1개다.
ClickHouse·전체 채널 탐색·Kubernetes·외부 Aurora·추가 관리 EC2는 필요하지 않다.
정식 배포는 S3 백업과 Secrets Manager를 사용한다. 상시 앱 서버 수는 늘리지 않는다.

## 2. 네트워크와 접근

- 주루마블과 같은 신규 VPC에 놓고 query를 private TCP 7443으로만 제공한다.
- 서버 인증서 SAN과 private hostname을 검증하고 mTLS consumer 신원을 채널 권한에 연결한다.
- 관리 RPC와 읽기 RPC의 scope를 구분한다. 인증서/CA와 회전 정보는 private 운영 입력이다.
- SG는 승인된 소비자 SG만 허용한다. PostgreSQL/Redis host port는 publish하지 않는다.
- SOOP outbound를 위해 초기 public subnet/공인 주소를 사용하되 인터넷 inbound를 열지 않는다.
- 현재 배포·확인·복구는 SSM을 사용한다. Tailscale 위 OpenSSH는 초기 관리 접근 목표이며 이 배포의 검증 완료 항목이 아니다. public SSH를 열지 않는다.

`SetChannelSubscription`은 현재 허용된 한 소비자·한 채널의 수집 시작/중지에 사용한다.
여러 소비자를 지원할 때 다른 활성 구독이 있으면 물리 수집을 계속하는 것은 향후 확장 계약이다.
플랫폼 opt-out은 구독보다 우선한다.
수집 설정이 없을 때 전체 플랫폼으로 확장하지 않는다. 첫 제품의 채널 승인은 주루마블 관리자가 담당한다.

## 3. 후원 RPC와 전달 의미

제품 RPC: `GetCollectionStatus`, `SetChannelSubscription`, `ListDonations(afterCursor, limit)`,
`WatchDonations(afterCursor)`, `AckDonations(cursor)`, `WatchChat(afterCursor)`.
구현 계획 리뷰에서 복구를 위한 관리 RPC `ResolveConsumerRecovery`를 추가했다.
원본 ChatQuery/ChatAdmin 계약과 별개이며 현재 query runtime은 collector v1만 등록한다.

```text
SOOP → 종류/원천 식별 → PostgreSQL journal + outbox
    → query의 TLS gRPC 목록/구독 → 소비자의 DB inbox + cursor → ACK
```

Redis는 빠른 알림 수단이다. query는 journal을 읽고 주기 조회도 수행하여 알림 누락을 복구한다.
List/Watch가 동일한 DB cursor 의미를 사용해 replay에서 live로 전환하는 틈이 없게 한다.
ACK는 소비자 DB가 수락한 상태이며 게임 결과나 OBS 표시 완료가 아니다.
동일 eventId를 여러 번 전송할 수 있고 소비자는 unique inbox로 중복 효과를 막는다.
소비자는 consumer/channel/generation별 수락을 직렬화하거나 연속 watermark를 사용해 미수락 offset을
넘지 않아야 한다. ACK는 그 연속 구간만 전진한다. 옛 stream/recovery revision의 ACK는 거부한다.

후원 event에는 안정적인 eventId와 `journalGeneration + channelOffset` cursor가 있다.
채널별 counter 잠금과 journal 커밋을 같은 트랜잭션으로 묶어 낮은 offset의 늦은 커밋을 건너뛰지 않는다.
64비트 offset은 JSON에서 문자열로 유지한다. 보존 기간 밖 cursor는 `CURSOR_EXPIRED`로 반환하며
earliest/current 범위를 알린다. 자동으로 최신으로 건너뛰지 않는다.
DB 복원 시 journalGeneration 변경을 감지·통지하되 기존 eventId는 바꾸지 않는다.
만료/세대 변경 후에는 복구 필요→범위 대조→운영자 재개점 확정→재개를 따른다.
ResolveConsumerRecovery는 관리 scope에서 이전/새 세대·기준점·미복구 범위·사유/작업자·멱등 키·revision을 저장한다.
기준점 변경과 실제 inbox 수락 ACK는 구분한다. 소비 제품의 로컬 반영까지 끝나기 전 수신을 재개하지 않는다.
응답 유실은 같은 키로 재시도하고 기존 inbox/결과는 보존한다. 상세 소비자·복원 절차는 [코드 인계](collector-code-handoff.md#소비자-동작과-보존)를 따른다.

영속 journal/spool에 수락할 때 생성한 ID는 journal 이동·재발행에도 유지한다.
원천 ID가 있으면 별도 보존하고 donationKind와 함께 검증한 중복 식별 규칙을 적용한다.
없다면 연결 epoch/순번으로 관측 충돌을 막되 재연결 사이의 같은 실제 후원을 완벽히 구분한다고 주장하지 않는다.
raw hash/짧은 TTL만으로 정상 동일 개수 후원을 합치지 않는다.
identityStatus(source_id/observation_only/reconnect_ambiguous)·qualityReasons·관련 관측 ID로 이벤트별 근거를 전달하고
journal/spool/replay에도 보존한다. 원천 ID 없음만으로 모든 정상 후원을 의심 처리하지 않는다.
소비 제품은 재접속 불확실 관측을 수락/ACK하되 게임 적용은 검토 대상으로 보류한다. collector가 게임 결정을 하지 않는다.

후원 경로에는 drop-on-full을 사용하지 않는다. DB 장애 시 bounded disk spool·재시도로 넘기고,
용량 포화·손상·플랫폼 단절을 degraded/gap으로 표시한다. DB 커밋 뒤 spool 삭제 전에 재시작해도
같은 eventId가 journal unique에 걸리게 한다. 미수신 후원과 영속 저장 전 디스크까지 잃은 후원은
복구를 보장할 수 없음을 소비자에게 알린다.

## 4. 채팅·공통 이벤트

일반 채팅은 기존 bounded Redis stream을 유지하고 query가 중계한다. 영구 archive가 아니며
trim 뒤 gap을 알린다. userId는 후원의 donorId와 같은 정규화 규칙을 사용한다.
현재 주루마블 소비자가 자신의 cursor로 읽는다. 여러 소비자의 독립 구독은 향후 확장 범위다.

`!이동` 해석·요청 소유권·게임 설정·Lottie 연출은 collector에 넣지 않는다.
채팅과 후원의 전달 순서가 다를 수 있으므로 주루마블이 명령 후보를 잠깐 저장·재검사한다.
채팅 유실 시 운영자 수동 처리/후원자 재입력을 제공하는 것은 주루마블의 책임이다.

계약은 이 레포의 versioned proto/schema/합성 fixture로 배포한다. nullable 시각·원천 ID,
native 별풍선 정수 개수와 donationKind를 명시한다. raw packet·인증정보는 기본 계약에서 제외한다.
코드 생성 산출물의 원본 버전/checksum을 소비자가 고정하고 private proto를 참조하지 않는다.

## 5. Compose·IaC

정식 실행 파일은 `deploy/compose.production.yaml`, `deploy/systemd/`,
`tools/ops/deploy.sh`, `tools/ops/status.sh`다. [운영 배포](deployment-production.md)와 [CI/CD](deployment-ci.md)를 따른다.
`deploy/compose.yaml`은 초기 health-only 골격이며 현재 앱용 로컬 실행 절차로 사용하지 않는다.

배포 helper 하나가 manifest/digest 검증 → host lock → 이미지 준비 → DB/Redis readiness →
일회성 marker/app migration → 7개 서비스 시작 → unit/container health 검증을 수행한다.
소비자 호스트의 인증된 gRPC와 실제 방송 수신은 별도 인수 검사다. 배포 helper의 성공만으로 이를 대체하지 않는다.
동작 중인 migration을 새 배포가 취소하지 않게 한다. 롤백은 호환되는 이전 이미지로 수행하며
DB down migration과 `down -v`는 자동 실행하지 않는다.

운영은 systemd가 Compose 프로세스를 감독하고 Compose restart 정책은 `"no"`로 둔다.
저장소/앱 역할별 unit이 담당 컨테이너의 종료를 감지하고 재시작한다. detached Compose 실행 성공만으로
감독이 완료됐다고 보지 않는다. 각 장기 실행 프로세스 강제 종료 후 실제 작업 재개를 검증한다.
data EBS 마운트·Docker/network·tmpfs secrets·저장소·앱 순서를 보장한다.
data EBS가 빠졌을 때 루트 디스크에 빈 DB가 생성되지 않게 한다.
DB/Redis·spool은 암호화 EBS에 보관하고 로그·CPU/RAM·PID를 제한한다.
프로세스 health와 실제 작업 진행 heartbeat/backlog를 함께 확인한다.
EC2 교체와 data EBS lifecycle을 분리하고 데이터 볼륨의 삭제/교체를 IaC와 배포 도구에서 차단한다.
보호 해제는 백업/복구 확인과 영향 기록이 있는 명시적 유지보수 절차다. 호스트 교체 후 기존 EBS 재연결로
DB/spool이 보존되는지 검증한다.

역할별 secret 파일과 DB 계정의 mount 표를 두고 각 앱에는 필요한 파일만 제공한다.
초기화/migration 자격증명은 해당 일회성 작업에만 제공하며 공용 secret 디렉터리를 앱에 통째로 mount하지 않는다.
Docker forwarding 경계에서 앱의 IMDS 접근을 차단한다. IMDSv2 설정만으로 앱 격리를 대신하지 않는다.
실제 앱 컨테이너에서 metadata·타 역할 secret·migration 계정 접근이 실패하는지 검증한다.

Terraform은 이 레포의 EC2 root가 collector EC2/EBS/SG/instance role/backup·secret 자원을 소유한다.
공유 VPC/subnet/route는 rogimarble의 별도 network root 소유이며 필요한 ID만 입력한다.
state를 `collector/prod` 경계로 분리하고 다른 root의 자원을 중복 선언하지 않는다.
remote state/plan/backend 실제 설정·운영 주소·접근키는 Git 밖에 둔다.

rogichat의 EC2/SSM/IMDSv2·암호화·비밀 없는 bootstrap·systemd·이미지 digest·lock 패턴을 활용한다.
Aurora/과거 Lightsail/관리 EC2와 실제 값을 복사하지 않는다. rogichat-ops는 private 운영 입력과
공개 소스 분리, 소스 SHA·배포 증거 검증의 참고다. 새로운 IaC 사본을 private ops에 중복 관리하지 않는다.
공개 PR CI에는 cloud/SSH 권한을 주지 않고, plan/apply/deploy는 기존 관리 장비나 승인된 운영 환경에서 실행한다.

## 6. 보존·복구와 완료 기준

현재 코드의 보존 한도는 후원 payload 최소 7일+ACK, 최대 30일/약 1GiB,
일반 채팅 최대 24시간·채널별 10,000건, spool 1GiB/1만 파일이다.
상세 tombstone·감사 보존은 [코드 인계](collector-code-handoff.md#소비자-동작과-보존)를 따른다.
이 값들은 코드의 정책이며 환경변수로 조정하는 기능은 없다. 강제 prefix 만료는 cursor 오류와 earliest 위치로 드러난다.
운영 알림 연결은 별도 미완료 작업이다. 지연 소비자 때문에 무제한 저장하지 않는다.

현재는 일일 `pg_dump --format=custom`을 gzip으로 보관하고 S3로 업로드한다. 로컬 백업은 7일 초과분을 정리한다.
초기 목표였던 pgBackRest base backup + WAL archive와 RPO 5분·RTO 2시간은 구현·복구 실측 전이다.
일일 dump로 그 목표를 충족했다고 표현하지 않는다. 복원 후 worker 시작 전에
consumer cursor·generation·원천 ID·spool을 대조하고 gap/중복 재수락 위험을 확인한다.
일반 채팅 최근 stream은 재생 범위 밖 유실을 허용한다. 후원은 그 정책을 따르지 않는다.

아래는 제품 인수 기준이다. 항목별 실제 완료 여부는 [검증 상태](implementation-status.md)를 따른다.

- clean checkout의 검증된 이미지와 운영 입력으로 정식 Compose의 실제 Go 서비스·DB·Redis·cookie-auth가 시작된다.
- 등록 채널만 수집하고 consumer/채널 간 읽기·관리 권한이 격리된다.
- 재전송·ACK 유실·DB/Redis 중단·worker 재시작에도 영속 수락한 eventId가 유지된다.
- 소비자의 낮은 offset 수락 실패·높은 offset 선처리 시도·겹친 stream에서 ACK가 미수락 구간을 넘지 않는다.
- 같은 내용의 정상 후원 2건을 합치지 않고 미확인 후원 종류를 구분한다.
- 재접속 불확실 관측의 근거를 spool/journal/replay에 보존하고 소비 제품의 검토 대기까지 전달한다.
- 버퍼/spool 포화·cursor 만료·플랫폼 단절·복원 세대 변경이 드러난다.
- cursor 만료/세대 변경 후 운영자가 재개점을 확정하고 정상 수신을 재개한다. RPC 응답 유실 재시도에도
  기준점 변경은 수락 ACK와 구분되며 옛 stream이 cursor를 되돌리지 않는다.
- 주루마블이 장시간 끊겨도 보존 범위 내 journal을 재생할 수 있다.
- 콜드 부팅·migration 실패·동시 배포·백업 복원을 실제 검증한다.
- EC2 교체 시 data EBS 보존, 앱의 자격증명 접근 격리, 역할별 프로세스 강제 종료 후 복구를 실제 검증한다.
- 실제 SOOP 원천 ID·종류·방송 전환 의미를 테스트 채널 관측으로 확인한다.
