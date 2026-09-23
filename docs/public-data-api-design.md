# data-api.rogi.chat 설계 초안

상태: 설계. 코드, DNS, Tunnel, 공개 API, 영구 채팅 보관은 아직 배포하지 않았다.
대상: SOOP `h66rogi` 한 채널. 구현 언어는 Go다.

## 목표와 이미 확정된 조건

- 후로기 방송의 `live`/`offline`/조회 불가 상태와 수집 연결 상태를 공개한다.
- 방송 중 채팅을 WebSocket으로 제공하고, 지난 방송의 채팅을 페이지 단위로 조회한다.
- API는 기본적으로 누구나 읽을 수 있다. 나중에 API key를 요구할 수 있는 인증 경계를 둔다.
- 채팅 기록에는 시간 기준 만료를 두지 않는다. 로컬 접근 로그는 용량을 제한해 보관한다.
- Go 서비스는 `rogi-collector-prod`의 기존 Compose 스택에 추가한다. 인터넷 경로는
  `data-api.rogi.chat` → Cloudflare Tunnel → 내부 HTTP/WebSocket 서비스다.
- 명세 사이트는 별도 공개 `h66rogi/rogi-docs` 저장소의 Docusaurus 정적 빌드를
  Cloudflare Workers Static Assets에 배포해 `docs.rogi.chat`으로 제공한다.

## 현재 경계와 차이

현재 query의 collector v1은 private mTLS gRPC 7443만 제공한다. `CheckBroadcast`는
`live`, `offline`, `cookie_required`, `auth_required`, `lookup_failed`를 구분한다.
`GetCollectionStatus.collection_active`는 worker의 연결 상태이며 방송 on/off가 아니다.
`WatchChat`은 Redis Stream을 읽고 generation·stream ID cursor 및 `gap_before`를 전달한다.
Redis 채팅은 최대 24시간·채널당 10,000건이며 영구 기록이 아니다.
원본 `chat-exporter`와 ClickHouse는 현재 운영 Compose에 없다. PostgreSQL의
`broadcast_sessions` 스키마만으로는 채팅 내역을 반환할 수 없다. 운영 profile은
원본 광역 ChatQuery/ChatAdmin 서버를 등록하지 않는다.

## 배치와 데이터 흐름

```text
SOOP → discover / worker ┬→ Redis 최근 채팅 → query의 내부 WatchChat
                         └→ 내구성 있는 채팅 보관 → PostgreSQL 색인 + Cloudflare R2 채팅 archive

Cloudflare 방문자 → data-api.rogi.chat → Tunnel → data-api (Go, Compose)
                                              ├→ 내부 query 읽기 계약
                                              └→ 보관 기록 읽기 계약
Cloudflare 방문자 → docs.rogi.chat → Workers Static Assets (Docusaurus build)
```

`cloudflared`는 collector 호스트에서 outbound 연결을 만들고 `data-api`의 Compose 내부
주소로 프록시한다. `data-api` 포트를 EC2 보안 그룹이나 공인 host port에 열지 않는다.
PostgreSQL·Redis와 기존 7443도 공개하지 않는다. 기존 주루마블 mTLS 호출과 관리 RPC는
유지한다. 공개 API가 필요한 읽기 기능은 collector 내부의 별도 read-only 계약으로
제공하며, 브라우저에는 mTLS 인증서·SOOP cookie·원본 패킷을 전달하지 않는다.

기존 `production-status.py`는 정확히 7개 컨테이너만 있는지 검사한다. Compose에
서비스를 추가할 때 release manifest, systemd 감독, 상태 검사, 백업·복원 절차도
함께 수정해야 한다. Tunnel token은 Git이나 이미지에 넣지 않고 운영 secret으로 주입한다.
`data-api.rogi.chat`의 Tunnel DNS 레코드는 기존 `rogi.chat` Cloudflare DNS 소유
Terraform root와 충돌 없이 관리한다. Tunnel과 DNS의 소유 root를 구현 전에 확정한다.

## 공개 HTTP/WebSocket v1 계약

| 경로 | 의미 |
| --- | --- |
| `GET /v1/broadcasts/current` | 방송 상태, 확인 시각, 현재 방송 식별자, collector 연결 상태와 마지막 수신 시각 |
| `GET /v1/broadcasts?cursor=&limit=` | 저장된 방송 목록과 다음 페이지 cursor |
| `GET /v1/broadcasts/{sessionId}/chats?cursor=&limit=` | 해당 방송의 보관 채팅과 다음 페이지 cursor |
| `GET /v1/chats/recent?cursor=&limit=` | 영구 archive가 준비되기 전에도 의미가 분명한 최근 채팅 조회. 보존 범위를 응답에 명시 |
| `GET /healthz`, `GET /ready` | 프로세스 생존과 필수 읽기 의존성 준비 상태 |
| `WS /v1/chat/stream` | `broadcast.status`, `chat.message`, `chat.gap`, `heartbeat`, `error` 이벤트 |

`GET /v1/broadcasts/current`의 `live`는 `true`, `false`, `null` 중 하나다.
`cookie_required`, `auth_required`, `lookup_failed`일 때는 `null`과 원래 상태를
반환한다. collector가 `waiting`이더라도 마지막 확인이 오래됐으면 신선도를 표시한다.
공개 요청마다 SOOP player API를 호출하지 않고 내부 조회 결과를 짧게 캐시하며
중복 조회를 합친다. 현재 진단 조회는 전체 초당 1회, 채널별 10초 캐시다.

WebSocket은 표준 RFC 6455와 JSON 메시지로 시작한다. 첫 메시지에 API 버전,
방송 상태, 서버 시각, cursor 보존 범위를 제공한다. 재접속 cursor는 불투명 문자열로
다루고 Redis generation 변경·trim으로 이어 받을 수 없을 때 `chat.gap`을 명시한다.
역사 조회 경로로 이동할 수 있는 방송 ID를 함께 반환한다. 한 공개 클라이언트마다
무제한 내부 gRPC polling을 만들지 않도록 상류 구독을 공유하거나 연결 수를 제한한다.
초기 동시 연결 한도는 설정값으로 두고 실제 관측 후 조정한다.

채팅 응답은 안정적인 event ID, 방송 session ID, 수신 시각, 메시지, 표시 이름,
공개 가능한 사용자 식별자, 순서·품질 정보를 포함한다. SOOP 원본 패킷, 로그인 정보,
후원·운영 전용 필드는 v1에서 공개하지 않는다. HTTP 목록에는 limit 상한과
서버가 만든 불투명 cursor를 사용한다. 정렬 기준은 방송 내 수집 순서로 고정한다.

## 시간 만료 없는 채팅 보관

40 GiB 로컬 데이터 볼륨에 채팅을 무기한 쌓는 것은 불가능하다. 보관 정책은
**시간 만료 없음**으로 정의하고, 오래된 본문은 별도 비공개 Cloudflare R2 archive bucket에 계속 보존한다.
현재의 PostgreSQL dump 백업 bucket과 archive bucket은 역할을 분리한다.

1. worker의 일반 채팅 경로에서 안정적인 event ID와 수집 시각을 확정한다.
   보관 writer는 내구성 있는 로컬 spool/DB 수락 지점을 둔다. Redis 구독을 나중에
   읽는 방식만으로는 24시간 trim·worker 중단 구간의 유실을 막을 수 없다.
2. 채팅을 내부 방송 session ID에 연결한다. SOOP broadcast ID는 원천 메타데이터로
   별도 보관한다. 방송 시작·종료, 재연결, 중복 관측, session 판정 불확실성을 기록한다.
   현재 history emitter는 선택 기능이므로 운영 profile의 실제 활성 여부와
   방송 전환 의미를 구현 시 확인한다.
3. 최근 기록과 방송·archive segment 색인은 PostgreSQL에서 조회한다.
   확정된 segment는 압축된 불변 객체로 R2의 S3 호환 API에 업로드하고 checksum·건수·첫/마지막
   position을 검증한 뒤 색인을 커밋한다. 온라인 DB 본문을 정리하더라도 R2와
   색인에서 과거 페이지 조회가 계속 가능해야 한다. R2에 만료 lifecycle은 두지 않는다.
4. 저장 장애·spool 포화·방송 연결 단절에는 기록 완전성을 보장한 척하지 않고
   session별 `complete`/`gaps`를 기록한다. Redis 실시간 전달과 영구 archive의
   수락 지점을 구분한다. 기존 Redis cursor보다 오래된 기록은 archive 조회로 복구한다.
5. archive 업로드·색인·복원 시험, 로컬 spool 사용량, R2 비용·증가율을 감시한다.
   "무제한"은 데이터를 자동 삭제하지 않는 정책이지 유한한 디스크의 무한 용량이 아니다.

과거에 Redis에서 이미 사라진 채팅은 소급 복원할 수 없다. archive 시작 시각과
각 방송의 보관 완전성을 공개 응답과 문서에 명시한다.

## 공개 접근과 운영

- v1은 익명 읽기다. HTTP와 WebSocket 모두 같은 인증·속도 제한 middleware를
  통과하게 하고, 나중에 API key 해시 검증·발급·폐기·호출량 정책을 추가할 수
  있도록 호출자 정보를 내부 context로 전달한다. 빈 key를 공개 사용자의 신원으로
  간주하지 않는다.
- IP별 요청·WebSocket 연결 제한, 요청 크기·페이지 크기·연결 수명·heartbeat를
  설정값으로 둔다. CORS 허용은 브라우저 편의이고 인증 수단이 아니다.
- 로컬 접근 로그에는 시각, 요청 ID, 경로 템플릿, 상태 코드, 지연, 결과 건수,
  신뢰 가능한 Tunnel 전달 IP만 남긴다. API key, 채팅 본문, cookie, 원본 사용자 ID는
  기록하지 않는다. 로그 파일·Docker 로그는 용량/개수 제한과 회전을 적용한다.
- `data-api`의 readiness는 프로세스만이 아니라 query·archive 색인의 읽기 가능성을
  구분한다. Cloudflare Tunnel 연결, WebSocket upgrade, 재접속, archive 페이지,
  DNS/TLS를 실제 외부 경로에서 검증한다.

## docs.rogi.chat

별도 공개 저장소 `h66rogi/rogi-docs`는 Docusaurus 소스, 시작 안내, 오류·cursor·
WebSocket 예제, changelog를 소유한다. HTTP OpenAPI와 WebSocket 메시지 JSON Schema의
원본은 API 코드와 같은 `rogi-collector` revision에서 관리하고 docs 빌드가 고정된
release를 가져와 문서와 구현의 어긋남을 검사한다. 공개 전에는 `planned` 계약과
실제 사용 가능한 endpoint를 명확히 구분한다.

Docusaurus `build/`를 Workers Static Assets로 배포한다. Worker 실행 코드가 필요 없는
정적 배포로 시작하고 `docs.rogi.chat`을 Worker custom domain으로 연결한다. 이
도메인은 collector Tunnel과 별도 경로다. docs 저장소의 CI는 build와 내부 링크,
OpenAPI/JSON Schema 유효성을 검증한 후 승인된 배포 흐름에서 publish한다.

## 구현 순서와 인수 기준

[운영 전환 계획](public-data-api-rollout.md)은 SecretString 키 확장과 8개 역할 감독,
Tunnel, R2 활성화의 실제 순서를 정리한다.

1. [방송 session 식별·영구 archive 저장 계약](public-chat-archive-contract.md)을 확정하고 합성 입력으로
   방송 전환·중복·복원·R2 장애를 검증한다.
2. collector 내부 archive writer와 read-only 조회 계약을 추가한다. 기존
   주루마블 gRPC, 후원 journal, Redis 실시간 경로의 회귀를 확인한다.
3. Go `data-api`와 내부 Tunnel connector를 추가한다. 로컬 로그·제한·readiness,
   WebSocket 재접속과 gap 신호를 검증한다.
4. 공개 DNS/TLS/Tunnel을 연결해 실제 외부 경로를 확인한다. 기존 private 7443과
   EC2 보안 그룹의 인터넷 인바운드 차단을 재검증한다.
5. `rogi-docs`를 생성해 계약을 게시하고 Workers 정적 배포 및 `docs.rogi.chat`을
   확인한다. 후로기 실방송에서 on/off·채팅·방송 종료·과거 조회를 최종 검증한다.

구현 전 확정할 세부값은 공개 사용자 ID 표현, archive의 초기 hot 기간, 접근 로그
회전 한도, 첫 연결 수·rate limit, archive 비용 알림 기준이다. 모두 설정 가능한
값으로 두며 v1 공개 범위는 `h66rogi` 한 채널로 제한한다.

## 참조

- [Cloudflare Tunnel: WebSocket 지원](https://developers.cloudflare.com/cloudflare-one/faq/cloudflare-tunnels-faq/)
- [Cloudflare Workers: Docusaurus 정적 배포](https://developers.cloudflare.com/workers/framework-guides/web-apps/more-web-frameworks/docusaurus/)
- [Cloudflare Workers: Static Assets 설정](https://developers.cloudflare.com/workers/static-assets/)
