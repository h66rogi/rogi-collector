# 공개 방송 데이터 API 구조

사용자 요청·응답 계약은 [docs.rogi.chat](https://docs.rogi.chat/)에서 관리한다. 이 문서는 수집기 내부의 저장·배포 경계를 설명한다.

## 요청 경로

`data-api.rogi.chat`은 Cloudflare Tunnel을 통해 `rogi-collector` Compose 네트워크의 Go `data-api:8080`으로 연결된다. data-api 포트는 EC2 host에 게시하지 않는다. 주루마블의 private query gRPC 7443, PostgreSQL, Redis, SOOP 인증 쿠키, R2 bucket은 인터넷에 직접 노출하지 않는다.

`data-api`는 방송 진단 결과를 최대 10초간 캐시하고, 수집 상태를 PostgreSQL에서 읽는다. 최근 채팅과 WebSocket은 Redis stream을 읽는다. 과거 채팅은 PostgreSQL의 방송·segment 색인과 Cloudflare R2 객체를 합쳐 읽는다. 공개 DB 역할은 필요한 테이블의 SELECT만, R2 자격 증명은 객체 읽기만 허용한다.

## 채팅 저장

worker는 SOOP에서 받은 일반 채팅을 로컬 disk spool에 수락한 뒤 PostgreSQL에 기록한다. 수락한 메시지의 안정적인 event ID로 재시도 중복을 제거한다. 원본 사용자 ID는 비밀키 기반 가명으로 바꾸고, 공개 기록에는 표시 이름과 메시지만 보존한다. 후원 및 운영 메시지는 공개 채팅 archive에 넣지 않는다.

archive-exporter는 오래된 PostgreSQL 채팅을 gzip NDJSON segment로 만들어 비공개 R2 bucket에 업로드한다. 업로드 후 객체를 다시 읽고 SHA-256을 확인한 뒤 segment 색인을 확정한다. 색인 확정 전에는 PostgreSQL 본문을 삭제하지 않는다. bucket에는 시간 기준 만료 lifecycle을 두지 않는다.

Redis 최근 채팅은 최대 24시간·10,000개 stream 항목이다. WebSocket cursor는 이 범위에만 유효하며, generation 변경 또는 범위 만료 시 `chat.gap`을 보낸다. 과거 채팅의 cursor와 호환되지 않는다.

보관 목록에는 채팅이 한 건 이상 수락된 방송만 포함된다. 보관 기능 활성화 이전의 채팅은 소급 생성할 수 없다. 세션 생성 전에 들어온 채팅은 비공개 대기열에 보존하고 재배정을 시도한다. 보관 스풀 저장 실패는 품질 공백으로 기록하며 `knownArchiveGap`에 반영한다. `sourceGapDetected`는 방송 탐색 관측 공백을 뜻하고, `complete=false`는 방송 전체 채팅의 무누락 수집을 증명하지 않는다는 뜻이다.

## 운영 경계

- Compose 역할: postgres, redis, discover, coordinator, worker, query, data-api, archive-exporter, cloudflared, cookie-auth.
- 후원 spool과 채팅 spool은 encrypted data volume의 서로 겹치지 않는 형제 디렉터리다. worker는 경로 중첩을 시작 시 거부한다.
- `data-api`는 별도 읽기 전용 PostgreSQL 역할과 R2 객체 읽기 자격 증명을 사용한다. `archive-exporter`만 R2 쓰기 자격 증명을 사용한다.
- 런타임 비밀은 AWS Secrets Manager에서 host tmpfs로 적재된다. 비밀 값이나 R2 객체 본문은 Git, 릴리스 번들, 공개 로그에 포함하지 않는다.
- 공개 요청 제한은 origin 전체 초당 20회·순간 60회, WebSocket 동시 연결은 기본 32개·IP당 2개다. readiness 조회는 짧게 캐시한다. 접근 로그는 host의 로컬 로그 순환 정책을 따른다.

배포 경로와 시스템 서비스는 [운영 배포](deployment-production.md), 변경 검증은 [CI/CD](deployment-ci.md)를 참고한다.
