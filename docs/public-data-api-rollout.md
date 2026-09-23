# 공개 data-api 운영 전환 계획

상태: draft PR의 실행 계획. 운영 변경은 아직 수행하지 않았다.

## 첫 단계: 내부에서만 data-api 실행

query 이미지에 `/data-api` 실행 파일을 함께 넣고, 별도 Compose `data-api` 역할과
systemd 감독·health 검사를 추가한다. 이 역할은 host port를 게시하지 않는다.
`PUBLIC_ARCHIVE_HISTORY_ENABLED`는 설정하지 않아 과거 조회는 503으로 유지한다.
내부 HTTP 상태·최근 채팅·WebSocket 기능을 먼저 검증한다.

운영 Secrets Manager의 `data-api.env`는 별도 역할 파일이다. 최소 설정은
`CHANNEL_ALLOWLIST=soop:h66rogi`, `DATABASE_URL`(읽기 전용 DB 계정),
`REDIS_ADDR`, `SOOP_DIAGNOSTIC_TOKEN`이다. `SOOP_DIAGNOSTIC_TOKEN`은
discover의 내부 진단 토큰과 일치해야 한다. API 서비스에는 SOOP 로그인 정보,
private gRPC TLS 키, DB DDL 권한을 주지 않는다. 로그는 현재 Compose의 10 MiB × 3
로컬 회전 설정을 따른다.

현재 host의 secret loader는 정확한 기존 키 집합을 검사한다. 기존 loader가 동작하는 동안
새 `data-api.env` 키를 먼저 추가하면 검증이 실패한다. 새 loader는 전환 기간에 기존
키 집합과 `data-api.env`를 포함한 확장 집합만 허용한다. 새 역할을 먼저 시작하면
파일이 없어 실패한다. 따라서 다음 순서를 하나의 점검된 전환 작업으로 수행한다.

1. 두 키 집합을 받아들이는 loader·validator와 host 준비 코드를 먼저 설치한다. 기존
   서비스가 살아 있는지 확인한다.
2. Secrets Manager SecretString의 정확한 키 집합에 `data-api.env`를 추가하고,
   실행 계정·모드·내용을 host에서 검증한다. 기존 키 값은 보존한다.
3. 새 release의 Compose·systemd를 활성화한다. 8개 역할의 health, private 7443,
   Redis 실시간 경로를 함께 확인한다.

이 순서는 자동 release의 변경분을 바로 `main`에 합치기 전에 별도 전환 절차로 확정해야
한다. 실패 시 이전 이미지로 되돌릴 수 있도록 새 secret key를 읽는 loader의 호환성을
양방향으로 확인한다.

## 둘째 단계: 외부 Tunnel

`data-api.rogi.chat`용 Cloudflare Tunnel을 독립 connector로 설정한다. origin은
Compose 내부 `http://data-api:8080`이고 EC2 보안 그룹에 새 inbound 규칙을 추가하지
않는다. Tunnel token은 별도 운영 secret으로 관리한다. Cloudflare DNS 소유 root는
`rogichat/infrastructure/environments/prod/cloudflare`이며, 기존 `rogi.chat`/Marble
레코드와 충돌하지 않는 별도 변경으로 계획한다. WebSocket upgrade와 재접속, 외부
TLS, 비정상 upstream 시 오류를 확인한다. 익명 접근 제한은 이 단계 전에 확정한다.

## 셋째 단계: R2 archive와 과거 조회

비공개 R2 bucket에 자동 만료 정책을 두지 않는다. bucket 범위의 S3 API 토큰은
exporter에는 객체 읽기·쓰기, data-api에는 객체 읽기 권한만 준다. R2 endpoint는
`https://<account_id>.r2.cloudflarestorage.com`, SDK region은 `auto`다. 토큰은
Cloudflare 일반 API 토큰과 별개이며 Git/이미지/문서에 값을 남기지 않는다.

PostgreSQL migration → 세션 emitter → worker spool shadow → R2 exporter와 readback →
복원 시험 → 영구 gap 기록 순으로 검증한다. `CHAT_ARCHIVE_ENABLED`와
`PUBLIC_ARCHIVE_HISTORY_ENABLED`는 모든 게이트가 통과하기 전까지 끈다.
과거부터 이미 사라진 Redis 채팅은 복원할 수 없다. 공개 전에는 방송 목록 범위와
`archiveStartedAt`·`complete`·`gaps` 응답 계약을 구현과 맞춰 고정한다.

Terraform `apply`와 운영 resource/secret 변경은 최신 exact plan, 영향 범위,
승인 후에만 수행한다. 인프라와 애플리케이션 전환 완료 후 외부 URL을 검증한다.

## 현재 남은 작업

- 별도 읽기 전용 DB 계정과 권한, secret 무중단 전환 절차
- host/Tunnel connector 이미지 또는 binary·토큰 관리와 DNS Terraform plan
- 공개 HTTP/IP·WebSocket 제한, 접근 로그 필드·보존, 실제 방송 검증
- R2 bucket·전용 토큰, exporter 상시 실행, 영구 gap 원장과 archive 복원 시험
