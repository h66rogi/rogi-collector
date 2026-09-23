# Collector 운영 배포

정식 profile은 `soop-single-channel`이다. GitHub main release → private GHCR digest → 승인된
SSM 전달 → host 검증/마이그레이션 → systemd 감독 경로를 사용한다.
배포 자동화는 [CI/CD](deployment-ci.md), 프로세스·방송·쿠키 상태 점검은
[모니터링](operations-monitoring.md), 공개 API의 저장 경계는 [공개 API 구조](public-data-api-architecture.md)를 따른다.
실행 Compose는 `deploy/compose.production.yaml`이다.

## 실행과 저장 경계

- 정식 Compose 프로젝트: `rogi-collector`; 역할은 PostgreSQL, Redis, discover,
  coordinator, worker, query, data-api, archive-exporter, cloudflared, cookie-auth이다.
- release bundle과 current: `/opt/rogi-collector/app`; 설정: `/etc/rogi-collector`;
  tmpfs 비밀 파일: `/run/rogi-collector`; encrypted data EBS: `/srv/rogi-collector`.
- host-ready는 EBS mount와 예상 UUID를 검사한다. 빠진 EBS를 root 디스크의 빈 폴더로 대체하지 않는다.
- 검증용 `live-check` DB·spool은 보존하고 정식 DB로 섞지 않는다. 첫 정식 시작의 대상은 h66rogi 하나다.
- discover/worker는 SOOP outbound를 사용한다. query는 VPC private IP의 7443만 게시하고,
  SG에서 주루마블 SG를 허용한다. 서버 SAN `collector.internal`, TLS 1.3, client URI 권한을 검증한다.
  PostgreSQL/Redis에는 host port가 없다. 기존 container IMDS guard를 유지한다.

## 비밀 설정과 재부팅

Secrets Manager의 runtime SecretString은 다음 정확한 키 집합이다. 실제 값은 Git이나 bundle에 넣지 않는다.

`postgres-admin-password`, `postgres-migrate-password`, `migrate.pgpass`, `redis-password`,
`discover.env`, `coordinator.env`, `worker.env`, `query.env`, `data-api.env`,
`archive-exporter.env`, `cookie-auth.env`, `app-migrate.env`, `public-api-db-password`,
`tunnel-token`, `tls-ca.pem`, `tls-ca.key`.

기존 root-only `secrets-manager.json`의 ARN/region을 통해 instance role이 읽는다.
새 비밀 세대를 tmpfs에 쓰고 검증한 뒤 역할 UID로 소유권을 설정한다.
모든 앱은 자기 역할 파일만 mount한다. SOOP_ID/PW는 cookie-auth 파일에만 들어가며
Docker env_file을 쓰지 않고 프로세스 안에서 `ROLE_ENV_FILE`의 KEY=value를 읽는다.
쿠키 자체는 encrypted EBS에 0600으로 원자 교체하고 discover/worker에 RO 공유한다.
root 호스트 관리자는 런타임 비밀에 접근할 수 있다.

`CHANNEL_ALLOWLIST=soop:h66rogi`, `SOOP_AUTH_MODE=cookie`를 해당 Go 역할에 설정한다.
기타 필드는 [역할별 인계](collector-code-handoff.md)를 따른다.

## 데이터베이스와 배포

기존 `deploy/migrations`의 marker checksum 원장은 보존한다. 이어 query 이미지의 `/migrate`를
app-migrate 역할로 한 번 실행하고 `shared/migrations`의 앱 원장을 별도로 유지한다.
둘 다 성공해야 current를 승격한다. 런타임 계정은 DDL 권한 없이 schema/table/sequence 권한만 받는다.
새 migration의 table/sequence에도 권한이 이어지도록 migration owner의 default privileges를 설정한다.

manifest는 8개 image digest, 공개 runtime 파일 allowlist checksum, SQL checksum,
private IP를 포함한 비밀 없는 host overlay를 검증한다. tag만 있는 이미지는 허용하지 않는다.
release updater는 검증된 digest와 runtime manifest만 적용한다.

systemd는 역할별 foreground Compose를 감독하고 컨테이너 종료 후 재시작한다.
`--force-recreate`로 새 secret inode와 이미지/설정을 다시 mount한다. Compose restart는 no다.
재부팅 때 host-ready → DB/Redis → marker/app migration → 각 역할 순서로 시작한다.
일일 DB backup/S3 upload와 10분 release timer는 기존 경로를 유지한다.
백업은 UTC 18:40부터 최대 20분 지연 후 custom-format pg_dump를 gzip으로 저장하며, 로컬 7일 초과분을 정리한다.
WAL 연속 보관·시점 복원은 구현하지 않았고 실제 백업 복원 시험도 남아 있다.
스키마 down migration, data volume 삭제, 이미지 host build는 배포 중 실행하지 않는다.

## 인증서

root-only issuer CA key는 Secrets Manager와 host tmpfs에만 두고 앱에 mount하지 않는다.
서버 leaf는 90일 유효하며 boot 준비와 일일 `rogi-collector-tls.timer`가 확인한다.
30일 미만이면 새 key/cert를 검증하고 디렉터리 세대를 교체한 뒤 query만 재시작한다.
수동 회전 확인은 `rotate-server-tls.py --force --restart`로 할 수 있다.
CA 변경은 소비자 신뢰 저장소와 함께 조정해야 하며 자동으로 별도 CA를 생성하지 않는다.

소비자는 자신의 client private key/certificate와 CA를 별도로 받아야 한다.
첫 주루마블 읽기 인증서는 365일이며 URI `spiffe://rogi-collector/rogimarble/reader`로 h66rogi에만 권한이 있다.
소비자 인증서 발급/갱신은 운영자가 issuer로 서명해 전달하는 절차다. 서버 leaf 자동 회전과 구분한다.
실제 게임 소비 프로세스의 재시작/credential reload 연결은 주루마블 inbox 구현 단계에 반영한다.

## 쿠키 브라우저

Ubuntu의 user namespace 제한에는 `deploy/cookie-auth.apparmor`를 설치한다.
Chromium sandbox는 유지하며 현재 이미지의 seccomp는 unconfined다.
쿠키 서비스 health는 프로세스 가용성이고 로그인 준비 상태와는 별개다.
24시간 갱신과 인증된 on-demand refresh API를 제공한다. API는 외부 host port로 공개하지 않는다.

## 검증

`make race`, `make build`, `make source-check`, `tools/ops/test.sh`,
`python3 tools/ops/test_rotate_server_tls.py`, Selenium 환경의 cookie-auth unittest를 사용한다.
manifest 변조·secret 경계·migration 실패 시 승격 거부·인증서 동일 CA 회전을 검사한다.
정식 EC2에서는 10개 역할 health, h66rogi 상태 RPC, 인증서 교체 후 peer 연결,
강제 종료/재부팅 복구, backup 완료를 별도로 확인한다.
프로세스 healthy를 방송 연결 또는 게임 연동 성공으로 표현하지 않는다.

## 운영 화면 방송 조회

주루마블의 조회 전용 방송 체크는 `discover.env`와 `query.env`의 동일한
`SOOP_DIAGNOSTIC_TOKEN`을 사용한다. 최소 32자 난수로 설정하고 운영 secret에만 보관한다.
미설정 시 기존 수집은 유지되며 방송 체크만 사용 불가다. discover의 8080 포트는
Compose 내부에서만 접근하며 host에 게시하지 않는다. [호출 경계와 제한](broadcast-diagnostics.md)을 따른다.

입장·채팅 테스트도 제공하려면 같은 `SOOP_DIAGNOSTIC_TOKEN`을 `worker.env`에 추가한다.
worker의 내부 8080 listener는 host에 게시하지 않는다. 테스트 수명과 보관 범위는 [진단 계약](broadcast-diagnostics.md#입장채팅-수신-테스트)을 따른다.
