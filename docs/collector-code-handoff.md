# 기존 코드 기반 작업 인계

원본 8개 모듈의 lifecycle을 유지했다. 반입 근거는 `source-import-manifest.json`,
현재 증거와 남은 제품 검증은 [구현 상태](implementation-status.md)를 따른다.
기존 CI/CD 관련 `.github`, `infrastructure`, `tools/ops`는 수정하지 않았다. 후속 요청으로 `deploy/live-check` 격리 배포를 추가했다.

## 실행 코드와 설정

| 역할 | entrypoint | 이번 연결 |
| --- | --- | --- |
| discover | `discover/cmd` | 등록한 한 채널만 조회, cookie snapshot, 오류/방송 대기 상태 |
| coordinator | `coordinator/cmd` | 원본 leader/할당, 대상·구독 제한; 원본 admin gRPC 시작 제거 |
| worker | `worker/cmd` | 원본 manager/connector/reconnect, cookie snapshot, 후원 spool/journal, 채팅 Redis |
| query | `query/cmd` | 원본 Server lifecycle, collector v1 전용 mTLS 7443, 보존 정리 |
| shared | 라이브러리와 `shared/cmd/migrate`, `shared/cmd/rotate-generation` | 기존 PgStore/RedisStore 확장, 앱 migration/명시적 restore |
| cleanup / chat-exporter | 각 `<role>/cmd` | 원본 코드 보존; 첫 제품에서는 실행하지 않음 |

- 공통 `CHANNEL_ALLOWLIST=soop:h66rogi`. 한 SOOP ID만 허용하며 미설정은 수집 대상 없음이다.
- 기존 `DATABASE_URL`, `REDIS_ADDR`, 역할별 worker/leader 설정을 사용한다. 실제 값은 이 저장소에 넣지 않는다.
- discover·worker의 `SOOP_COOKIE_FILE`: cookie-auth가 원자 교체하는 snapshot의 **절대 경로**.
- worker의 `DONATION_SPOOL_DIR`: 지속 디스크의 절대 디렉터리. 단일 writer lock, 0700 디렉터리/0600 파일.
  현재 한도는 1GiB 및 1만 파일이며 파일 최대 재생 연령은 30일이다.
- query: `COLLECTOR_TLS_CERT_FILE`, `COLLECTOR_TLS_KEY_FILE`, `COLLECTOR_CLIENT_CA_FILE`이 필수다.
  TLS 1.3과 클라이언트 인증서를 사용하며 소비자는 서버 SAN/CA를 검증해야 한다.
- `COLLECTOR_CONSUMER_ID` 기본값은 `rogimarble`. `COLLECTOR_READ_CERT_URI`는 필수,
  `COLLECTOR_MANAGE_CERT_URI`, `COLLECTOR_RECOVERY_CERT_URI`는 선택이다. 미설정 권한은 거부한다.
  인증서 URI SAN을 정확히 비교한다. 하나의 운영 인증서에 여러 scope를 줄 때는 의도적으로 같은 URI를 설정할 수 있다.
- 외부 포트는 query의 private 7443만 소비자에 연결한다. 원본 50051/50052·query HTTP 관리 handler는 runtime에서 등록하지 않는다.
  보존한 원본 query/admin 코드·proto는 제품 endpoint가 아니다.
- `/healthz`, `/readyz` probe와 채널 수집 상태는 별개다. 프로세스 ready가 SOOP 연결 성공을 뜻하지 않는다.
  probe 기본 bind는 127.0.0.1이며 외부 공개용 포트가 아니다.

## 쿠키 프로세스

cookie-auth만 `SOOP_ID`/`SOOP_PW`를 받는다. Python/Selenium, 설치된 Chrome/Chromium,
호환 ChromeDriver와 `SOOP_CHROMEDRIVER` 설정이 필요하다. 브라우저는 비-root/sandbox로 실행한다.
24시간/명시적 갱신 요청에 로그인하며 기존 쿠키는 실패한 로그인으로 덮어쓰지 않는다.

cookie 디렉터리는 cookie-auth RW, discover/worker RO로 mount한다. 0600 파일을 읽는 동일 UID를 사용한다.
원자 교체를 위해 단일 파일 bind mount는 사용하지 않는다. 8091 갱신 API는 내부 token 인증이며
쿠키 원문을 반환하지 않는다. 자세한 설정은 [컴포넌트 문서](../cookie-auth/README.md)를 따른다.

현재 player/login endpoint는 `.com`이다. 실제 cookie domain을 `.co.kr`에서 `.com`으로 바꾸지 않는다.
기존 domain snapshot이 맞지 않으면 새 로그인으로 적합한 쿠키를 얻어야 한다. 웹소켓 호스트에는 Cookie 헤더를 전달하지 않는다.

## 앱 스키마와 기존 배포 골격 연결

Go 빌드는 `./<role>/cmd`이며 기존 `./cmd/<role>` health-only 골격과 다르다.
원본 8개 모듈과 `shared`, `proto`가 build context에 필요하다.
`make build`는 기존 역할 6개와 migration/restore/healthcheck/collector-check 명령을 검사한다.

앱 스키마는 `shared/migrations/001`~`006`이다. 별도 `collector_app_migrations` 체크섬 원장을 쓰는
`go run ./shared/cmd/migrate`를 추가했다. **기존 배포 marker migration과 원장을 덮어쓰지 않는다.**
동시 실행은 advisory lock으로 직렬화하고 SQL/체크섬 기록을 한 transaction으로 적용한다.
테스트는 빈 격리 schema에서 재실행까지 확인했다. 이전 별도 migration 도구로 source SQL을 이미 적용한 DB에 대한
자동 원장 채택은 지원하지 않는다. 그 경우 상태를 대조해 CI/CD 작업에서 연결해야 한다.

반입한 003의 원본 전용 DB role/pg_partman 설치는 제거하고 보존된 history 테이블에 일반 default partition을 둔다.
기존 운영에 적용된 migration을 교체한 작업이 아니라 아직 연결하지 않은 원본 앱 스키마의 제품용 조정이다.
006에는 채널 상태/소유권 grant/후원 journal/outbox/소비자 cursor/멱등 요청/복구 감사 테이블이 들어간다.
ClickHouse는 최초 제품 실행에서 사용하지 않는다.

## 소비자 동작과 보존

1. `GetCollectionStatus`에서 earliest/current cursor와 recovery revision을 읽는다.
2. 최초 `ListDonations`/`WatchDonations`에도 명시적 cursor가 필요하다. 최초 retained beginning이나
   자신의 연속 저장 위치에서 읽는다. 임의 최신 위치로 넘어갈 때는 명시적 복구 결정이 필요하다.
3. 주루마블은 자신의 inbox와 cursor를 transaction으로 저장한 뒤 `AckDonations` 한다.
   같은 eventId는 게임 효과를 다시 만들지 않는다. Collector는 소비자의 DB commit 자체를 대신 확인할 수 없다.
4. generation/보존 범위/revision 오류는 정지하고 운영자가 `ResolveConsumerRecovery`로 범위·사유·operator·멱등 키를 기록한다.
   같은 generation에서 건너뛴 구간은 `unrecoveredRanges`의 inclusive 범위로 명시한다.
   baseline은 ACK로 취급하지 않는다. 기존 Watch는 다음 DB 읽기에서 종료되므로 소비자는 복구 결정 후 기존 stream을 취소하고
   이미 전송 중이던 응답도 이전 revision의 작업으로 폐기해야 한다.
5. `WatchChat` 최초 요청은 보존된 최근 범위에서 시작한다. 같은 stream의 잘린 cursor는 다음 이벤트에 `gapBefore`를 붙인다.
   stream generation 변경은 FailedPrecondition으로 종료하므로 명시적 재구독이 필요하다. 채팅은 후원처럼 영구 재생되지 않는다.

후원 payload: 최소 7일+소비자 ACK, 최대 30일/1GiB. 한 번의 정리 batch는 500건이다.
강제 prefix 만료는 cursor 오류/earliest로 드러난다. ID tombstone·grant·멱등 키는 90일,
복구 감사 기록은 365일 보존한다. 90일 이전의 멱등 키를 재사용하지 않는다.
일반 채팅은 최대 24시간/채널 1만 건, raw 패킷은 제품 stream에서 제외한다.

DB 복원 후 **worker를 다시 시작하기 전에** `shared/cmd/rotate-generation`을 실행한다.
`RESTORE_CHANNEL_ID`, `RESTORE_EXPECTED_GENERATION`, `RESTORE_REASON`이 필수이며 DB 접속은 `DATABASE_URL`이다.
자동 시작 시 generation을 바꾸지 않는다. 보존 eventId는 유지하면서 old cursor/owner를 무효화한다.
이전 generation spool은 자동 수락하지 않는다. 복구 운영자가 누락 구간과 보관 파일을 대조해야 한다.

spool `.pending`/checksum 오류/full은 자동 삭제하지 않는다. 수집 상태와 보관 파일을 점검한 뒤 복구한다.
grant의 단조 시계 deadline을 넘으면 새 수락을 중단한다. 이미 저장된 파일은 원래 grant/수락 시점을 DB 기록과 대조한다.
호스트·DB 시계 동기화가 필요하며 실제 강제 종료/EC2 교체 시험은 별도다.

## 코드 검증 재실행

- `make test`, `make race`, `make build`, `make source-check`.
- Selenium이 설치된 Python으로 `make cookie-test PYTHON=/path/to/python`.
- 격리 PostgreSQL/Redis에만 `COLLECTOR_TEST_DATABASE_URL`, `COLLECTOR_TEST_REDIS_ADDR`을 설정하고 `make integration-test`.
  각 PG 테스트는 고유 schema를 만들고 삭제하며, Redis는 고유 key만 만든다.

실제 SOOP 로그인·연령제한 방송·별도 EC2 수신/파일 inbox ACK는 검증했다. 주루마블 실제 DB inbox/게임 통합은 아직 연결되지 않았다. [실방송 기록](implementation-status.md)을 따른다.

## 실배포에서 확인된 연결 차이

- 역할 secret file 로더 `ROLE_ENV_FILE`과 `REDIS_PASSWORD`를 연결해야 한다.
- Redis orphan cleanup은 STREAM 타입만 스캔해야 한다. generation 문자열을 지우면 채팅 재생 cursor가 깨진다.
- cookie-auth Chromium의 Ubuntu user namespace 제한은 전용 AppArmor profile로 풀었다. sandbox는 꺼두지 않는다.
- 이번 후보 스택은 별도 systemd에서 감독하지만 재부팅 secret 복원/인증서 자동 회전은 정식 배포 과정에 연결해야 한다.
- 주루마블 EC2의 테스트 inbox는 실제 소비 구현이 아니다. 소비자가 DB inbox+cursor를 transaction으로 저장한 뒤 ACK하고, eventId로 중복 게임 효과를 막는 구현이 남아 있다.
