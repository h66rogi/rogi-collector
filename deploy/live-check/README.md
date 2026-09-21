# EC2 실방송 검증 배포

기존 health-only 배포와 분리한 `rogi-collector-live-check` Compose 프로젝트다.
사용자가 배포·실방송 검증을 요청해 추가했다. 원본 collector 역할 4개와 PostgreSQL·Redis·cookie-auth를 실행한다.
정식 CI release, 재부팅 시 secret 준비, 인증서 자동 회전을 대체하지 않는다.
이번 검증용 서버/소비자 인증서는 발급 후 7일 유효하므로 그 전에 교체해야 한다.

## 준비와 시작

1. 검증한 소스로 Linux amd64 정적 바이너리 `discover`, `coordinator`, `worker`, `query`,
   `migrate`, `healthcheck`, `collector-check`를 빌드한다. Dockerfile context에는 `bin/`과 LICENSE/NOTICE를 둔다.
   cookie-auth 이미지는 저장소 root context와 `Dockerfile.cookie-auth`로 빌드한다.
2. 서버의 별도 EBS 경로와 tmpfs secret 경로를 준비한다. 실제 값은 Git 밖에 둔다.
   Compose 변수: `CANDIDATE_IMAGE`, `COOKIE_AUTH_IMAGE`, `POSTGRES_IMAGE`, `REDIS_IMAGE`,
   `CHECK_DATA_ROOT`, `CHECK_SECRETS_ROOT`, `CHECK_BIND_IP`(EC2 private IP).
   DB/Redis host port는 열지 않는다. 7443은 소비자 SG에서만 접근하도록 한다.
3. 역할별 `discover.env`, `coordinator.env`, `worker.env`, `query.env`는 0600/UID 65532다.
   `ROLE_ENV_FILE`은 셸이 아닌 `KEY=value` 데이터로 읽으며 기존 프로세스 환경변수를 우선한다.
   DB 비밀번호는 URL 인코딩한다. `REDIS_PASSWORD`를 지정한다.
   `CHANNEL_ALLOWLIST=soop:h66rogi`, `SOOP_AUTH_MODE=cookie`를 모든 역할에 일치시킨다.
   일반 방송의 쿠키 없는 시험만 `anonymous-test`를 명시할 수 있다.
4. cookie-auth.env에는 SOOP_ID/SOOP_PW와 내부 갱신 API token을 둔다.
   쿠키 디렉터리는 UID 65532, 파일은 0600이며 discover/worker에 읽기 전용으로 공유한다.
   이 Compose의 env_file은 Docker inspect에 환경변수를 노출할 수 있으므로 호스트 관리자만 접근한다.
5. PostgreSQL/Redis를 먼저 시작하고 `migrate`를 별도 관리자 접속으로 실행한다.
   앱 역할별 DB 계정을 따로 만든다. 서버/소비자 인증서, private key, CA를 준비한다.
   상세 역할 설정은 [코드 인계](../../docs/collector-code-handoff.md)를 따른다.
6. Ubuntu의 user namespace 제한이 있는 호스트에는 `cookie-auth.apparmor`를 로드한다.
   Chromium sandbox는 유지한다. 현재 컨테이너는 seccomp unconfined를 사용하므로 정식 배포 시
   호스트 버전에 맞춘 프로파일을 별도 검토한다.
7. `rogi-collector-live-check.service`는 이미 준비된 설정에서 Compose를 foreground로 감독한다.
   한 프로세스 종료 시 전체 검증 스택을 재시작한다. 기존 production updater와 다른 이름/경로다.
   **tmpfs secrets를 재부팅 시 복원하는 준비 과정이 없으므로 boot enable하지 않는다.**
   중지는 `systemctl stop rogi-collector-live-check`; 데이터 볼륨을 삭제하지 않는다.

## 검증 명령

`collector-check`는 `CHECK_ADDRESS`, `CHECK_SERVER_NAME`, `CHECK_CHANNEL`,
`CHECK_CERT_FILE`, `CHECK_KEY_FILE`, `CHECK_CA_FILE`을 받는다. `CHECK_CONSUMER` 기본은 rogimarble이다.
이 값도 `ROLE_ENV_FILE`에 둘 수 있다. 인증서 URI는 query의 읽기 권한과 일치해야 한다.

- 인자 없음: 수집 상태 조회.
- `chat`: 호출 이후 새 채팅 1건을 최대 45초 기다린다. `CHECK_CHAT_COUNT`로 1~100건을 지정한다.
  방송 종료/조용한 방송의 timeout을 수신 성공으로 세지 않는다.
- `donations`: 절대 경로 `CHECK_INBOX_DIR`에 별도 검증용 파일 inbox를 fsync한 뒤 ACK한다.
  `CHECK_REPLAY_FROM_START=1`이면 보존 시작점에서 다시 읽고 같은 eventId/payload인지 확인한다.
  단일 프로세스로 실행하며 한 번에 최대 100건이다. 실제 게임의 DB inbox가 아니다.

표준 출력에는 상태/집계/offset만 남긴다. 시청자 ID·메시지·쿠키를 출력하지 않는다.
테스트 방송을 썼다면 종료 시 모든 역할 설정을 h66rogi로 복원하고 기다림 상태를 확인한다.
실제 관측 후원으로 게임 효과를 만들거나 테스트 후원을 방송에 보내지 않는다.
