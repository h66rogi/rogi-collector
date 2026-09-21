# SOOP 쿠키 획득 컴포넌트

후로기 주루마블 수집기가 연령제한 방송에 로그인 상태로 접속하도록 쿠키를 마련한다.
사용자는 수집용 계정의 `SOOP_ID`, `SOOP_PW`를 제공한다. 계정 로그인과 방송 대상 `h66rogi`는 별개다.
기존 Go 수집기의 8개 모듈은 유지하고 브라우저 로그인이 필요한 부분만 작은 Python 컴포넌트로 추가했다.

## 동작

1. 시작할 때 같은 계정으로 저장한 쿠키와 마지막 성공 시각을 확인한다. 없거나 24시간이 지났으면 로그인한다.
2. Selenium의 새 브라우저에서 SOOP 로그인 폼을 제출한다. 로그인 페이지를 벗어나고
   `AuthTicket`과 해당 ID의 `UserTicket`이 발급됐는지 확인한다.
3. SOOP 쿠키의 domain/path/expiry를 포함한 JSON snapshot을 권한 0600 파일로 원자 교체한다.
4. 마지막 성공 후 24시간마다 다시 로그인한다. 서비스 재시작만으로 불필요하게 다시 로그인하지 않는다.
5. `POST /v1/refresh` 또는 `refresh` 명령으로 필요할 때 갱신한다. 동시 요청은 같은 로그인을 기다리며,
   60초 이내 연속 요청은 직전 결과를 사용한다. 응답은 상태와 시각만 포함한다.
6. discover와 worker는 SOOP player API를 요청할 때마다 파일을 읽는다. 다음 방송 확인/재접속부터 새 쿠키가 반영된다.
   이미 열려 있는 WebSocket을 일일 갱신만으로 강제 종료하지 않는다.

갱신 실패 시 같은 계정의 마지막 정상 파일은 보존하고 `refresh_failed`를 표시한다.
계정이 바뀌었거나 시작 시 파일이 손상된 경우에는 `.rejected` 파일 하나로 격리해 잘못된 계정 쿠키 사용을 막는다.
로그인 미확인/timeout은 자동으로 하루마다 재시도하고 명시적 갱신도 가능하다. 그 외 일시 실패는 5분 후 재시도한다.
Go 소비자는 발급 후 48시간이 지났거나 인증 쿠키가 만료/누락된 snapshot을 거부한다.
쿠키가 없다고 익명 수집으로 바꾸지 않는다.

## 실행 입력

Python 3.12+, Selenium(고정 버전은 requirements.txt), Chrome/Chromium과 호환 ChromeDriver가 필요하다.
정식 [Dockerfile](../deploy/Dockerfile.cookie-auth)이 Chromium과 ChromeDriver를 설치한다.
[운영 Compose](../docs/deployment-production.md)가 실행·저장·비밀 파일을 연결한다. 실행 중 driver를 자동 다운로드하지 않는다.
Linux의 file lock/process group을 사용한다. Chrome sandbox를 켜고 실행할 수 있는 비-root 사용자로 실행한다.

| 환경 변수 | 사용 역할 | 의미 |
| --- | --- | --- |
| `ROLE_ENV_FILE` | cookie-auth만, 선택 | 실행 시 읽을 `KEY=value` 파일; 셸 실행 없이 읽고 이미 설정된 환경변수를 우선 |
| `SOOP_ID` | cookie-auth만 | 로그인할 계정 ID |
| `SOOP_PW` | cookie-auth만 | 비밀번호 |
| `SOOP_COOKIE_FILE` | cookie-auth/discover/worker | JSON snapshot의 절대 경로; 역할마다 같은 파일을 가리킴 |
| `SOOP_COOKIE_API_TOKEN` | cookie-auth/갱신 호출자 | 32자 이상 내부 갱신 API token; 배포 측에서 생성/주입 |
| `SOOP_CHROMEDRIVER` | cookie-auth만 | 설치한 ChromeDriver 절대 경로 |
| `SOOP_CHROME_BINARY` | cookie-auth만, 선택 | 브라우저 자동 발견이 안 될 때 executable 경로 |
| `SOOP_COOKIE_BIND` | cookie-auth만, 선택 | 기본 `127.0.0.1`; Compose 내부 연결은 배포 측에서 설정 |
| `SOOP_COOKIE_PORT` | cookie-auth만, 선택 | 기본 `8091` |
| `SOOP_COOKIE_API_URL` | refresh 명령, 선택 | 기본 `http://127.0.0.1:8091` |

ID/PW는 프로세스 환경변수 또는 `ROLE_ENV_FILE`로 주입한다. 정식 배포는 Secrets Manager에서
복원한 역할 파일을 mount하고 프로세스 안에서 읽는다. Docker `env_file`로 ID/PW를 노출하지 않는다.
실제 값을 코드·`.env.example`·명령 인자에 적지 않는다.
토큰·경로·브라우저 설정은 배포 측이 준비하며 수집 프로세스에는 ID/PW를 전달할 필요가 없다.

```sh
python3 -m venv /tmp/rogi-cookie-venv
/tmp/rogi-cookie-venv/bin/pip install -r cookie-auth/requirements.txt
# 위 환경 변수들이 운영 환경에 주입된 뒤 실행
/tmp/rogi-cookie-venv/bin/python cookie-auth/service.py serve
```

다른 프로세스에서 요청할 때는 API URL/token만 있으면 된다. 이 명령은 쿠키 원문을 출력하지 않는다.

```sh
python3 cookie-auth/service.py refresh
```

API는 같은 collector 호스트 안에서만 사용한다. 공개 포트나 주루마블 브라우저에 노출하지 않는다.
`GET /healthz`는 프로세스 생존만, 인증된 `GET /v1/status`는 갱신 상태를 제공한다.
`POST /v1/refresh`는 `Authorization: Bearer <token>`과 빈 body를 받으며 실패 시 503을 반환한다.
쿠키를 다운로드하는 API는 없다. 권한을 맞춘 공유 디렉터리에서 cookie-auth는 쓰기, discover/worker는 읽기만 허용한다.
파일 하나를 bind mount하면 atomic rename 뒤 옛 inode를 계속 볼 수 있으므로 **디렉터리를 mount**한다.
0600 파일을 읽을 수 있도록 해당 역할의 실행 UID를 맞춘다. query/브라우저에는 mount하지 않는다.

## 검증과 남은 범위

```sh
/tmp/rogi-cookie-venv/bin/python -m unittest discover -s cookie-auth -v
go test -race ./shared/soopauth/... ./discover/... ./worker/...
```

합성 로그인 결과로 일일/수동 갱신, 동시 요청, 재시작, 실패 시 보존, API 인증, 원자 저장을 검증한다.
실제 Python 저장물→Go reader, 기존 discovery live-check와 connector factory의 쿠키 연결도 검사한다.
테스트는 실제 로그인·브라우저 실행·방송 접속을 하지 않는다.

2026-09-21 EC2에서 실제 계정 로그인·요청 갱신, 다른 연령제한 방송의 player 응답과 WebSocket 입장·채팅을 확인했다.
정식 배포 재부팅 뒤 실제 갱신도 성공했다. 후로기 본인 방송의 수신은 방송이 켜질 때 확인해야 한다.
세부 범위는 [검증 기록](../docs/implementation-status.md)을 따른다.

로그인 성공은 페이지 전환과 인증/계정 쿠키를 확인한 결과이며, 모든 방송의 성인 인증·접근 성공을 보장하지 않는다.
CAPTCHA·추가 인증·성인 확인이 필요하면 상태를 확인해야 한다. 우회하거나 인증 쿠키를 만들어내지 않는다.
Go 연결 코드는 HTTP 200만으로 성공을 판단하지 않고 player 응답의 인증 거절/방송 종료를 구분하며,
요청한 WebSocket join의 성공 응답을 확인해야 연결 상태로 전환한다.

설계 근거와 원본 비교는 [조사 기록](../docs/cookie-acquisition-review.md)에 있다.
