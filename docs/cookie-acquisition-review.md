# 쿠키 획득 컴포넌트 조사와 반영

이 문서는 조사 당시의 근거다. 현재 실행 방법은 [컴포넌트 README](../cookie-auth/README.md),
실제 로그인·연령제한 방송·정식 배포 검증은 [구현 상태](implementation-status.md)를 따른다.

조사: 2026-09-21. 사용자 지정 참고는 `dylabs/tongnamu_temp`이며
고정 SHA는 `23f75cc92e1a04606c658318d20cbfeee6f6291f`다.
접근 가능한 private 저장소로 확인했고, 원본 clone은 작업 레포 밖에 보관했다.
명시적인 라이선스 파일/메타데이터는 확인되지 않았다. 원본 파일·Git 이력·운영 값·binary는 반입하지 않았다.
기존 공개 collector 기반은 유지하고, 새로 요청한 쿠키 컴포넌트만 작성했다.

## 확인한 흐름

| 확인한 원본 | 동작 | rogimarble에 적용한 판단 |
| --- | --- | --- |
| `login.py`의 로그인 처리 | Selenium으로 SOOP 로그인 페이지의 ID/PW 입력 후 submit, 쿠키 추출 | 브라우저 로그인 경로를 참고하되 환경변수 입력과 명시적 성공 확인을 추가 |
| 같은 파일의 Redis 저장/조회 | 계정별 쿠키 JSON 저장, force login 또는 기존 값 재사용 | 한 계정용 snapshot으로 단순화; 기존 Go 프로세스는 비밀번호 없이 파일을 읽음 |
| 같은 파일의 TTL | 코드상 14일; 인접 주석의 설명과 불일치 | 서버 쿠키 만료와 24시간 재로그인 주기를 별개로 관리 |
| `driver/load.py` | headless Chrome과 driver 로딩/fallback | 설치된 browser/driver 경로를 입력받음; 로그인마다 새 profile, 종료/시간 제한 |
| `main.py` | 로그인 계정과 방송 대상 계정을 따로 받고 쿠키를 방송 페이지에 주입 | 로그인 ID와 `h66rogi` 대상 채널을 분리 |
| 쿠키 domain 처리 | 특정 SOOP domain 쿠키만 주입 | domain/path/expiry를 보존하고 맞는 player API 요청에만 전송; `.co.kr`와 `.com`을 임의 치환하지 않음 |
| `live_checker.py` 및 cron 관련 파일 | 방송 상태/기존 배포 replica 제어 | 원본 discover가 이미 담당하므로 해당 배포/스케일링 기능은 가져오지 않음 |
| `review.py`, `starballon.py`, 기타 실행 코드 | 별도 조회/재생 등 목적과 운영 데이터 포함 | 이번 컴포넌트에 불필요하며 반입하지 않음 |

로그인 코드에는 일일 갱신 서비스나 외부 갱신 요청 endpoint가 없다. 고정 sleep 뒤 쿠키를 저장하므로
로그인 거절/추가 인증 상태도 저장할 수 있다. 이번에는 새 브라우저의 페이지 전환과 인증/계정 쿠키를 확인한 뒤 저장한다.
원본의 쿠키 원문 출력과 운영 값은 신규 코드·문서·fixture에 사용하지 않는다.
성인 인증·실제 방송 접근 확인을 수행하는 근거는 원본의 이 로그인 절차만으로 확보되지 않는다.

## 추가한 코드

- `cookie-auth/service.py`: 환경변수 로그인, 24시간 주기, 인증된 요청 갱신, 동시 요청 합치기, 원자 저장, 상태.
- `shared/soopauth`: snapshot 재읽기, domain/path/expiry 검사, 인증 쿠키가 없으면 실패, 고정 player endpoint 적용.
- 기존 discover SOOP 생성자와 worker connector/factory/entrypoint: 새 cookie file 입력 연결.
- 합성 테스트: 원본을 실행하지 않고 새 코드의 저장/갱신/실패/소비 경로를 검사.

Python 컴포넌트는 새로 작성했으며 private 원본을 복제/개작해 반입한 파일은 없다.
공개 collector의 수정 파일은 기존 source manifest에 변경 사유·현재 해시를 추가한다.
원본 8개 Go 모듈·lifecycle·manager는 유지했다. 조사/초기 컴포넌트 작성 단계에서는 CI/CD 파일을 변경하지 않았으며, 후속 배포 단계에서 정식 실행 경로를 연결했다.

Selenium 사용 방식은 [명시적 대기 문서](https://www.selenium.dev/documentation/webdriver/waits/)와
[쿠키 API 문서](https://www.selenium.dev/documentation/webdriver/interactions/cookies/)도 확인했다.
정적 조사와 합성 테스트는 실제 SOOP 로그인/연령제한 방송 검증과 구분한다.


## player 응답과 현재 endpoint 보완

2026-09-21에 공개 [Streamlink SOOP adapter](https://github.com/streamlink/streamlink/blob/master/src/streamlink/plugins/soop.py)의
`.com` player endpoint와 RESULT=1/로그인 필요 -6 사용을 확인했다.
[soop4j](https://github.com/zzik2/soop4j)의 문서에서 RESULT=0 방송 종료 구분도 대조했다.
이 근거로 기본 login/player URL을 `.com`으로 맞추고, 0/1/-6 외의 응답과 빈 CHATNO를 성공이나 종료로 추정하지 않는다.
쿠키 domain은 그대로 검사하며 도메인 간 쿠키 값 복제는 하지 않는다.
이는 당시 공개 구현을 대조한 정적 근거다. 이후 다른 연령제한 방송에서 익명 -6/쿠키 1과 join·채팅을 실증했다. 관측하지 않은 응답까지 플랫폼 계약으로 확정하지 않는다.
