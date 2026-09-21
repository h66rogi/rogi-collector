# rogi-collector · rogimarble 방송 입력 수집기

후로기(`h66rogi`)의 SOOP 연령제한 방송을 감지하고, 로그인 쿠키로 그 방의 채팅·별풍선을 수집합니다.
[주루마블](https://github.com/h66rogi/rogimarble)의 후원 기반 게임 진행과 후원자 채팅 선택에 필요한 입력을 제공합니다.
주사위·보드·후원 개수별 규칙·게임 세션 판정은 주루마블이 담당합니다.

공개 chat-collector의 실제 코드와 8개 모듈을 가져온 뒤 기존 discover → coordinator → worker → query를 확장했습니다.
`shared / proto / cleanup / chat-exporter`도 보존하며, cleanup·chat-exporter와 원본 광역 조회/관리 API는 첫 제품에서 실행하지 않습니다.
151개 반입 파일의 출처·변경 사유·해시는 [반입 기록](docs/source-imports.md)에서 확인할 수 있습니다.

## 현재 상태

2026-09-21 기준 `soop-single-channel` 정식 배포를 완료했습니다.
독립 EC2의 PostgreSQL·Redis·Go 역할 4개·cookie-auth가 실행되며, 주루마블 EC2는 private 7443 mTLS로 연결합니다.
재부팅 후 서비스·비밀 설정·쿠키 복원, 서버 인증서 교체 후 재접속, 실제 쿠키 갱신과 DB 백업/S3 업로드를 확인했습니다.

다른 공개/연령제한 방송으로 채팅 수신과 실제 관측 후원의 파일 inbox 저장·ACK·재전송을 검증했습니다.
정식 대상은 `h66rogi`만이며 검증 당시 방송 대기 상태였습니다. **후로기 본인 실방송 확인과 주루마블 실제 DB inbox·게임 연동은 남아 있습니다.**
백업 생성과 복원 성공도 별개입니다. 배포 버전과 검증 범위는 [구현·검증 상태](docs/implementation-status.md)에 기록합니다.

## 입력과 전달

- cookie-auth만 SOOP ID/PW를 받아 마지막 성공 후 24시간마다 또는 인증된 요청에 따라 쿠키를 갱신합니다.
  discover/worker는 공유 snapshot을 읽습니다. 실제 비밀 값은 저장소에 넣지 않습니다.
- 등록된 SOOP 채널 하나만 감지·할당·접속합니다. 대상이 미설정이면 플랫폼 요청을 하지 않습니다.
  방송 종료·쿠키 문제·조회 실패·재연결·저장 장애를 구분합니다.
- 별풍선은 native 정수 개수와 관측 ID를 보존해 disk spool·PostgreSQL journal/outbox에 저장합니다.
  정상적인 동일 개수 후원 두 건을 raw hash로 합치지 않습니다.
- collector v1의 7개 RPC로 상태·후원 재생/구독/ACK·최근 채팅·수집 설정·복구를 제공합니다.
  소비자는 자신의 inbox와 cursor를 함께 저장한 뒤 ACK해야 합니다.
- 일반 채팅은 Redis에 최대 24시간·채널당 1만 건을 보관합니다. 후원과 같은 영속 재생 보장은 없습니다.

## 문서 안내

| 목적 | 문서 |
| --- | --- |
| 제품 경계와 전달 계약 | [최종 배포·소비 설계](docs/deployment-design.md) |
| 구현 단계와 남은 제품 작업 | [구현 계획](docs/implementation-plan.md) · [검증 상태](docs/implementation-status.md) |
| 역할별 설정·스키마·소비자 계약 | [코드 인계](docs/collector-code-handoff.md) · [쿠키 컴포넌트](cookie-auth/README.md) |
| 정식 배포·인증서·재부팅 | [운영 배포](docs/deployment-production.md) · [CI/CD](docs/deployment-ci.md) |
| 인프라·상태 확인 | [인프라 준비](docs/infrastructure-preparation.md) · [모니터링](docs/operations-monitoring.md) |
| 출처와 초기 조사 | [이식 정책](docs/source-import-policy.md) · [원본 조사](docs/repository-review.md) · [쿠키 조사](docs/cookie-acquisition-review.md) |

`docs/upstream/`은 수정하지 않은 원본 참고 문서입니다. 현재 제품의 실행 안내는 위 문서를 따릅니다.
`deploy/compose.yaml`은 초기 health-only 골격으로 현재 앱 실행용이 아닙니다.
정식 실행은 `deploy/compose.production.yaml`, 격리 실방송 검증의 재현 절차는 [live-check 문서](deploy/live-check/README.md)를 사용합니다.

## 코드 검증

Go 1.27, Node 22.18 이상 23 미만, npm 10을 사용합니다. CI의 정확한 버전은
[workflow](.github/workflows/ci.yml)에 고정합니다. 참고 저장소의 앱은 실행하지 않습니다.

```sh
npm ci
make source-check
make build
make test
make race
./tools/ops/test.sh
python3 tools/ops/test_rotate_server_tls.py
```

`make test/race`는 합성 Go 테스트와 Go↔TypeScript 계약 검사를 실행하며 외부 저장소를 요구하지 않습니다.
proto를 수정한 경우 `npm run generate`로 생성물을 갱신한 뒤 계약 검사를 실행합니다.
쿠키 검사는 [Selenium Python 환경](cookie-auth/README.md)을 준비한 뒤 `make cookie-test PYTHON=/path/to/python`으로 실행합니다.
실제 저장소 검사는 격리된 `COLLECTOR_TEST_DATABASE_URL`, `COLLECTOR_TEST_REDIS_ADDR`을 명시하고 `make integration-test`로 실행합니다.
로컬 검사 통과를 실방송·게임 연동·운영 배포 성공으로 해석하지 않습니다.
