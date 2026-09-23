# rogi-collector

SOOP `h66rogi` 방송을 감지하고 채팅과 별풍선 후원을 수집하는 Go 서비스입니다. 주루마블에는 private mTLS gRPC로 후원과 채팅을 전달하고, 공개 읽기 API는 [data-api.rogi.chat](https://data-api.rogi.chat/v1/broadcasts/current)에서 제공합니다. API 사용법과 응답 계약은 [docs.rogi.chat](https://docs.rogi.chat/)을 참고하세요.

## 데이터 흐름

```text
SOOP → discover → coordinator → worker
                               ├→ 후원 journal / outbox (PostgreSQL) → private query gRPC → 주루마블
                               ├→ 최근 채팅 (Redis) → data-api HTTP / WebSocket
                               └→ 채팅 spool → PostgreSQL → Cloudflare R2 → data-api 과거 조회

인터넷 → Cloudflare Tunnel → data-api (Go)
```

수집 대상은 `h66rogi` 한 채널로 제한합니다. 별풍선은 원천의 정수 개수와 종류를 보존하고, 소비자가 자신의 inbox와 cursor를 저장한 후 ACK하는 전달 계약을 사용합니다. 최근 채팅은 Redis에서 최대 24시간·10,000개 stream 항목 범위로 읽습니다. 채팅 archive에는 시간 기준 만료를 두지 않으며, 조회 응답의 `complete=false`는 전체 방송 채팅의 무누락 수집을 보증하지 않는다는 뜻입니다.

공개 API는 방송 상태, 최근 채팅, WebSocket 실시간 이벤트, 방송별 보관 채팅을 읽기 전용으로 제공합니다. 공개 data-api의 PostgreSQL 계정과 R2 자격 증명은 읽기 권한으로 제한합니다. SOOP 로그인 정보, 내부 gRPC, PostgreSQL, Redis, R2 bucket은 공개 API로 노출하지 않습니다.

## 저장소 구성

| 경로 | 역할 |
| --- | --- |
| `discover/`, `coordinator/`, `worker/` | 방송 탐지·할당·수집 |
| `query/` | private gRPC, 공개 Go HTTP/WebSocket, archive exporter |
| `shared/`, `proto/` | 공통 저장소·계약·마이그레이션 |
| `cookie-auth/` | 로그인 쿠키 갱신 |
| `deploy/`, `infrastructure/` | Compose 배포와 인프라 정의 |

이 저장소는 공개 chat-collector의 코드를 선별 반입해 확장했습니다. 파일별 출처와 변경 내용은 [반입 기록](docs/source-imports.md), 라이선스는 [NOTICE](NOTICE)에서 확인할 수 있습니다.

## 개발과 검증

Go 1.27, Node.js 22.18 이상 23 미만, npm 10을 사용합니다. Node 버전은 [CI 설정](.github/workflows/ci.yml)에 고정돼 있습니다.

```sh
npm ci
make source-check
make build
make test
make race
./tools/ops/test.sh
python3 tools/ops/test_rotate_server_tls.py
```

proto를 수정하면 `npm run generate`로 생성물을 갱신하세요. 쿠키 컴포넌트 테스트와 격리된 PostgreSQL·Redis 통합 테스트의 준비 방법은 [코드 인계](docs/collector-code-handoff.md)를 참고하세요. 운영 비밀과 실제 채팅은 저장소에 추가하지 않습니다.

## 운영 문서

| 목적 | 문서 |
| --- | --- |
| 공개 HTTP/WebSocket 계약 | [API 문서](https://docs.rogi.chat/) · [OpenAPI](https://docs.rogi.chat/openapi.yaml) |
| 공개 API의 저장·권한 구조 | [공개 API 구조](docs/public-data-api-architecture.md) |
| private 소비자 계약 | [코드 인계](docs/collector-code-handoff.md) |
| 배포·복구 | [운영 배포](docs/deployment-production.md) · [CI/CD](docs/deployment-ci.md) |
| 상태·진단 | [방송 진단](docs/broadcast-diagnostics.md) · [모니터링](docs/operations-monitoring.md) |

`docs/upstream/`은 원본 프로젝트의 참고 문서입니다. 운영 Compose 설정은 `deploy/compose.production.yaml`에 있습니다.
