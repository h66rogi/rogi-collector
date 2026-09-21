# rogi-collector · 로기챗 공통 수집기

SOOP 방송 상태, 채팅, 후원을 수집·정규화하고 여러 제품에 제공하는 공통 수집기입니다.
첫 소비자는 [주루마블](https://github.com/h66rogi/rogimarble)입니다.

현재 단계는 독립 실행 기반입니다. 역할별 Go 프로세스와 PostgreSQL·Redis migration을 로컬 Compose로
실행할 수 있지만, 모든 역할은 등록 채널 0개로 시작하며 SOOP 연결·journal·gRPC RPC는 구현 전입니다.
프로세스 health는 실제 수집 성공을 뜻하지 않습니다.
로컬 Compose는 사용자 요청으로 중지했으며 DB volume을 보존했습니다. 현재 EC2 배포 파일을 준비 중이고 외부 서버는 아직 생성하지 않았습니다.

- [최종 배포·소비 계약 v1](docs/deployment-design.md): 단일 EC2·Compose·IaC, 내부 gRPC, 후원 journal/replay와 복구
- [수집기 구현 계획 v1](docs/implementation-plan.md): 공통 작업 ID·의존성·산출물·장애 검증·운영 순서
- [기존 저장소 조사](docs/repository-review.md): 원본 코드 조사와 선별 이식 후보
- [소스 이식 정책](docs/source-import-policy.md)
- [운영 Compose 배포](docs/deployment-production.md) · [인프라 준비](docs/infrastructure-preparation.md)

주사위, 보드, 후원 개수별 게임 규칙은 소비 제품의 책임입니다.
수집기에는 주루마블 게임 로직을 포함하지 않습니다.

자체 EC2 1대에서 PostgreSQL·Redis와 함께 실행하고, 다른 제품에는 인증된 내부 gRPC를 제공합니다.
## 로컬 기반 실행

Go 1.27, Node 22.18 이상 23 미만, npm 10, Docker Compose가 필요합니다. 계약 도구 bootstrap에는
Git, curl, unzip이 필요하고 로컬 비밀 생성에는 OpenSSL을 사용합니다. fresh checkout에서는 먼저 고정된
Node 의존성을 설치하고 계약을 생성합니다. `npm run generate`가 repo-local protoc와 Go plugin을 내려받거나
기존 설치의 checksum·버전을 검증합니다.

```sh
npm ci
npm run generate

umask 077
sed "s/replace-with-local-generated-value/$(openssl rand -hex 24)/" deploy/.env.example > deploy/.env
make build
make test
make race
docker compose --env-file deploy/.env -f deploy/compose.yaml up --build --wait
docker compose --env-file deploy/.env -f deploy/compose.yaml ps
```

종료할 때 named volume을 보존합니다.

```sh
docker compose --env-file deploy/.env -f deploy/compose.yaml down --remove-orphans
```

`down -v`와 volume prune은 사용하지 않습니다. proto v1은 고정 도구로 Go/TypeScript bundle을 생성하고
동일 protobuf wire의 교차 round-trip을 검증합니다. 자세한 상태는 [구현 상태](docs/implementation-status.md)를 봅니다.
