# 구현 상태

기준일: 2026-09-21

| Gate | 상태 | 현재 증거 | 남은 조건 |
| --- | --- | --- | --- |
| P01 선별·공개 준비 | 부분 완료 | 외부 구현 직접 반입 없음, `docs/source-imports.md` 기록, 공개 후보 비밀 패턴 검사 | 실제 이식 시 파일별 SHA·allowlist·LICENSE/NOTICE·변경 기록 |
| P02 workspace·로컬 기반 | 부분 완료 | 역할별 Go module/workspace build, unit/race, PG/Redis migration, 격리 Compose 실제 health | clean checkout 재검증, 역할별 실제 저장소 연결, 운영 toolchain checksum/digest |
| P03 공통 계약 | 계약 생성·교차 검증 완료 | 고정 protoc/Go/TS generator, v1 generated bundle·manifest checksum, Go↔TS deterministic wire와 null/uint64 max/unknown kind/additive field 보존 검사 | 소비 제품 반입은 별도 통합 작업; RPC 구현은 C03 |
| C01 등록·연결 | 미착수 | 프로세스 health가 등록 채널 0과 collection inactive를 표시 | 구독·lease·connector·합성 SOOP 입력 |
| C02 journal·spool | 미착수 | 없음 | PostgreSQL journal/outbox, durable spool, fencing |
| C03 query RPC | 미착수 | health에 `rpcImplementation=unimplemented` 표시 | 인증 gRPC, replay/watch/ACK/recovery 구현 |
| 운영 배포 | 준비 중 | 운영 Compose, manifest·mount·secret 검사, systemd와 EC2 Terraform 구현/리뷰 | 실제 이미지 발행·EC2 적용·부팅/복구·백업 검증 |

사용자 요청으로 로컬 Compose를 중지했고 named DB volume은 보존했다. 신규 EC2 두 대 중 수집기 전용 한 대를
Mac mini의 AWS 자격으로 배포하도록 준비하고 있다. AWS 리소스/DNS 변경과 실제 외부 배포는 아직 하지 않았다.

현재 Go와 TypeScript 테스트는 generated protobuf 타입과 동일 binary fixture로 wire 호환성을 검증한다.
JSON 소비 경계에서는 generated TypeScript bigint를 문자열로 변환해야 하며 이를 API schema에서 별도로 고정해야 한다.
proto v1은 버전 1.0.0 생성 bundle이다. 이후 호환 필드는 additive하게 추가하고 breaking change는 새 major로 분리한다.

## P03 재현·검증 명령

```sh
npm ci
npm run generate
npm run contracts:test
make test
make race
```

fresh checkout에는 Node `>=22.18.0 <23`, npm 10, Go 1.27과 Git/curl/unzip이 필요하다. `npm ci`가
lockfile 그대로 TypeScript generator/runtime을 설치하고 `npm run generate`가 protoc 33.0과
protoc-gen-go 1.36.10을 repo-local `.tools`에 준비·검증한다. 그 뒤 `make test`와 `make race`가
fixture를 스스로 다시 만들므로 이전 실행의 ignored fixture에 의존하지 않는다.

`contracts:test`는 TypeScript가 만든 donation/recovery wire를 Go에서 읽고 다시 deterministic encode하며,
Go가 만든 donation wire를 TypeScript에서 같은 bytes로 확인한다. `uint64` 최대값은 양쪽 모두 bigint/uint64로
유지하고 JavaScript `Number`로 변환하지 않는다. nullable `occurredAt`·source ID, unknown donation kind,
필수 `observedAt`, native count의 양수 JS safe-integer 범위와 additive unknown field 보존을 검사한다.
또한 generated type 기반 validator가 event/channel/platform/currency/donor/connection/generation/identity 필수값과
timestamp 유효성을 양쪽에서 같은 정책으로 거부한다. serialization 호환 검사와 의미 검증은 별도 테스트다.
manifest provenance 테스트는 임시 Git 저장소에서 clean HEAD, staged dirty, unstaged dirty와 반복 생성을 확인한다.
