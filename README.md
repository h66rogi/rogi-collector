# rogi-collector · rogimarble 방송 입력 수집기

첫 제품은 **후로기(`h66rogi`)의 SOOP 연령제한 방송을 감지하고, 주입한 로그인 쿠키로 채팅·별풍선을 수집하는 수집기**입니다.
목적은 [주루마블](https://github.com/h66rogi/rogimarble)의 후원 기반 게임 진행과 후원자 채팅 선택을 연결하는 것입니다.
공개 chat-collector는 이 도구를 구현하는 기본 코드베이스로 사용합니다.

현재 작업은 **기존 공개 collector의 실제 코드와 전체 구조를 가져온 출발점**입니다.
`discover / coordinator / worker / query / shared / proto` 및 원본 보조 모듈을 그대로 유지하고,
기존 manager·connector·pipeline·store·query를 수정해 최종 설계에 맞춥니다.
참고만 하여 별도로 작성했던 runtime은 사용자 요청으로 완전히 폐기했습니다.

공개 원본 151개 파일의 경로·고정 커밋·변경 내용은 [반입 기록](docs/source-imports.md)에 있습니다.
현재 등록 SOOP 채널만 단일 live-check로 확인하도록 entrypoint를 제한했습니다. 등록이 없으면 플랫폼 요청도 없습니다.
[쿠키 획득 컴포넌트](cookie-auth/README.md)는 ID/PW 로그인·일일/요청 갱신·저장을 제공하고 discover/worker에 연결했습니다.
EC2에서 실제 SOOP 로그인·쿠키 갱신·연령제한 방송 접속과 채팅/후원을 확인했습니다. 후로기는 방송 대기 상태여서 다른 켜진 방송을 사용했습니다.
후원 journal/spool·구독/ACK·mTLS collector v1 RPC를 기존 코드에 연결하고 격리 PostgreSQL/Redis 및 TLS 통신으로 검증했습니다. 주루마블 실제 inbox/게임 통합은 남아 있습니다.
기존 최근 채팅 RPC/Redis stream은 후원 내구성 구현이 아닙니다.

- [최종 배포·소비 계약 v1](docs/deployment-design.md): 단일 EC2·Compose·IaC, 내부 gRPC, 후원 journal/replay와 복구
- [후로기 방송용 구현 계획 v3](docs/implementation-plan.md): 쿠키로 방송 연결 → 자동 수집 → 주루마블 전달 → 방송 운영 검증
- [기존 저장소 조사](docs/repository-review.md): 원본 코드 조사와 선별 이식 후보
- [소스 이식 정책](docs/source-import-policy.md)
- [운영 Compose 배포](docs/deployment-production.md) · [인프라 준비](docs/infrastructure-preparation.md)

주사위, 보드, 후원 개수별 게임 규칙은 소비 제품의 책임입니다.
수집기에는 주루마블 게임 로직을 포함하지 않습니다.

자체 EC2 1대에서 PostgreSQL·Redis와 함께 실행하고, 다른 제품에는 인증된 내부 gRPC를 제공합니다.
## 코드 검증

Go 1.27, Node 22.18 이상 23 미만, npm 10을 사용합니다. 다음 명령은 반입한 대상 코드에서 실행합니다.
참고 저장소의 앱을 실행하지 않습니다.

```sh
npm ci
npm run generate
make source-check
make build
make test
make race
```

원본 Go/TypeScript 계약과 합성 HTTP/WebSocket/miniredis 회귀 테스트를 포함합니다.
실제 PostgreSQL/Redis 서버·SOOP 방송·rogimarble 통합·운영 배포 검증과는 구분합니다.

## 배포 코드와의 연결

정식 `soop-single-channel` 배포는 실제 역할 이미지·cookie-auth·앱 migration·mTLS·재부팅 secret 복원을 포함합니다.
[정식 배포 설명](docs/deployment-production.md)과 [실제 적용 기록](docs/implementation-status.md)을 구분해 확인합니다.
검증용 별도 배포/DB는 정식 데이터와 합치지 않습니다.
