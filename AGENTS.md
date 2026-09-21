# 프로젝트 작업 원칙

- 사용자 확정 출발점은 **기존 공개 collector의 코드와 디렉터리 구조를 가져온 뒤 수정하는 방식**이다. 참고만 하고 별도 runtime/manager/store로 재구현하지 않는다. 원본 8개 모듈과 기존 lifecycle/인터페이스를 기반으로 확장한다.

- 첫 제품 대상은 후로기(`h66rogi`) 한 채널과 주루마블 소비자 하나다. 연령제한 방송용 쿠키를 discover/worker에 주입하고 실제 연결·채팅 수신을 먼저 확인한다. 범용 다채널/다소비자 관리 구축을 선행하지 않는다. 실제 쿠키는 코드·로그·fixture에 넣지 않는다.

- `docs/deployment-design.md`, `docs/implementation-plan.md`, `docs/source-import-policy.md`를 읽는다. 배포·전달은 deployment-design이 우선한다.
- EC2 1대의 독립 Compose·PostgreSQL·Redis로 실행한다. 외부 소비자는 DB/Redis에 직접 연결하지 않고 내부 인증 gRPC를 사용한다.
- 이 도구의 목적은 rogimarble의 방송 진행에 필요한 후원·채팅 입력을 제공하는 것이다. chat-collector는 기본 코드베이스다. 기능 우선순위와 완료 기준을 후원→게임 진행·후원자 채팅→목적지 선택·운영자 대응으로 정한다. 게임 규칙/세션 판정 자체는 rogimarble에 둔다.
- 공개 `meloming-chat-collector`를 일차 재사용 후보로 삼고 private service와 비교한다.
- 파일별로 선별한다. private 원본 전체 복사·커밋·이력 병합을 하지 않는다.
- source LICENSE/NOTICE와 고지를 보존한다. 실제 운영 값과 실사용자 fixture는 가져오지 않는다.
- SOOP의 native 별풍선 개수와 후원 종류를 명시적으로 보존한다. 원화 환산으로 덮어쓰지 않는다.
- 연결별 sequence만으로 전역 event ID를 만들지 않는다. 재발행 시 event ID가 바뀌면 안 된다.
- 일반 채팅의 bounded/drop 정책을 후원 경로에 그대로 적용하지 않는다.
- 원천 ID 없는 동일 raw 패킷을 무조건 중복으로 합치지 않는다.
- 수집 대상 미설정 시 전체 플랫폼/전체 채널 수집으로 확장하지 않는다.
- 합성 fixture와 격리된 저장소로 검증하고, 실시간 플랫폼 검증 여부를 별도로 보고한다.
