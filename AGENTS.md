# 프로젝트 작업 원칙

- `docs/deployment-design.md`, `docs/implementation-plan.md`, `docs/source-import-policy.md`를 읽는다. 배포·전달은 deployment-design이 우선한다.
- EC2 1대의 독립 Compose·PostgreSQL·Redis로 실행한다. 외부 소비자는 DB/Redis에 직접 연결하지 않고 내부 인증 gRPC를 사용한다.
- 로기챗은 공통 수집기다. 주사위/게임/주루마블 설정을 수집기 안에 넣지 않는다.
- 공개 `meloming-chat-collector`를 일차 재사용 후보로 삼고 private service와 비교한다.
- 파일별로 선별한다. private 원본 전체 복사·커밋·이력 병합을 하지 않는다.
- source LICENSE/NOTICE와 고지를 보존한다. 실제 운영 값과 실사용자 fixture는 가져오지 않는다.
- SOOP의 native 별풍선 개수와 후원 종류를 명시적으로 보존한다. 원화 환산으로 덮어쓰지 않는다.
- 연결별 sequence만으로 전역 event ID를 만들지 않는다. 재발행 시 event ID가 바뀌면 안 된다.
- 일반 채팅의 bounded/drop 정책을 후원 경로에 그대로 적용하지 않는다.
- 원천 ID 없는 동일 raw 패킷을 무조건 중복으로 합치지 않는다.
- 수집 대상 미설정 시 전체 플랫폼/전체 채널 수집으로 확장하지 않는다.
- 합성 fixture와 격리된 저장소로 검증하고, 실시간 플랫폼 검증 여부를 별도로 보고한다.
