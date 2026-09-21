# 공개 수집기 코드 반입 기록

2026-09-21 사용자 확인: 기존 코드를 참고한 별도 재구현을 폐기하고, 기존 코드와 전체 구조를 가져온 뒤 수정하는 방식으로 다시 시작했다.
폐기한 `codex/collector-runtime` 브랜치와 그 작업 폴더는 제거했다. 그 구현 파일은 이 브랜치에 사용하지 않았다.

## 출발점

- 원본: `dylabs/meloming-chat-collector`, 공개 저장소.
- 고정 커밋: `85b8aa1dd9284ded8717c97dc6aee556b5bcf70a`.
- 원본의 `discover`, `coordinator`, `worker`, `query`, `shared`, `proto`, `cleanup`, `chat-exporter` 구조와 구현을 유지했다.
- 151개 파일을 경로별 허용 목록으로 반입했다. 목록에 없는 디렉터리/파일을 통째로 복사하지 않았다.
- private `meloming-chat-service`는 차이 확인 자료이며 반입하지 않았다. Git 이력·원본 CI·Dockerfile·운영 설정도 반입하지 않았다.
- 공개 원본의 LICENSE(AGPL-3.0-only), NOTICE, DISCLAIMER.md를 원문 그대로 보존했다.
- JSON fixture는 원본에 이미 포함된 synthetic/example.invalid 데이터임을 확인했다.

파일별 원본/대상 경로, 원본 SHA-256, 모듈 주소 치환 직후 SHA-256, 현재 검토 SHA-256과 추가 수정 사유는
[source-import-manifest.json](source-import-manifest.json)에 기록한다.
`make source-check`는 현재 코드와 기록의 일치를 검사한다. `--reference`로 고정 원본까지 읽기 전용 대조할 수 있다.

## 변경 원칙

기존 package, 타입, 인터페이스, manager lifecycle, connector, pipeline, lease/leader, store, query의 틀을 유지한다.
`github.com/dylabs/meloming-chat-collector` 모듈 주소만 대상 저장소 주소로 치환한다.
원본 query/admin의 공개 protobuf package는 호환성 기준으로 유지하고, go_package만 대상 주소로 바꿔 재생성한다.
기존 `rogi.collector.v1` 계약은 별도 계약으로 보존하며, 원본 query에 이미 구현됐다고 간주하지 않는다.

기존 대상의 `cmd/*`, `pkg/shared` health-only 골격은 원본의 역할별 `<role>/cmd`, `shared` 모듈로 교체한다.
후속 배포 요청으로 신규 `deploy/live-check` Compose/Dockerfile을 작성했다. 원본 운영 설정은 가져오지 않았다. 변경된 빌드/실행/마이그레이션 경로는
[코드 인계 문서](collector-code-handoff.md)에 기록한다.

원본 코드의 일반 채팅/다중 플랫폼/ClickHouse 기능은 출발점 보존을 위해 남겨 두되 첫 SOOP 제품 경로와 구분한다.
전체 플랫폼 수집을 기본 동작으로 사용하지 않는다. 후원 journal/spool/ACK, 채널별 consumer 권한은
기존 클래스의 확장으로 구현해야 하며, 원본의 최근 채팅 저장을 후원 내구성으로 표시하지 않는다.

## 검증 경계

반입한 대상 코드에서 build/unit/race와 기존 Go↔TypeScript wire 테스트를 수행한다.
참고 저장소의 빌드나 개발 서버는 실행하지 않는다. SOOP 실접속, 실금전 후원, 운영 DB 변경,
배포 성공은 이 검증에 포함하지 않는다. 실행 결과는 [현재 상태](implementation-status.md)에 기록한다.

## 쿠키 컴포넌트 추가

사용자 지정 private `tongnamu_temp`의 로그인/쿠키 흐름을 읽고 [조사 기록](cookie-acquisition-review.md)을 남겼다.
그 파일·운영 값·이력은 반입하지 않았다. 새 `cookie-auth`와 `shared/soopauth`는 이 작업의 신규 코드다.
공개 collector의 SOOP 생성자·factory·entrypoint에 JSON cookie file 입력만 연결하고 source manifest를 갱신했다.
