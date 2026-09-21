# Source import record

작성일: 2026-09-21

이번 P01~P03 기반 wave의 Go workspace, health skeleton, migration, Compose, proto v1과 합성 fixture는
설계 문서에서 신규 작성했다. 외부 참고 저장소의 구현 파일, Git 이력, private proto, 운영 값은 직접 반입하지 않았다.

`meloming-chat-collector`와 private `meloming-chat-service`는 후속 C01 후보를 파일별 비교할 대상이며,
이번 wave에서는 직접 이식 allowlist가 없다. 따라서 AGPL 코드 반입 또는 고지 복사는 발생하지 않았다.
Go와 TypeScript protobuf bundle은 저장소가 고정한 생성기로 만든다. 같은 합성 wire를 양방향으로 읽고 deterministic
bytes를 비교하며 nullable timestamp, uint64 최대값, unknown donation kind와 additive unknown field 보존을 검사한다.
manifest는 계약 1.0.0, schema checksum, 실제 tool versions와 source SHA를 기록하며 현재 미커밋 상태는 임의 SHA 대신
`uncommitted`로 표시한다. RPC 서버와 durable journal은 여전히 구현 전이며 P03 계약 검증은 실제 수집 성공을 뜻하지 않는다.
