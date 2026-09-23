# 공개 채팅 archive 저장 계약

상태: 구현 중 계약. SQL과 선택적 worker spool/DB writer는 draft PR에 있으며 운영에서 꺼져 있다.
R2 exporter, 영구 gap 기록, 과거 조회가 완료되기 전에는 보관 기능을 운영에서 켜지 않는다.
이 문서의 전체 절차는 현재 운영 기능을 뜻하지 않는다.
대상: SOOP `h66rogi` 한 채널. [상위 설계](public-data-api-design.md)의 첫 구현 단계다.

## 확인한 기존 동작

- worker는 connector의 `ChatMessage`를 받아 선택적인 ClickHouse writer에 enqueue한 다음 Redis에 발행한다.
- Redis product stream은 최대 10,000건, TTL 24시간이다. Redis에서 사라진 메시지는 복구할 수 없다.
- `broadcast_sessions`는 PostgreSQL에 있지만 discover의 `HISTORY_ENABLED=true`에서만 작성된다.
  `VIEWER_HISTORY_ENABLED=false`를 함께 주면 ClickHouse viewer writer 없이 PostgreSQL 세션
  기록만 사용할 수 있다. 기본값은 기존의 ClickHouse 동작을 유지한다.
- 현재 일반 `UpsertLiveChannels`는 `current_session_seq`를 증가시키지 않는다. archive를 켜려면
  PostgreSQL 세션 기록을 먼저 활성화하고, 실방송 시작·종료·재연결을 확인해야 한다.
- 현재 `WatchChat`의 cursor는 Redis generation과 stream ID다. 영구 보관의 수락 여부나 순서를
  나타내지 않는다.

## 세션과 event identity

`archive_sessions`는 공개용 무작위 UUID `session_id`를 기본키로 사용하고,
`(platform, channel_id, started_at, session_seq)`를 기존 `broadcast_sessions`의 고유 원천 키로
보관한다. 같은 원천 키를 다시 관측하면 같은 공개 ID를 반환한다. 원천 `broadcast_id`는
별도 선택 필드다. 페이지에 노출하는 `sessionId`는 이 UUID의 문자열이다.

writer는 connector의 채팅을 받았을 때 **그 시점에 유효한 열린 방송 세션**을 조회해 연결한다.
열린 세션이 없거나 모호하면 `session_id`를 추측하지 않는다. 채팅은 별도의 `unassigned`
격리 구간에 보관하고, API에는 보관 품질 결손으로 표시한다. 운영 검증 후에만 명시적인
재연결 절차로 세션에 붙인다. 방송 종료 직후 늦게 도착한 채팅은 연결 시점의 원천 상태와
수신 시각을 같이 보존하며 재분류 근거를 남긴다.

`event_id`는 connector가 부여한 ID를 원문 그대로 내부에 보존한다. 동일 세션의 동일
`event_id` 재시도만 멱등 처리한다. source ID가 없는 동일 내용의 채팅을 내용 해시만으로
합치지 않는다. 방송 내 순서는 PostgreSQL의 `position BIGINT GENERATED ALWAYS AS IDENTITY`
로 정한다. 이 값은 수집·저장 순서이지 플랫폼의 전역 순서가 아니다. 재연결 경계에서
순서가 불명확하면 품질 표시에 기록한다.

## 수락 경계와 데이터 최소화

worker는 `type=chat` 메시지만 archive writer에 전달한다. 후원·시스템 메시지와 원본 패킷은
공개 archive에 넣지 않는다. 원본 `userId`는 archive spool에 쓰기 전에 버전이 있는
비밀키로 `HMAC-SHA256(platform || 0x00 || channelId || 0x00 || userId)`를 계산해
가명 식별자로 바꾼다. display name과 메시지는 그대로 보존한다. 키 회전 후 가명은
달라질 수 있으므로 응답에 `userIdVersion`을 포함한다. 키와 원본 ID는 API 응답·로그·R2에
넣지 않는다.

worker의 내구성 수락 시점은 로컬 spool 파일과 디렉터리의 `fsync`가 끝난 순간이다.
메시지를 Redis에 발행하기 **전**에 수락을 시도한다. DB 장애 시 파일을 유지하고 다시
전송한다. spool은 단일 writer lock, 파일별 checksum, 크기·건수 상한을 갖는다. 재시도
중복은 DB의 영구 `archive_event_ids(session_id, event_id, position)` unique 제약으로 제거한다.
본문을 R2로 옮긴 뒤에도 이 작은 중복 방지 색인은 유지한다. 색인·백업 용량 증가율을
감시하고, 용량이 부족해지면 EBS를 증설한다. 기록 수락 실패나 spool
포화는 Redis 발행까지 중단시키지 않고 별도 gap/지표를 남긴다. 따라서 API는 그 구간을
완전한 archive라고 주장하지 않는다. 비밀키 부재나 spool 손상은 archive 기능 준비 실패다.

PostgreSQL hot 테이블은 `session_id`, `position`, `event_id`, `received_at`, 가명 ID와
버전, `display_name`, `message`만 담는다. spool과 DB가 위치한 EBS는 암호화돼 있지만
로컬 파일 모드와 Docker mount도 최소 권한으로 둔다. 운영 서비스가 보관 수락을 시작하기
전에 마이그레이션, 비공개 R2 bucket, bucket 범위의 S3 API 토큰, 재시도 경로를 모두 준비한다.

초안 worker는 `CHAT_ARCHIVE_ENABLED=true`일 때만 spool을 생성하고, 설정이 없으면 기존
Redis 경로만 실행한다. 이 스위치는 아직 운영 활성화 권한을 뜻하지 않는다. 현재 save 실패는
로그와 지표에 남지만 세션별 영구 gap 원장까지 보장하지 않으므로, 읽기 API는 완전성을
주장할 수 없다.

## R2 segment와 읽기

하나의 session에서 연속한 `position` 구간을 최대 1,000건의 gzip NDJSON 객체로 만든다.
객체 키는 `v1/soop/h66rogi/{sessionId}/{firstPosition}-{lastPosition}-{sha256}.jsonl.gz`
형태이며, 본문에는 공개 응답에 필요한 필드만 있다. bucket에는 자동 만료 lifecycle을
두지 않는다. 업로더는 객체 checksum·건수·첫/마지막 position을 검증하고
`archive_segments` 색인을 커밋한다. 색인 commit 전에는 hot 행을 지우지 않는다.
`archive_event_ids` 행도 지우지 않는다.
같은 구간의 재시도는 같은 키를 사용하고, 업로드만 끝난 orphan 객체는 정리 대상으로
기록한다. export가 지연되면 hot 행을 임의로 삭제하지 않고 용량 경보를 낸다.

페이지 cursor는 버전, session ID, 마지막 `position`을 서버가 서명한 불투명 값이다.
조회는 segment 색인과 hot 테이블을 같은 position 오름차순으로 병합한다. segment
이동 중에도 마지막 position 이후를 읽으므로 중복·누락이 생기지 않아야 한다. 첫 조회
결과의 `archiveStartedAt`, `complete`, `gaps`는 요청 범위의 사실을 나타낸다. R2/색인
조회가 실패하면 일부 페이지를 정상 성공처럼 반환하지 않고 503과 재시도 가능한 오류를
반환한다. `limit` 기본값 50, 최대 200, segment 객체 크기 상한과 압축 해제 상한을 둔다.

## 단계별 배포 게이트

1. PostgreSQL 세션 emitter를 독립적으로 켜고 ClickHouse가 없어도 세션 시작·종료가
   올바른지 합성 입력과 운영 관측으로 확인한다. 기존 세션의 소급 복원을 약속하지 않는다.
2. 마이그레이션, 비공개 R2 bucket과 bucket 범위의 S3 API 토큰, spool mount와 모니터링을 준비한다. Terraform
   production 변경은 최신 exact plan 검토와 명시 승인을 거친다.
3. worker archive writer를 shadow 모드로 켠다. Redis 실시간 경로, 후원 journal,
   spool 압력과 DB 중복 제거를 확인하고 archive 시작 시각을 기록한다.
4. R2 exporter를 켜서 업로드 실패·재시도·복원을 합성 데이터로 검증한 뒤 hot 정리를
   허용한다. 백업 복원에는 segment 색인과 R2 객체의 대조가 포함된다.
5. 그 후에만 과거 채팅 HTTP 경로를 공개한다. 표준 WebSocket은 Redis cursor의 gap을
   명시하며, 영구 archive 수락 여부를 Redis cursor와 혼동하지 않는다.

## 검증할 실패 사례

| 상황 | 요구되는 결과 |
| --- | --- |
| 방송 종료와 재연결이 겹침 | 동일 원천 세션 키는 같은 공개 ID, 다른 방송은 다른 ID |
| 같은 event 재전송 | archive 1건, `position`은 안정적 |
| DB 중단 후 worker 재시작 | spool replay, 유실 없는 재시도 |
| spool 포화·손상 | gap과 readiness에 노출, Redis 경로는 독립적으로 동작 |
| R2 업로드 성공 뒤 색인 실패 | deterministic key 재시도, hot 행 유지 |
| 색인 성공 뒤 hot 삭제 전 중단 | 읽기 중복 제거, 재시도 시 정확히 한 페이지 |
| R2 객체 손상·누락 | 503, 해당 세션의 품질 장애 경보 |
| 세션 없이 받은 채팅 | 미할당 구간으로 격리, 다른 방송에 임의 포함하지 않음 |
