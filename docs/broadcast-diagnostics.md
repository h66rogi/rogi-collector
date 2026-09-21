# 운영 화면의 방송 조회

주루마블 `/collector` 화면이 운영 수집 상태를 보여 주고, 입력한 SOOP 채널 ID의 방송 여부를 조회한다.
아래 방송 조회는 `CheckBroadcast`에 해당한다. 실제 채팅 입장 테스트는 문서 하단의 별도 흐름을 따른다.
운영 수집은 계속 `h66rogi` 한 채널이다. 테스트 조회는 구독 등록, worker 배정, 채팅 입장,
후원 journal 기록 또는 게임 입력을 만들지 않는다.

## 호출 경계

1. 주루마블 서버는 로그인 세션·CSRF·채널 운영 권한을 확인한다.
2. 운영 consumer/channel의 reader 인증서로 `CheckBroadcast`를 호출한다.
   요청의 `channel_id`는 기존 인증 범위이며, `target_channel_id`만 조회할 SOOP ID다.
3. query가 고정된 내부 주소 `http://discover:8080/diagnostics/broadcast`로 전달한다.
4. discover가 기존 cookie snapshot을 사용해 고정된 SOOP player API를 한 번 조회한다.
   쿠키, player ticket, 원본 응답은 RPC에 포함하지 않는다.

`target_channel_id`는 영문·숫자·밑줄·하이픈 1–50자만 받는다. URL을 받지 않는다.
응답은 채널 ID, 상태, 제공되는 경우 제목·방송자 표시명·방송 번호, 확인 시각, 캐시 여부다.

| 상태 | 의미 |
| --- | --- |
| `live` | 성공한 player 응답에 채팅방 번호가 있음 |
| `offline` | player가 방송 종료/대기를 명시함 |
| `cookie_required` | 쿠키 snapshot이 없거나 유효하지 않음 |
| `auth_required` | player가 로그인을 요구함 |
| `lookup_failed` | 네트워크·응답 파싱·미지원 결과 등으로 방송 여부를 확인하지 못함 |

마지막 세 상태를 방송 종료로 바꾸지 않는다. 방송 조회 성공은 실제 채팅/후원 수신 성공과 다르다.

## 운영 설정과 제한

Secrets Manager의 `discover.env`, `query.env`에 같은 `SOOP_DIAGNOSTIC_TOKEN`을 둔다.
최소 32자 이상의 난수이며 사용자 로그인 정보와 별개다. 코드·fixture·로그에 운영 값을 넣지 않는다.
누락 시 조회만 사용 불가가 되며 기존 수집과 전달은 계속 동작한다.

discover의 probe listener는 Compose 내부 `0.0.0.0:8080`에 바인딩한다. host port는 게시하지 않는다.
기존 `/trigger`의 관리자 인증은 별개이며 diagnostic token으로 실행할 수 없다.
동시에 한 요청, 전체 초당 한 신규 조회, 같은 채널 10초 캐시(최대 64개)를 적용한다.
초과 요청은 gRPC `RESOURCE_EXHAUSTED`, 주루마블 HTTP 429로 표시한다.

합성 테스트는 상태 분류·응답 최소화·ID 검사·캐시·동시 요청 제한과 실제 mTLS 소켓의
consumer/channel/certificate 검사를 다룬다. 플랫폼 실방송 검증은 이 테스트와 구분한다.

## 입장·채팅 수신 테스트

`StartChatTest`, `GetChatTest`, `StopChatTest`는 같은 운영 consumer/channel의 mTLS 인증을 유지한다.
주루마블 API에서는 세 RPC 모두 채널 운영 권한이 필요하며 시작/종료는 CSRF도 검사한다.
시작 요청의 `session_id`는 UUID 멱등 키다. 동일 키·동일 대상 재시도는 보관된 결과를 반환하며
다른 대상을 같은 키로 요청하거나 활성 테스트 중 다른 키를 요청하면 충돌한다.
종료는 해당 session ID에만 적용하며 이전 테스트의 종료 요청이 새 테스트를 끊지 않는다.

query는 고정된 내부 `http://worker:8080/diagnostics/chat-test`에 요청한다.
기존 `SOOP_DIAGNOSTIC_TOKEN`을 `worker.env`에도 동일하게 배치한다. worker의 probe는
Compose 내부에만 바인딩하며 host port는 게시하지 않는다. 새 비밀 값이나 쿠키를 브라우저에 주지 않는다.

worker는 기존 `SoopConnector`의 쿠키 인증·SOOP 호스트 제한·입장 ACK·채팅 파서를 그대로 사용한다.
테스트 커넥터에는 publisher, donation sink, DB/Redis, coordinator 구독을 연결하지 않는다.
일반 채팅만 세고 닉네임·내용·수신 시각을 메모리에 보관한다. raw packet·사용자 ID·후원 이벤트는 반환하지 않는다.
화면에 나타나는 수신 건수는 테스트 커넥터에서 받은 일반 채팅 건수이며 플랫폼 전체 전달을 보장하는 수치가 아니다.

- 동시에 한 테스트, 시작 간격 5초, 시작부터 최대 2분. 브라우저 종료와 무관하게 서버 deadline을 적용한다.
- 최근 채팅 20개, 내용 1,000자·표시 이름 80자 상한. 종료 후 최근 결과만 최대 5분 보관한다.
- 입장 ACK 뒤 `joined`, 첫 일반 채팅 뒤 `receiving`으로 표시한다. 조용한 방송의 입장 성공을 채팅 수신 성공으로 표현하지 않는다.
- 종료·시간 만료·연결 끊김·방송 종료·쿠키 필요·인증 필요·일반 실패를 구분한다. 원본 오류 문자열은 노출하지 않는다.
- 중단 중에는 새 연결을 만들지 않으며 기존 소켓을 닫은 뒤 슬롯을 해제한다. worker 재시작 시 테스트는 복구하지 않는다.
- 같은 게임 채널의 운영자들이 테스트 슬롯을 공유한다. 조회 전용 사용자는 채팅 샘플을 열람하거나 테스트를 제어할 수 없다.

기존 worker의 수집 거부(opt-out) 캐시도 적용한다. 거부된 채널의 시작은 차단하고, 실행 중 거부가 갱신되면 연결을 종료하며 샘플을 비운다.
