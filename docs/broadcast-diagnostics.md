# 운영 화면의 방송 조회

주루마블 `/collector` 화면이 운영 수집 상태를 보여 주고, 입력한 SOOP 채널 ID의 방송 여부를 조회한다.
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
