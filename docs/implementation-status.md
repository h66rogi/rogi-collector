# 구현 상태

기준일: 2026-09-21. 대상은 후로기(`h66rogi`) 한 채널과 주루마블 소비자 하나다.
공개 chat-collector의 **151개 파일·8개 모듈과 기존 discover → coordinator → worker → query 구조**를 출발점으로 수정했다.
별도 runtime/manager로 교체하지 않았다. 사용자의 후속 배포 요청에 따라 `deploy/live-check`에 격리 검증 배포를 추가했다. 이후 정식 배포 요청에 따라 기존 main/GHCR/SSM 릴리스도 실제 수집기로 연결했다.

## 정식 배포 요약

- 배포 소스: main `62638a4051858a8a7cfb4c7730db91f2f745fb90`, receipt `main-62638a405185`.
- [PR #1](https://github.com/h66rogi/rogi-collector/pull/1) · [정식 release](https://github.com/h66rogi/rogi-collector/releases/tag/production-62638a4051858a8a7cfb4c7730db91f2f745fb90).
- [release 실행](https://github.com/h66rogi/rogi-collector/actions/runs/35583208461)과 [private-deploy 실행](https://github.com/h66rogi/rogi-collector/actions/runs/35583707019) 성공.
- 7개 서비스 healthy, 앱 migration 6개 적용, h66rogi `waiting` / offset 0을 확인했다.
- 실제 EC2 재부팅, 서버 leaf 교체 후 서버 간 mTLS, 쿠키 재로그인·갱신, DB backup/S3 upload를 확인했다.

아래 결과는 기준일의 검증 기록이며 실시간 상태 조회가 아니다. 현재 상태는
[운영 점검 절차](operations-monitoring.md)로 확인한다. 정식 수집기 배포와 주루마블 게임 연동 완료는 구분한다.

## 구현한 동작

| 제품 흐름 | 구현 | 검증 범위 |
| --- | --- | --- |
| 연령제한 방송용 로그인 | 별도 cookie-auth가 역할 파일/환경변수 ID/PW로 로그인, 24시간/요청 갱신, 원자 JSON 저장 | Python 합성 테스트 11개; EC2 실제 로그인·요청 갱신 성공 |
| 후로기만 찾고 연결 | 단일 SOOP 설정 검증, discover·할당·worker 접속 제한, 구독 중지 반영 | 범위 제한 테스트; 공개/연령제한 테스트 방송 실수신, h66rogi는 offline 확인 |
| 쿠키 문제와 방송 종료 구분 | 누락/만료 snapshot, 로그인 필요, 명시적 offline, 알 수 없는 응답 분리 | 합성 API 응답·쿠키 교체 테스트 |
| 입장 확인 | login 뒤 join 전송, 요청한 join의 성공 응답을 받아야 연결 성공 | 합성 packet 검사와 실제 연령제한 방 입장·채팅 확인 |
| 별풍선 전달 | cmd 18의 native 정수 개수, donorId, 연결 epoch/sequence; 채팅 drop 경로 전에 spool 저장 | 가득 찬 채팅 버퍼에서도 같은 33개 후원 두 건 유지 |
| 후원 보관과 재전송 | 기존 PgStore에 journal/outbox, 채널별 직렬 offset, 동일 eventId 재생, 재접속 의심 관측 별도 보존 | 실제 격리 PostgreSQL 17에서 동시 저장·응답 유실·재전송 검사 |
| 저장 장애 | 1GiB/1만 파일 제한 spool, fsync·checksum·단일 writer, 소유권 grant·기록된 수락 시점 대조 | 합성 DB 장애, 실제 DB commit 뒤 응답 유실, full/손상/재시작 검사 |
| 주루마블 내부 연결 | 기존 query lifecycle에 collector v1 11 RPC, 7443 mTLS, 인증서 URI별 읽기/관리/복구 권한 | 실제 TLS socket에서 조회·Watch·ACK·다른 신원/권한 거부 |
| 원하는 칸 선택용 채팅 | 원본 Redis publisher/store 확장, 동일 userId 규칙, 최근 24시간·1만 건, stream generation | 실제 격리 Redis 7과 TLS WatchChat, reset 감지; 게임 명령 파싱은 소비자 책임 |
| 중단 이후 복구 | 명시적 cursor, generation/revision 검사, baseline과 ACK 분리, 멱등 복구·감사 기록 | 구버전 stream 종료, 보존 범위 만료, restore generation 변경 검사 |
| 운영 상태 | waiting/connecting/connected/cookie_required/auth_required/lookup_failed/reconnecting/storage_delayed/storage_stopped/disabled | 상태 연결 코드와 저장소 검사; 실방송 운영 시나리오는 남음 |

정상 동일 개수 후원은 raw hash로 합치지 않는다. 연결 직후의 같은 내용 재관측은 별도 eventId로
남기고 `reconnect_ambiguous` 및 관련 ID를 전달한다. 주루마블은 이를 inbox에 저장·ACK하되 자동 게임 적용 여부를 결정해야 한다.
확인되지 않은 다른 SOOP 후원 command는 별풍선으로 바꾸지 않는다. cmd 18에서 별도 사용자 메시지·원천 시각·원천 ID는
확인하지 못했으므로 빈 메시지/optional 미설정으로 전달한다. 플랫폼에 없는 사용자 메시지나 source ID를 만들어내지 않는다.

## 실행·검증 증거

- Go 8개 모듈 `go test -race` 통과. 격리 PostgreSQL/Redis를 명시한 실행에서 신규 통합 테스트도 수행했다.
- Go 역할 6개와 migration/restore 보조 명령 2개 빌드 통과.
- collector v1 Go↔TypeScript protobuf 검사 4개 통과. offset·sequence·revision은 uint64, JSON은 십진 문자열이다.
- cookie-auth Python 합성 검사 11개 통과.
- PostgreSQL 시험은 매번 고유 schema를 만들고 지웠다. Redis 시험은 고유 key만 사용하고 지웠다.
  이 합성/격리 회귀 검사에는 운영 저장소, 실제 계정, 실제 후원을 사용하지 않았다. 아래 실방송 시험은 별도다.
- `make integration-test`는 `COLLECTOR_TEST_DATABASE_URL`과 `COLLECTOR_TEST_REDIS_ADDR`을 명시해야 실행된다.
  일반 `make test`에는 외부 저장소 연결이 필요 없다.

## 아직 완료하지 않은 제품 검증

1. 후로기 본인의 방송 중 player/login/join·채팅/후원 관측. 이번에는 offline이므로 다른 실제 연령제한 방송으로 검증했다.
2. 실제 WebSocket 서버를 포함한 방송 시작→종료→다음 방송·쿠키 교체·네트워크 단절의 역할 전체 시험.
3. **query → 주루마블 실제 inbox → 게임 명령 → 화면** 통합. 현재 주루마블 작업에는 collector 소비 실행기가 연결되지 않았다.
   33개 한 건→주사위 한 번, 486개 후원자의 채팅→목적지 선택, pause/세션 밖 후원 동작은 아직 통합 미검증이다.
4. 백업의 실제 복원과 EC2 교체 후 기존 data EBS 재연결 검증. 백업 생성/S3 업로드 성공은 복원 성공을 뜻하지 않는다.
5. 운영 알림 수신처 연결·백업 freshness 지표 발행, 소비자 인증서 갱신/게임 소비 프로세스 reload 절차. 서버 leaf 자동 회전과는 별개다.

현재 결과는 **수집기 정식 배포와 격리 실방송 입력·서버 간 전달 검증 완료**다.
주루마블 전체 서비스의 게임 연동과 방송 운영 인수 기준은 위 남은 작업을 완료해야 충족한다.

## 정식 전환 전 격리 EC2 실방송 확인 (2026-09-21)

- 기존 배포와 별도 Compose/DB/Redis/data 경로에 소스를 정적 빌드한 이미지를 배포했다.
- 제공된 테스트 계정으로 로그인했고 쿠키 갱신 요청 후 snapshot 시각이 바뀌었다. 계정/쿠키는 Git 밖에만 두고 전달용 평문 임시 파일은 삭제했다.
- 연령제한 테스트 방송의 익명 player 응답은 -6, 쿠키를 넣은 응답은 1이었다. worker 입장 후 채팅 10건을 읽었다.
- 실제 관측 별풍선 4건을 주루마블 EC2의 **격리된 파일 inbox**에 저장하고 ACK offset 4를 받았다. 보존 시작점 재생은 신규 0건/기존 4건이었다. 실제 게임 DB·규칙·OBS 적용은 하지 않았다.
- 테스트 방송 종료 시 waiting으로 전환됐다. 다른 켜진 방송에서 주루마블 EC2 → private mTLS → query의 새 채팅 1건도 확인했다. 방송에 채팅/후원을 보내지 않았다.
- 실검증에서 원본 orphan 정리의 `chat:*:*` 스캔이 generation 문자열 키까지 삭제하는 문제를 발견했다. Redis STREAM 타입만 조회하도록 수정하고 실제 Redis 회귀 검사로 확인했다.
- 테스트 도구/이미지/Compose 실행 방법과 임시 배포 제약은 [실방송 배포 문서](../deploy/live-check/README.md)에 기록했다.
- 수정 이미지를 재배포하고 worker를 SIGKILL했다. systemd 재시작 1회 후 Go 역할·PostgreSQL·Redis health가 정상으로 복귀했고, 주루마블 EC2에서 새 채팅 수신도 재확인했다. 재시작 직후 상태 조회는 reconnecting이었고 이어 스트림에서 새 이벤트를 받았다.
- 최종 Go 이미지 전달 archive SHA-256: `48c8499be0d7099efeb6193ca1ba26aa42d5ed18f03f200db71e5cd5dabd564b`. 로컬 검증 소스 스냅샷이며 정식 CI 커밋 release는 아니다.
- 최종 코드 검사는 Go 전체 race/계약 4개, 앱/보조 명령 build, 실제 격리 PG17/Redis7 통합, Python 11개, 원본 151개 파일 해시 대조가 통과했다.
- 최종 설정은 모든 역할 `soop:h66rogi`/cookie 모드로 복원했다. 주루마블 EC2의 mTLS 상태 조회는 `waiting`, `collectionActive=false`, 오류 사유 없음이었다. 이 단계에서는 검증 서비스를 실행하되 boot enable하지 않았다. 이후 정식 전환 때 중지·비활성화했고 재부팅 후에도 다시 시작되지 않았다.

## 정식 배포 전환

- PR #1에서 원본 수집기와 운영 profile을 main `62638a4051858a8a7cfb4c7730db91f2f745fb90`에 반영했다. PR CI/소스 비밀 검사와 로컬 Docker 빌드·인증서 회전 검사가 통과했다.
- 새 역할별 DB 계정과 default privileges, Secrets Manager의 확장된 비밀 키 집합을 준비했다. SOOP 계정은 cookie-auth만 읽는다.
- boot 때 instance role로 비밀 파일을 복원하고, encrypted EBS의 쿠키를 다시 사용한다. 서버 인증서 90일/잔여 30일 갱신 경로와 일일 timer를 설치했다.
- 기존 정식 DB를 백업하고 검증 스택은 중지했다. 검증용 데이터는 보존했으며 정식 DB에 복사하지 않았다.
- main release workflow `35583208461`과 private-deploy `35583707019`가 성공했다. 정식 receipt는 `main-62638a405185`, image digest 7개, `soop-single-channel` profile이다.
- 정식 앱 migration 6개가 적용됐고 7개 서비스가 healthy다. 정식 DB는 `h66rogi / waiting / offset 0`이며 테스트 방송 데이터는 들어가지 않았다.
- 주루마블 EC2의 운영 client 인증서로 상태 RPC가 성공했다. 서버 leaf를 강제 교체해 serial 변경을 확인한 뒤 같은 client로 재접속도 성공했다.
- EC2를 실제 재부팅했다. boot ID가 바뀌었고 별도 수동 시작 없이 7개 서비스·비밀 설정·쿠키·6개 앱 migration이 복원됐다. 준비 전 이른 상태 조회는 inactive였으며, boot 의존 순서 완료 후 모두 healthy가 됐다.
- 재부팅 후 주루마블 EC2에서 mTLS 상태 조회가 다시 성공했다. cookie-auth에 실제 갱신을 요청해 `ready`, 새 updated_at, 다음 24시간 갱신 시각을 확인했다.
- 정식 DB backup/S3 upload unit이 성공했고 update/backup/server-TLS timer가 active다. 테스트 tmpfs credentials는 재부팅으로 사라졌으며 검증 스택은 다시 시작되지 않았다.
- 공개된 main 소스 archive를 제공된 계정 ID/PW와 직접 대조해 둘 다 포함되지 않음을 확인했다. 전체 Git history Gitleaks 검사도 통과했다.
- 실제 게임 inbox·후원 효과 적용은 이번 배포에 포함되지 않는다. h66rogi 실방송이 켜졌을 때 해당 채널의 최종 수신 확인도 이어서 필요하다.
