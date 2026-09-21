# 이식 범위와 Git 이력

원본 clone은 대상 밖의 `_references/rogimarble/dylabs/`에 있다.
현재 공개 collector의 코드와 구조를 경로별 허용 목록에 따라 직접 반입했다.
원본 커밋/대상 파일/수정 이유는 `source-imports.md`와 `source-import-manifest.json`에 기록한다.
원본 Git 이력과 private 코드 전체는 반입하지 않았다.

공개 `meloming-chat-collector`의 필요한 파일을 우선 사용한다. private
`meloming-chat-service`는 차이와 설계 의도를 검토하는 자료다. 이미 공개된 collector도
전체를 자동 반입하지 않고 SOOP에 필요한 파일·테스트·저장소부터 선택한다.

1. 원본 commit과 파일별 허용 목록을 고정한다.
2. 별도 작업 공간에서 불필요한 기능, 내부 설정, 실데이터 fixture를 정리한다.
3. 대상 경로로 필요한 파일만 복사하고 원본/대상/변경 이유/고지/테스트를 기록한다.
4. 첫 커밋 전에 staged 파일과 민감 정보를 검사한다. 첫 push 전에 공개되는 전체 ref와
   커밋을 검사한다. public PR을 만들고 나서 정리하는 것은 이미 공개한 뒤다.

원본 remote/fetch/cherry-pick/merge/subtree/submodule/`.git` 복사로 이력을 연결하지 않는다.
전체 복사 후 커밋하고 나중에 삭제하는 방식도 사용하지 않는다. shallow clone이나
`.gitignore`는 원본 전체의 공개 방지 수단이 아니다. 과거 운영 배포 설정은 새로 작성한다.

공개 collector의 `LICENSE`는 **AGPL-3.0-only**, `NOTICE`에는 DYLabs 고지가 있다.
실제 이식 시 둘을 보존하고 수정 내역을 기록한다. 별도 권리자 허락을 적용한다면
그 근거를 기록하며 임의로 permissive 라이선스를 붙이지 않는다.
원본의 소스 공개 조건과 Git 이력을 가져오지 않는 정책은 별개의 사항이다.
추가 IaC 참고인 public `rogichat`의 자체 코드에는 `PolyForm-Noncommercial-1.0.0` 및 NOTICE가 있다.
해당 코드도 파일별 고지/허용 범위를 확인해 선별 이식한다. private `rogichat-ops`의 실제
manifest·주소·접근키·운영 증거는 public 대상에 가져오지 않는다.
참고: [GNU AGPL](https://www.gnu.org/l/agpl).
