# SOOP 이모티콘 원본 형식 확인 (2026-09-23)

- SOOP `emoticons.php` 응답의 `data.default.groups` 316개, `data.subscribe.groups` 58개를 보관된 원본 응답에서 확인했다. 2026-09-23 재조회에서는 각각 334개와 58개였다. 일반 이모티콘은 채팅 본문의 `/토큰/` 문자열을 이 목록의 `keyword`에 대조한다.
- SOOP `signature_emoticon_api.php`의 공개 채널 응답에서 `img_path`와 `tier1`/`tier2`의 `title`, `pc_img` 필드를 확인했다. 예시 채널 응답은 27개와 24개이며, 응답 URL 형식의 이미지가 `200 image/webp`를 반환했다.
- 보관된 브라우저 trace의 `ws_frames.json`은 빈 배열이라 실제 OGQ 수신 프레임은 확보하지 못했다. 아래 OGQ 필드 순서는 SOOP의 [LivePlayer.js](https://static.sooplive.com/asset/app/liveplayer/player/dist/LivePlayer.js)에서 `SVC_OGQ_EMOTICON`(109) 처리부가 `t.packet`을 읽는 방식으로 검증했다.
- OGQ 원본 필드: `[0] chatNo`, `[1] 동반 텍스트`, `[2] groupId`, `[3] subId`, `[4] version`, `[5] userId`, `[6] nickname`, `[17] animated flag`. 일반 채팅 명령 5와 달라 기존 수집기의 명령 분기에서 누락됐다.
- SOOP [ViewVendor.js](https://static.sooplive.com/asset/app/liveplayer/view/dist/ViewVendor.js)는 OGQ 채팅 이미지를 `sticker/{groupId}/{subId}_160.{png|webp}`로 조합한다. 공개 CDN의 `sticker/17d73948ad610a6/1_160.png`가 `200 image/png`를 반환하는 것도 확인했다.
- 재현 테스트는 개인 메시지가 없는 합성 프레임을 사용한다. 실제 방송의 OGQ 수신·렌더링은 배포 후 프레임이 들어와야 확인할 수 있다.
