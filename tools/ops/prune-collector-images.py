#!/usr/bin/env python3
"""Remove old collector images while retaining the active and rollback releases."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

APP_IMAGE_ROLES = ('discover', 'coordinator', 'worker', 'query', 'cookie-auth')


def manifest_images(link: Path, releases: Path) -> dict[str, str] | None:
    if not link.exists() and not link.is_symlink():
        return None
    release = link.resolve(strict=True)
    if not release.is_relative_to(releases.resolve(strict=True)):
        raise ValueError(f'{link} points outside the release directory')
    manifest = json.loads((release / 'manifest.json').read_text(encoding='utf-8'))
    if manifest.get('product') != 'rogi-collector' or not isinstance(manifest.get('images'), dict):
        raise ValueError(f'{link} has an invalid collector manifest')
    return manifest['images']


def stale_image_ids(images: list[dict], used_ids: set[str], protected: set[str], owned_repos: set[str]) -> list[str]:
    stale = []
    for image in images:
        refs = set(image.get('RepoDigests') or [])
        if not refs or refs & protected or image['Id'] in used_ids:
            continue
        if any(ref.partition('@')[0] in owned_repos for ref in refs):
            stale.append(image['Id'])
    return stale


def docker_json(command: list[str]) -> list[dict]:
    return json.loads(subprocess.check_output(['docker', *command], text=True))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--app-root', type=Path, default=Path('/opt/rogi-collector/app'))
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    releases = args.app_root / 'releases'
    current = manifest_images(args.app_root / 'current', releases)
    if current is None or any(not isinstance(current.get(role), str) or '@sha256:' not in current[role] for role in APP_IMAGE_ROLES):
        raise ValueError('active collector images are unavailable')
    previous = manifest_images(args.app_root / 'previous', releases)
    if previous is not None and any(not isinstance(previous.get(role), str) or '@sha256:' not in previous[role] for role in APP_IMAGE_ROLES):
        raise ValueError('rollback collector images are unavailable')

    protected = set(current.values()) | set((previous or {}).values())
    owned_repos = {current[role].partition('@')[0] for role in APP_IMAGE_ROLES}
    image_ids = sorted(set(subprocess.check_output(['docker', 'image', 'ls', '-q', '--no-trunc'], text=True).split()))
    if not image_ids:
        print(json.dumps({'candidates': 0, 'removed': 0}))
        return 0
    images = docker_json(['image', 'inspect', *image_ids])
    container_ids = subprocess.check_output(['docker', 'ps', '-aq'], text=True).split()
    containers = docker_json(['container', 'inspect', *container_ids]) if container_ids else []
    stale = stale_image_ids(images, {container['Image'] for container in containers}, protected, owned_repos)
    removed = 0
    if not args.dry_run:
        for image_id in stale:
            subprocess.run(['docker', 'image', 'rm', image_id], check=True, stdout=subprocess.DEVNULL)
            removed += 1
    print(json.dumps({'candidates': len(stale), 'removed': removed}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
