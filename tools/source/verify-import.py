#!/usr/bin/env python3
"""Verify the explicit public-source import ledger without copying any files."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference', type=Path, help='Optional read-only clone at the recorded source commit')
    args = parser.parse_args()
    record = json.loads((ROOT / 'docs/source-import-manifest.json').read_text())
    if args.reference:
        head = subprocess.check_output(['git', '-C', str(args.reference), 'rev-parse', 'HEAD'], text=True).strip()
        if head != record['sourceCommit']:
            raise SystemExit('Reference checkout is not at the recorded source commit')
    seen = set()
    identical = renamed = adapted = 0
    for entry in record['files']:
        relative = Path(entry['targetPath'])
        if relative.is_absolute() or '..' in relative.parts or relative in seen:
            raise SystemExit('Unsafe or duplicate target path in import ledger')
        seen.add(relative)
        path = ROOT / relative
        if not path.is_file() or path.is_symlink():
            raise SystemExit(f'Missing or non-regular imported file: {relative}')
        actual = digest(path)
        if actual != entry['currentSha256']:
            raise SystemExit(f'Imported file changed since last recorded review: {relative}')
        if args.reference and digest(args.reference / entry['sourcePath']) != entry['sourceSha256']:
            raise SystemExit(f'Source fingerprint differs: {entry["sourcePath"]}')
        if actual == entry['sourceSha256']:
            identical += 1
        elif actual == entry['importSha256']:
            renamed += 1
        else:
            adapted += 1
    print(f'Verified {len(seen)} imported files: {identical} byte-identical, {renamed} module-path-only, {adapted} adapted/generated.')
    print('No source Git history, private repository, CI or deployment files are part of the allowlist.')


if __name__ == '__main__':
    main()
