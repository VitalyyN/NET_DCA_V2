"""Verify that the original NetDCA files still match the copy-time manifest."""
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT.parent / 'NetDCA'


def main():
    manifest = json.loads((ROOT / 'source_manifest.json').read_text(encoding='utf-8-sig'))
    changed = []
    for entry in manifest:
        path = SOURCE / entry['path']
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest().upper() != entry['sha256'].upper():
            changed.append(entry['path'])
    print(json.dumps({'checked': len(manifest), 'original_changed': changed}, ensure_ascii=False))
    if changed:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
