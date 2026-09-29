from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location('prune_collector_images', Path(__file__).with_name('prune-collector-images.py'))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class PruneCollectorImagesTest(unittest.TestCase):
    def test_only_old_owned_unreferenced_images_are_selected(self):
        owned = 'ghcr.io/h66rogi/rogi-collector-worker'
        current = f'{owned}@sha256:{"a" * 64}'
        previous = f'{owned}@sha256:{"b" * 64}'
        old = f'{owned}@sha256:{"c" * 64}'
        other = f'ghcr.io/another/project@sha256:{"d" * 64}'
        images = [
            {'Id': 'current', 'RepoDigests': [current]},
            {'Id': 'previous', 'RepoDigests': [previous]},
            {'Id': 'old', 'RepoDigests': [old]},
            {'Id': 'stopped-container', 'RepoDigests': [f'{owned}@sha256:{"e" * 64}']},
            {'Id': 'other-product', 'RepoDigests': [other]},
            {'Id': 'unknown', 'RepoDigests': []},
        ]
        self.assertEqual(
            module.stale_image_ids(images, {'current', 'stopped-container'}, {current, previous}, {owned}),
            ['old'],
        )

    def test_release_manifest_must_stay_inside_releases(self):
        with tempfile.TemporaryDirectory() as temporary:
            app = Path(temporary) / 'app'
            releases = app / 'releases'
            release = releases / 'r1'
            release.mkdir(parents=True)
            (release / 'manifest.json').write_text(json.dumps({'product': 'rogi-collector', 'images': {'worker': 'digest'}}))
            (app / 'current').symlink_to(release)
            self.assertEqual(module.manifest_images(app / 'current', releases), {'worker': 'digest'})
            outside = Path(temporary) / 'outside'
            outside.mkdir()
            (outside / 'manifest.json').write_text(json.dumps({'product': 'rogi-collector', 'images': {}}))
            (app / 'previous').symlink_to(outside)
            with self.assertRaises(ValueError):
                module.manifest_images(app / 'previous', releases)


if __name__ == '__main__':
    unittest.main()
