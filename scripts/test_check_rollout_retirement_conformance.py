import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('guard', Path(__file__).with_name('check-rollout-retirement-conformance.py'))
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)


class GuardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.corpus = Path(self.temp.name) / 'v1'
        shutil.copytree(Path(__file__).resolve().parents[1] / 'conformance/rollout-retirement/v1', self.corpus)

    def rewrite(self, filename, mutate):
        path = self.corpus / filename
        value = json.loads(path.read_text())
        mutate(value)
        path.write_text(json.dumps(value))
        manifest_path = self.corpus / 'manifest.json'
        manifest = json.loads(manifest_path.read_text())
        manifest['artifacts'][filename] = hashlib.sha256(path.read_bytes()).hexdigest()
        manifest_path.write_text(json.dumps(manifest))

    def test_valid(self):
        guard.validate(self.corpus)

    def test_updated_hash_does_not_waive_wire_semantics(self):
        self.rewrite('http.json', lambda value: value['cases'][0].update(retryable=True))
        with self.assertRaises(ValueError):
            guard.validate(self.corpus)

    def test_updated_hash_does_not_waive_preservation(self):
        self.rewrite('journeys.json', lambda value: value['cases'][0].update(pending_outcome='ready'))
        with self.assertRaises(ValueError):
            guard.validate(self.corpus)

    def test_duplicate_case_is_rejected(self):
        self.rewrite('intent.json', lambda value: value['cases'][1].update(id=value['cases'][0]['id']))
        with self.assertRaises(ValueError):
            guard.validate(self.corpus)

    def test_wrong_type_is_rejected(self):
        self.rewrite('boundaries.json', lambda value: value['cases'][0].update(status='426'))
        with self.assertRaises(ValueError):
            guard.validate(self.corpus)


if __name__ == '__main__':
    unittest.main()
