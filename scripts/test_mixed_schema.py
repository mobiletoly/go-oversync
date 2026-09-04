import copy
import importlib.util
import json
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('mixed', Path(__file__).with_name('mixed-schema.py'))
mixed = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mixed)

class ReportTests(unittest.TestCase):
    def setUp(self):
        self.m = mixed.manifest()
        for c in self.m['cases']:
            c['status'] = 'implemented'
        parents = sorted({c['test'].split('/')[0] for c in self.m['cases']})
        self.events = [dict(Action='start', Package=mixed.PKG)]
        for p in parents:
            self.events.append(dict(Action='run', Package=mixed.PKG, Test=p))
            for c in self.m['cases']:
                if c['test'].startswith(p + '/'):
                    for a in ('run', 'pass'):
                        self.events.append(dict(Action=a, Package=mixed.PKG, Test=c['test']))
            self.events.append(dict(Action='pass', Package=mixed.PKG, Test=p))
        self.events.append(dict(Action='pass', Package=mixed.PKG))

    def check(self, events, code=0):
        return mixed.check(self.m, [json.dumps(e) for e in events], code)

    def test_complete(self):
        self.assertTrue(self.check(self.events)['complete'])
        self.assertEqual(34, self.check(self.events)['passed'])

    def test_empty_truncated_cancelled_failed_skipped_duplicate_unknown(self):
        streams = [[], self.events[:-1], self.events[:-4]]
        for action in ('skip', 'fail', 'cancelled'):
            events = copy.deepcopy(self.events)
            events[2]['Action'] = action
            streams.append(events)
        streams.append(self.events + [self.events[-1]])
        streams.append(self.events + [dict(Action='pass', Package=mixed.PKG, Test='Unknown')])
        for stream in streams:
            with self.subTest(stream=stream[-1:] if stream else []):
                self.assertFalse(self.check(stream)['complete'])
        self.assertFalse(self.check(self.events, 137)['complete'])
        self.assertFalse(mixed.check(self.m, ['{"Action":'])['complete'])
        self.assertFalse(mixed.check(self.m, ['[]'])['complete'])

    def test_pending_never_passes(self):
        self.m['cases'][0]['status'] = 'pending'
        self.assertFalse(self.check(self.events)['complete'])

    def test_manifest_rejects_missing_duplicate_empty_selector(self):
        import tempfile
        for mutation in ('missing', 'duplicate', 'empty', 'role', 'profile'):
            m = copy.deepcopy(self.m)
            if mutation == 'missing': m['cases'].pop()
            if mutation == 'duplicate': m['cases'][1] = m['cases'][0]
            if mutation == 'empty': m['cases'][0]['test'] = ''
            if mutation == 'role': m['cases'][0]['runtime_roles'] = []
            if mutation == 'profile': m['cases'][0]['profiles'] = ['S']
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json') as file:
                json.dump(m, file); file.flush()
                with self.assertRaises(ValueError): mixed.manifest(file.name)


class DocumentationTests(unittest.TestCase):
    def test_local_document_links(self):
        import re
        for name in ('README.md',):
            path = mixed.ROOT / 'conformance/mixed-schema' / name
            for target in re.findall(r'\]\(([^)]+)\)', path.read_text()):
                if '://' not in target and not target.startswith('#'):
                    self.assertTrue((path.parent / target.split('#')[0]).exists(), target)

    def test_required_workflow_obligations(self):
        workflow = (mixed.ROOT / '.github/workflows/mixed-schema.yml').read_text()
        for text in ('pg: [16, 17]', 'lane: [package, full, mobile, vet]',
                     'needs: [coexistence, regression, guards]', 'if: always()',
                     'retention-days: 14', 'if-no-files-found: error',
                     "v['result'] == 'success'", 'name: mixed-schema-required'):
            self.assertIn(text, workflow)
        self.assertNotIn('paths:', workflow)

if __name__ == '__main__':
    unittest.main()
