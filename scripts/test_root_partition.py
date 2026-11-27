import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('partition', Path(__file__).with_name('root-partition.py'))
rp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rp)

M = rp.MODULE
ROOT = [M, M + '/oversync', M + '/oversqlite', M + '/oversqlite_e2e', M + '/internal/jcs',
        M + '/integration/schema_evolution', M + '/integration/schema_evolution/cmd/rollout']
MOBILE = [M + '/integration/mobile_flow', M + '/integration/mobile_flow/simulator']


def events(packages, terminal='pass'):
    out = []
    for p in packages:
        out += [dict(Action='start', Package=p), dict(Action='run', Package=p, Test='TestX'),
                dict(Action='pass', Package=p, Test='TestX'), dict(Action=terminal, Package=p)]
    return [json.dumps(e) for e in out]


class PartitionTests(unittest.TestCase):
    def test_ownership(self):
        self.assertEqual('package', rp.lane_of(M + '/oversync'))
        self.assertEqual('package', rp.lane_of(M + '/oversqlite'))
        self.assertEqual('root-evolution', rp.lane_of(M + '/integration/schema_evolution'))
        self.assertEqual('root-evolution', rp.lane_of(M + '/integration/schema_evolution/cmd/rollout'))
        self.assertEqual('root-rest', rp.lane_of(M + '/integration/schema_evolution_authority'))
        self.assertEqual('root-rest', rp.lane_of(M + '/oversqlite_e2e'))
        self.assertEqual('root-rest', rp.lane_of(M + '/brand/new/package'))
        for bad in ('example.com/other', M + '/integration/mobile_flow/simulator'):
            with self.assertRaises(ValueError):
                rp.lane_of(bad)

    def test_assignment_is_a_disjoint_cover(self):
        lanes = rp.assign(ROOT, MOBILE)
        self.assertEqual([], rp.partition_errors(lanes))
        root = [p for lane in rp.ROOT_LANES for p in lanes[lane]]
        self.assertEqual(sorted(ROOT), sorted(root))
        self.assertEqual(len(root), len(set(root)))

    def test_rejects_invalid_partitions(self):
        good = rp.assign(ROOT, MOBILE)
        mutations = {
            'missing lane': lambda l: l.pop('root-rest'),
            'duplicate across lanes': lambda l: l['root-rest'].append(M + '/oversync'),
            'wrong owner': lambda l: l['root-rest'].append(M + '/integration/schema_evolution/x'),
            'empty lane': lambda l: l.__setitem__('root-evolution', []),
            'package lane not exact': lambda l: l['package'].remove(M + '/oversqlite'),
            'mobile outside module': lambda l: l['mobile'].append(M + '/oversync_x'),
        }
        for name, mutate in mutations.items():
            lanes = copy.deepcopy(good)
            mutate(lanes)
            self.assertTrue(rp.partition_errors(lanes), name)


class LaneEvidenceTests(unittest.TestCase):
    def test_complete_lane(self):
        lines = ['go: downloading example.com/x v1.0.0'] + events(ROOT[1:]) + events([M], 'skip')
        result = rp.check_lane(ROOT, lines)
        self.assertTrue(result['complete'], result['errors'])
        self.assertEqual(1, result['skipped_no_tests'])

    def test_incomplete_lanes_fail(self):
        full = events(ROOT)
        cases = {
            'missing package': (events(ROOT[1:]), 0),
            'failed package': (events(ROOT[:-1]) + events(ROOT[-1:], 'fail'), 0),
            'truncated': (full[:-1], 0),
            'duplicate start': (full + [json.dumps(dict(Action='start', Package=ROOT[0]))], 0),
            'duplicate terminal': (full + [json.dumps(dict(Action='pass', Package=ROOT[0]))], 0),
            'unexpected package': (full + events([M + '/surprise']), 0),
            'nonzero exit': (full, 124),
        }
        for name, (lines, code) in cases.items():
            self.assertFalse(rp.check_lane(ROOT, lines, code)['complete'], name)
        self.assertFalse(rp.check_lane([], [])['complete'])


class ArtifactTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.lanes = rp.assign(ROOT, MOBILE)
        self.write(rp.PARTITION_ARTIFACT, 'partition.json', dict(version=1, module=M, lanes=self.lanes))
        self.write(rp.COEXISTENCE_ARTIFACT, 'fixture.json', self.fixture('coexistence'))
        self.write(rp.COEXISTENCE_ARTIFACT, 'summary.json', dict(complete=True, expected=34, passed=34, failed=0, skipped=0, pending=0))
        for lane in rp.REGRESSION_LANES:
            fixture = self.fixture(lane)
            if lane != 'vet':
                fixture['packages'] = dict(complete=True)
                (self.dir / ('mixed-schema-' + lane)).mkdir(parents=True, exist_ok=True)
                (self.dir / ('mixed-schema-' + lane) / 'lane.log').write_text('\n'.join(events(self.lanes[lane])) + '\n')
            if lane == 'package':
                fixture['supporting_tests'] = [dict(test='TestSupport', passed=True)]
            self.write('mixed-schema-' + lane, 'fixture.json', fixture)

    def tearDown(self):
        self.tmp.cleanup()

    def reset(self):
        self.tearDown()
        self.setUp()

    @staticmethod
    def fixture(lane):
        return dict(lane=lane, postgres_major=17, exit_code=0, cleanup='complete', leftovers=[])

    def write(self, artifact, name, value):
        path = self.dir / artifact / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value))

    def edit(self, artifact, name, change):
        path = self.dir / artifact / name
        value = json.loads(path.read_text())
        change(value)
        path.write_text(json.dumps(value))

    def assertIncomplete(self, name):
        self.assertFalse(rp.check_artifacts(self.dir)['complete'], name)

    def test_complete(self):
        result = rp.check_artifacts(self.dir)
        self.assertTrue(result['complete'], result['errors'])
        self.assertEqual(set(rp.ROOT_LANES) | {'mobile'}, set(result['lanes']))

    def test_missing_lane_artifact(self):
        for artifact in [rp.PARTITION_ARTIFACT, rp.COEXISTENCE_ARTIFACT] + ['mixed-schema-' + l for l in rp.REGRESSION_LANES]:
            with self.subTest(artifact=artifact):
                self.reset()
                for f in (self.dir / artifact).iterdir():
                    f.unlink()
                (self.dir / artifact).rmdir()
                self.assertIncomplete(artifact)

    def test_failed_or_dirty_fixtures(self):
        mutations = {
            'exit': lambda f: f.__setitem__('exit_code', 124),
            'cleanup': lambda f: f.__setitem__('cleanup', 'failed'),
            'leftovers': lambda f: f.__setitem__('leftovers', ['vol']),
            'major': lambda f: f.__setitem__('postgres_major', 16),
            'lane': lambda f: f.__setitem__('lane', 'full'),
        }
        for name, change in mutations.items():
            with self.subTest(name=name):
                self.reset()
                self.edit('mixed-schema-root-rest', 'fixture.json', change)
                self.assertIncomplete(name)

    def test_lane_evidence_failures(self):
        self.edit('mixed-schema-root-evolution', 'fixture.json', lambda f: f.__setitem__('packages', dict(complete=False)))
        self.assertIncomplete('recorded package evidence')
        self.reset()
        log = self.dir / 'mixed-schema-root-rest' / 'lane.log'
        log.write_text('\n'.join(log.read_text().splitlines()[:-1]))
        self.assertIncomplete('truncated lane log')
        self.reset()
        self.edit('mixed-schema-package', 'fixture.json', lambda f: f['supporting_tests'][0].__setitem__('passed', False))
        self.assertIncomplete('supporting test')

    def test_coexistence_and_partition_failures(self):
        self.edit(rp.COEXISTENCE_ARTIFACT, 'summary.json', lambda s: s.__setitem__('passed', 33))
        self.assertIncomplete('coexistence count')
        self.reset()
        self.edit(rp.PARTITION_ARTIFACT, 'partition.json', lambda p: p['lanes']['root-rest'].append(M + '/oversync'))
        self.assertIncomplete('partition overlap')


class WorkflowTests(unittest.TestCase):
    def test_workflow_runs_every_required_lane(self):
        workflow = (rp.ROOT / '.github/workflows/mixed-schema.yml').read_text()
        self.assertIn('lane: [' + ', '.join(rp.REGRESSION_LANES) + ']', workflow)
        for text in ('scripts/test-lane.sh precompile', 'scripts/root-partition.py partition --output',
                     'name: ' + rp.PARTITION_ARTIFACT, 'scripts/root-partition.py check-artifacts',
                     'pattern: mixed-schema-*', 'cache: false', 'timeout-minutes: 4',
                     'workflow_dispatch:', "cache: ${{ github.event_name != 'workflow_dispatch' || inputs.go-cache }}"):
            self.assertIn(text, workflow)
        self.assertNotIn("'full'", workflow)


if __name__ == '__main__':
    unittest.main()
