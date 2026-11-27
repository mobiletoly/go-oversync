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
GROUP = 'package-oversqlite'
SHARD_1, SHARD_2 = rp.SHARD_GROUPS[GROUP]
ROOT = [M, M + '/oversync', M + '/oversqlite', M + '/oversqlite_e2e', M + '/internal/jcs',
        M + '/integration/schema_evolution', M + '/integration/schema_evolution/cmd/rollout']
MOBILE = [M + '/integration/mobile_flow', M + '/integration/mobile_flow/simulator']
TESTS = sorted(set(rp.SHARD_PINS[GROUP]) | {f'TestCase{i:03}' for i in range(40)} | {'ExampleClient', 'FuzzDecode'})


def events(packages, terminal='pass', tests=('TestX',)):
    out = []
    for p in packages:
        out.append(dict(Action='start', Package=p))
        for t in tests:
            out += [dict(Action='run', Package=p, Test=t), dict(Action='output', Package=p, Test=t, Output='ok\n'),
                    dict(Action='run', Package=p, Test=t + '/sub'), dict(Action='pass', Package=p, Test=t + '/sub'),
                    dict(Action='pass', Package=p, Test=t)]
        out.append(dict(Action=terminal, Package=p))
    return [json.dumps(e) for e in out]


class PartitionTests(unittest.TestCase):
    def test_ownership(self):
        self.assertEqual('package-oversync', rp.lane_of(M + '/oversync'))
        self.assertEqual(GROUP, rp.lane_of(M + '/oversqlite'))
        self.assertEqual((SHARD_1, SHARD_2), rp.owner_lanes(M + '/oversqlite'))
        self.assertEqual('root-evolution', rp.lane_of(M + '/integration/schema_evolution'))
        self.assertEqual('root-evolution', rp.lane_of(M + '/integration/schema_evolution/cmd/rollout'))
        self.assertEqual('root-rest', rp.lane_of(M + '/integration/schema_evolution_authority'))
        self.assertEqual('root-rest', rp.lane_of(M + '/oversqlite_e2e'))
        self.assertEqual('root-rest', rp.lane_of(M + '/brand/new/package'))
        for bad in ('example.com/other', M + '/integration/mobile_flow/simulator'):
            with self.assertRaises(ValueError):
                rp.lane_of(bad)

    def test_assignment_covers_every_package(self):
        lanes = rp.assign(ROOT, MOBILE)
        self.assertEqual([], rp.partition_errors(lanes))
        self.assertEqual([M + '/oversqlite'], lanes[SHARD_1])
        self.assertEqual([M + '/oversqlite'], lanes[SHARD_2])
        unsharded = [p for lane in rp.ROOT_LANES if lane not in rp.SHARD_LANES for p in lanes[lane]]
        self.assertEqual(sorted(set(ROOT) - {M + '/oversqlite'}), sorted(unsharded))
        self.assertEqual(len(unsharded), len(set(unsharded)))

    def test_rejects_invalid_partitions(self):
        good = rp.assign(ROOT, MOBILE)
        mutations = {
            'missing lane': lambda l: l.pop('root-rest'),
            'duplicate across lanes': lambda l: l['root-rest'].append(M + '/oversync'),
            'wrong owner': lambda l: l['root-rest'].append(M + '/integration/schema_evolution/x'),
            'empty lane': lambda l: l.__setitem__('root-evolution', []),
            'sharded package in one shard only': lambda l: l[SHARD_2].remove(M + '/oversqlite'),
            'sharded package outside its shards': lambda l: l['root-rest'].append(M + '/oversqlite'),
            'L2 lanes swapped': lambda l: l.update({'package-oversync': [M + '/oversqlite'], SHARD_1: [M + '/oversync']}),
            'shard widened': lambda l: (l['root-rest'].remove(M + '/oversqlite_e2e'), l[SHARD_1].append(M + '/oversqlite_e2e')),
            'mobile outside module': lambda l: l['mobile'].append(M + '/oversync_x'),
        }
        for name, mutate in mutations.items():
            lanes = copy.deepcopy(good)
            mutate(lanes)
            self.assertTrue(rp.partition_errors(lanes), name)


class ShardTests(unittest.TestCase):
    def test_assignment_is_stable_disjoint_and_pinned(self):
        tests = rp.assign_tests(GROUP, TESTS)
        self.assertEqual([], rp.test_partition_errors(tests))
        self.assertEqual(sorted(TESTS), sorted(tests[SHARD_1] + tests[SHARD_2]))
        self.assertFalse(set(tests[SHARD_1]) & set(tests[SHARD_2]))
        for name, lane in rp.SHARD_PINS[GROUP].items():
            self.assertIn(name, tests[lane])
        self.assertTrue(tests[SHARD_1] and tests[SHARD_2])
        # Adding a test never moves an existing one.
        grown = rp.assign_tests(GROUP, TESTS + ['TestBrandNew'])
        for lane in (SHARD_1, SHARD_2):
            self.assertEqual(tests[lane], [t for t in grown[lane] if t != 'TestBrandNew'])

    def test_stale_pin_fails(self):
        with self.assertRaises(ValueError):
            rp.assign_tests(GROUP, [t for t in TESTS if t not in rp.SHARD_PINS[GROUP]])

    def test_rejects_invalid_test_partitions(self):
        good = rp.assign_tests(GROUP, TESTS)
        moved = next(t for t in good[SHARD_1] if t not in rp.SHARD_PINS[GROUP])
        mutations = {
            'missing shard': lambda t: t.pop(SHARD_2),
            'duplicate across shards': lambda t: t[SHARD_2].append(good[SHARD_1][0]),
            'moved against the rule': lambda t: (t[SHARD_1].remove(moved), t[SHARD_2].append(moved)),
            'pin dropped': lambda t: [t[lane].remove(n) for n, lane in rp.SHARD_PINS[GROUP].items()],
            'not a test name': lambda t: t[SHARD_1].append('helper'),
        }
        for name, mutate in mutations.items():
            tests = copy.deepcopy(good)
            mutate(tests)
            self.assertTrue(rp.test_partition_errors(tests), name)

    def test_run_pattern_matches_exactly_the_shard(self):
        import re
        tests = rp.assign_tests(GROUP, TESTS)
        pattern = re.compile(rp.run_pattern(tests[SHARD_1]))
        self.assertEqual(tests[SHARD_1], [t for t in TESTS if pattern.search(t)])
        self.assertIsNone(pattern.search(tests[SHARD_1][0] + 'Suffix'))

    def test_shard_test_evidence(self):
        package = M + '/oversqlite'
        tests = rp.assign_tests(GROUP, TESTS)[SHARD_1]
        lines = events([package], tests=tests)
        self.assertTrue(rp.check_tests(tests, package, lines)['complete'])
        skipped = [json.dumps(dict(Action='run', Package=package, Test=tests[0])),
                   json.dumps(dict(Action='skip', Package=package, Test=tests[0]))]
        self.assertTrue(rp.check_tests(tests[:1], package, skipped)['complete'])
        failed = [l.replace('"pass"', '"fail"') if f'"Test": "{tests[-1]}"' in l else l for l in lines]
        cases = {
            'missing test': events([package], tests=tests[1:]),
            'failed test': failed,
            'test from the other shard': events([package], tests=tests + ['TestFromOtherShard']),
            'test ran twice': lines + events([package], tests=tests[:1]),
            'truncated': [l for l in lines if f'"Test": "{tests[-1]}"' not in l or '"run"' in l],
        }
        for name, case in cases.items():
            self.assertFalse(rp.check_tests(tests, package, case)['complete'], name)
        self.assertFalse(rp.check_tests([], package, [])['complete'])


class LaneEvidenceTests(unittest.TestCase):
    def test_complete_lane(self):
        lines = ['go: downloading example.com/x v1.0.0'] + events(ROOT[1:]) + events([M], 'skip', ())
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
        self.tests = rp.assign_tests(GROUP, TESTS)
        self.write(rp.PARTITION_ARTIFACT, 'partition.json', dict(version=1, module=M, lanes=self.lanes, tests=self.tests))
        self.write(rp.COEXISTENCE_ARTIFACT, 'fixture.json', self.fixture('coexistence'))
        self.write(rp.COEXISTENCE_ARTIFACT, 'summary.json', dict(complete=True, expected=34, passed=34, failed=0, skipped=0, pending=0))
        for lane in rp.REGRESSION_LANES:
            fixture = self.fixture(lane)
            if lane != 'vet':
                fixture['packages'] = dict(complete=True)
                lines = events(self.lanes[lane], tests=self.tests.get(lane, ('TestX',)))
                (self.dir / ('mixed-schema-' + lane)).mkdir(parents=True, exist_ok=True)
                (self.dir / ('mixed-schema-' + lane) / 'lane.log').write_text('\n'.join(lines) + '\n')
            if lane in rp.SHARD_LANES:
                fixture['tests'] = dict(complete=True)
            if lane == rp.SUPPORTING_TEST_LANE:
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
        self.assertEqual(len(TESTS), sum(result['lanes'][l]['tests']['passed'] for l in rp.SHARD_LANES))

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
        self.edit('mixed-schema-package-oversync', 'fixture.json', lambda f: f['supporting_tests'][0].__setitem__('passed', False))
        self.assertIncomplete('supporting test')

    def test_shard_evidence_failures(self):
        self.edit('mixed-schema-' + SHARD_2, 'fixture.json', lambda f: f.__setitem__('tests', dict(complete=False)))
        self.assertIncomplete('recorded test evidence')
        self.reset()
        log = self.dir / ('mixed-schema-' + SHARD_1) / 'lane.log'
        dropped = self.tests[SHARD_1][0]
        log.write_text('\n'.join(l for l in log.read_text().splitlines() if f'"Test": "{dropped}"' not in l))
        self.assertIncomplete('shard test never ran')
        self.reset()
        self.edit(rp.PARTITION_ARTIFACT, 'partition.json', lambda p: p['tests'][SHARD_2].append(self.tests[SHARD_1][0]))
        self.assertIncomplete('shard lists overlap')

    def test_coexistence_and_partition_failures(self):
        self.edit(rp.COEXISTENCE_ARTIFACT, 'summary.json', lambda s: s.__setitem__('passed', 33))
        self.assertIncomplete('coexistence count')
        self.reset()
        self.edit(rp.PARTITION_ARTIFACT, 'partition.json', lambda p: p['lanes']['root-rest'].append(M + '/oversync'))
        self.assertIncomplete('partition overlap')
        self.reset()
        self.edit(rp.PARTITION_ARTIFACT, 'partition.json', lambda p: p.pop('tests'))
        self.assertIncomplete('partition without shard tests')


REQUIRED_STRATEGY = ('    strategy:\n      fail-fast: false\n      matrix:\n'
                     '        lane: [' + ', '.join(rp.REGRESSION_LANES) + ']\n    steps:\n')


def strategy_errors(workflow):
    """The regression matrix must be exactly the required lane list: no exclude, include or extra axis."""
    errors = []
    if workflow.count('strategy:') != 1 or workflow.count('matrix:') != 1:
        errors.append('workflow must define exactly one matrix')
    if REQUIRED_STRATEGY not in workflow:
        errors.append('regression matrix must be exactly the required lane list')
    return errors


class WorkflowTests(unittest.TestCase):
    def test_regression_matrix_cannot_drop_or_add_lanes(self):
        workflow = (rp.ROOT / '.github/workflows/mixed-schema.yml').read_text()
        self.assertEqual([], strategy_errors(workflow))
        lane_line = '        lane: [' + ', '.join(rp.REGRESSION_LANES) + ']\n'
        mutations = {
            'exclude': lane_line + '        exclude:\n          - lane: root-rest\n',
            'include': lane_line + '        include:\n          - lane: extra\n',
            'extra axis': lane_line + '        shard: [1, 2]\n',
            'dropped lane': lane_line.replace('root-rest, ', ''),
        }
        for name, replacement in mutations.items():
            self.assertTrue(strategy_errors(workflow.replace(lane_line, replacement)), name)
        second = workflow.replace('  guards:\n', '  guards:\n    strategy:\n      matrix:\n        x: [1]\n', 1)
        self.assertTrue(strategy_errors(second), 'second matrix')

    def test_workflow_runs_every_required_lane(self):
        workflow = (rp.ROOT / '.github/workflows/mixed-schema.yml').read_text()
        self.assertIn('lane: [' + ', '.join(rp.REGRESSION_LANES) + ']', workflow)
        for text in ('scripts/test-lane.sh precompile', 'scripts/root-partition.py partition --output',
                     'name: ' + rp.PARTITION_ARTIFACT, 'scripts/root-partition.py check-artifacts',
                     'pattern: mixed-schema-*', 'cache: false', 'timeout-minutes: 4',
                     'workflow_dispatch:', "cache: ${{ github.event_name != 'workflow_dispatch' || inputs.go-cache }}"):
            self.assertIn(text, workflow)
        self.assertNotIn("'full'", workflow)
        self.assertNotIn('lane: [package,', workflow)

    def test_lane_runner_knows_every_lane(self):
        runner = (rp.ROOT / 'scripts/test-lane.sh').read_text()
        for lane in rp.ROOT_LANES + ('mobile',):
            self.assertIn(lane, runner)


if __name__ == '__main__':
    unittest.main()
