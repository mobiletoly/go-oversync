#!/usr/bin/env python3
"""Root regression partition and package/test completeness evidence (m0011 §16).

This file is the single checked-in definition of which required lane owns each
root-module package, and of how a sharded package's top-level tests are split
across its shard lanes. The lane runner, the guards job and mixed-schema-required
all read it. A Go exit status alone never proves coverage: every expected package
must report exactly one start and one terminal pass (or skip, for a package
without test files), and every test assigned to a shard must run exactly once
and pass or skip, in that lane's `go test -json` stream.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
MODULE = 'github.com/mobiletoly/go-oversync'
MOBILE_DIR = 'integration/mobile_flow'
OVERSYNC = MODULE + '/oversync'
OVERSQLITE = MODULE + '/oversqlite'
EVOLUTION = MODULE + '/integration/schema_evolution'
# L2a owns oversync and the manifest's supporting test (m0011 §16.8).
SUPPORTING_TEST_LANE = 'package-oversync'
# L2b shards oversqlite by top-level test (m0011 §16.10).
SHARD_GROUPS = {'package-oversqlite': ('package-oversqlite-1', 'package-oversqlite-2')}
SHARDED_PACKAGES = {OVERSQLITE: 'package-oversqlite'}
# Pins keep the two longest tests (about 22 s and 18 s locally) in different
# shards. A pin naming a test that no longer exists fails the partition.
SHARD_PINS = {
    'package-oversqlite': {
        'TestWorkSliceCompletion_FourteenTableSnapshot3399': 'package-oversqlite-1',
        'TestSettlementReservation_PublicCompletion': 'package-oversqlite-2',
    },
}
SHARD_LANES = tuple(lane for lanes in SHARD_GROUPS.values() for lane in lanes)
ROOT_LANES = ('package-oversync',) + SHARD_LANES + ('root-evolution', 'root-rest')
# Local aggregates: `package` is L2a+L2b and `full` is every root lane.
AGGREGATE_LANES = ('package', 'full')
# Lanes whose go test -json stream must account for every expected package.
EVIDENCE_LANES = ROOT_LANES + ('mobile',) + AGGREGATE_LANES
# Hosted regression matrix legs, in addition to the coexistence job.
REGRESSION_LANES = ROOT_LANES + ('mobile', 'vet')
COEXISTENCE_ARTIFACT = 'mixed-schema-pg17'
PARTITION_ARTIFACT = 'mixed-schema-partition'
POSTGRES_MAJOR = 17
COEXISTENCE_CASES = 34
TEST_NAME = re.compile(r'(Test|Example|Fuzz)\w*')


def lane_of(package):
    """Return the lane, or shard group, that owns an import path."""
    if package != MODULE and not package.startswith(MODULE + '/'):
        raise ValueError('package outside the root module: ' + package)
    if package.startswith(MODULE + '/' + MOBILE_DIR + '/') or package == MODULE + '/' + MOBILE_DIR:
        raise ValueError('nested mobile module package in root list: ' + package)
    if package == OVERSYNC:
        return 'package-oversync'
    if package in SHARDED_PACKAGES:
        return SHARDED_PACKAGES[package]
    if package == EVOLUTION or package.startswith(EVOLUTION + '/'):
        return 'root-evolution'
    # Fail-safe default: a new package always lands in a required lane.
    return 'root-rest'


def owner_lanes(package):
    owner = lane_of(package)
    return SHARD_GROUPS.get(owner, (owner,))


def shard_of(group, test):
    """Return the shard lane that owns a top-level test of a sharded package."""
    pinned = SHARD_PINS.get(group, {}).get(test)
    if pinned:
        return pinned
    lanes = SHARD_GROUPS[group]
    return lanes[int(hashlib.sha256(test.encode()).hexdigest(), 16) % len(lanes)]


def _go(args, directory, timeout=300):
    env = dict(os.environ, GOTOOLCHAIN=os.environ.get('GOTOOLCHAIN', 'go1.25.12'))
    out = subprocess.run(['go', *args], cwd=directory, env=env, text=True,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
    if out.returncode:
        raise RuntimeError('go ' + args[0] + ' failed: ' + out.stderr.strip())
    return out.stdout


def go_list(directory):
    packages = _go(['list', './...'], directory, 180).split()
    if not packages:
        raise RuntimeError('go list returned no packages')
    return packages


def list_tests(package, directory=ROOT):
    """List a package's top-level tests, examples and fuzz targets (not benchmarks)."""
    lines = _go(['test', '-count=1', '-list', '.', package], directory).splitlines()
    names = sorted({line for line in lines if TEST_NAME.fullmatch(line)})
    if not names:
        raise RuntimeError('go test -list found no tests in ' + package)
    return names


def assign(root_packages, mobile_packages):
    lanes = {lane: [] for lane in ROOT_LANES}
    for package in sorted(set(root_packages)):
        for lane in owner_lanes(package):
            lanes[lane].append(package)
    lanes['mobile'] = sorted(set(mobile_packages))
    return lanes


def assign_tests(group, names):
    pins = SHARD_PINS.get(group, {})
    missing = sorted(set(pins) - set(names))
    if missing:
        raise ValueError('stale shard pin: ' + ', '.join(missing))
    tests = {lane: [] for lane in SHARD_GROUPS[group]}
    for name in sorted(set(names)):
        tests[shard_of(group, name)].append(name)
    return tests


def partition_errors(lanes):
    """Validate a partition document's lane map without consulting the toolchain."""
    errors = []
    if not isinstance(lanes, dict) or set(lanes) != set(ROOT_LANES) | {'mobile'}:
        return ['partition must define exactly the lanes ' + ', '.join(ROOT_LANES + ('mobile',))]
    placed = {}
    for lane, packages in lanes.items():
        if not isinstance(packages, list) or not packages or not all(isinstance(p, str) and p for p in packages):
            errors.append(lane + ': package list must be a non-empty list of import paths')
            continue
        if len(set(packages)) != len(packages):
            errors.append(lane + ': duplicate package')
        for package in packages:
            placed.setdefault(package, []).append(lane)
    for package, found in sorted(placed.items()):
        if 'mobile' in found:
            if found != ['mobile']:
                errors.append(f'{package}: assigned to {", ".join(found)}')
            elif not (package == MODULE + '/' + MOBILE_DIR or package.startswith(MODULE + '/' + MOBILE_DIR + '/')):
                errors.append('mobile: package outside the nested module: ' + package)
            continue
        try:
            owners = owner_lanes(package)
        except ValueError as exc:
            errors.append(str(exc))
            continue
        if sorted(found) != sorted(owners):
            errors.append(f'{package}: assigned to {", ".join(sorted(found))}, owned by {", ".join(owners)}')
    if not errors:
        if lanes['package-oversync'] != [OVERSYNC]:
            errors.append('package-oversync lane must own exactly ' + OVERSYNC)
        for package, group in SHARDED_PACKAGES.items():
            for lane in SHARD_GROUPS[group]:
                if lanes[lane] != [package]:
                    errors.append(f'{lane} lane must own exactly {package}')
    return errors


def test_partition_errors(tests):
    """Validate the shard test lists: disjoint, complete for the rule, and pin-consistent."""
    if not isinstance(tests, dict) or set(tests) != set(SHARD_LANES):
        return ['tests must define exactly the shard lanes ' + ', '.join(SHARD_LANES)]
    errors = []
    for group, lanes in SHARD_GROUPS.items():
        owner = {}
        for lane in lanes:
            names = tests[lane]
            if not isinstance(names, list) or not names or not all(isinstance(n, str) and TEST_NAME.fullmatch(n) for n in names):
                errors.append(lane + ': test list must be a non-empty list of test names')
                continue
            for name in names:
                if name in owner:
                    errors.append(f'{name}: assigned to {owner[name]} and {lane}')
                owner[name] = lane
                if shard_of(group, name) != lane:
                    errors.append(f'{name}: assigned to {lane}, owned by {shard_of(group, name)}')
        missing = sorted(set(SHARD_PINS.get(group, {})) - set(owner))
        if missing:
            errors.append('stale shard pin: ' + ', '.join(missing))
    return errors


def partition(root=ROOT, with_tests=False):
    root = Path(root)
    lanes = assign(go_list(root), go_list(root / MOBILE_DIR))
    errors = partition_errors(lanes)
    document = dict(version=1, module=MODULE, lanes=lanes)
    if with_tests:
        tests = {}
        for package, group in SHARDED_PACKAGES.items():
            tests.update(assign_tests(group, list_tests(package, root)))
        errors += test_partition_errors(tests)
        document['tests'] = tests
    if errors:
        raise ValueError('; '.join(errors))
    return document


def lane_packages(lane, root=ROOT):
    root = Path(root)
    if lane == 'full':
        return sorted(go_list(root))
    if lane not in EVIDENCE_LANES:
        raise ValueError('no package evidence for lane: ' + lane)
    lanes = partition(root)['lanes']
    if lane == 'package':
        return sorted(set(lanes['package-oversync']).union(*(lanes[l] for l in SHARD_LANES)))
    return lanes[lane]


def lane_tests(lane, root=ROOT):
    """Return the top-level tests a shard lane must run, or None for an unsharded lane."""
    for package, group in SHARDED_PACKAGES.items():
        if lane in SHARD_GROUPS[group]:
            return assign_tests(group, list_tests(package, Path(root)))[lane]
    return None


def run_pattern(names):
    return '^(' + '|'.join(re.escape(n) for n in names) + ')$'


def _events(lines):
    for line in lines:
        try:
            event = json.loads(line)
        except (ValueError, TypeError):
            continue  # go command diagnostics such as module downloads are not JSON
        if isinstance(event, dict) and isinstance(event.get('Package'), str):
            yield event


def check_lane(expected, lines, exit_code=0):
    """Check one lane's go test -json stream against its expected packages."""
    expected = set(expected)
    starts, terminals, unexpected, errors = {}, {}, set(), []
    if not expected:
        errors.append('no expected packages')
    if exit_code:
        errors.append(f'lane exit {exit_code}')
    for event in _events(lines):
        if event.get('Test'):
            continue
        action, package = event.get('Action'), event['Package']
        if package not in expected:
            if package not in unexpected:
                errors.append('unexpected package: ' + package)
            unexpected.add(package)
            continue
        if action == 'start':
            starts[package] = starts.get(package, 0) + 1
        elif action in ('pass', 'fail', 'skip'):
            if package in terminals:
                errors.append('duplicate terminal: ' + package)
            terminals[package] = action
    for package in sorted(expected):
        if starts.get(package) != 1:
            errors.append(f'start count {starts.get(package, 0)}: {package}')
        if terminals.get(package) not in ('pass', 'skip'):
            errors.append(f'{terminals.get(package, "missing terminal")}: {package}')
    passed = sum(a == 'pass' for a in terminals.values())
    skipped = sum(a == 'skip' for a in terminals.values())
    return dict(complete=not errors, expected=len(expected), started=len(starts),
                passed=passed, skipped_no_tests=skipped, errors=errors)


def check_tests(expected, package, lines):
    """Check that every assigned top-level test of a sharded package ran once and passed or skipped."""
    expected = set(expected)
    runs, terminals, unexpected, errors = {}, {}, set(), []
    if not expected:
        errors.append('no expected tests')
    for event in _events(lines):
        test = event.get('Test')
        if event['Package'] != package or not isinstance(test, str) or '/' in test:
            continue
        if test not in expected:
            if test not in unexpected:
                errors.append('unexpected test: ' + test)
            unexpected.add(test)
            continue
        action = event.get('Action')
        if action == 'run':
            runs[test] = runs.get(test, 0) + 1
        elif action in ('pass', 'fail', 'skip'):
            if test in terminals:
                errors.append('duplicate terminal: ' + test)
            terminals[test] = action
    for test in sorted(expected):
        if runs.get(test) != 1:
            errors.append(f'run count {runs.get(test, 0)}: {test}')
        if terminals.get(test) not in ('pass', 'skip'):
            errors.append(f'{terminals.get(test, "missing terminal")}: {test}')
    return dict(complete=not errors, expected=len(expected), ran=len(runs),
                passed=sum(a == 'pass' for a in terminals.values()),
                skipped=sum(a == 'skip' for a in terminals.values()), errors=errors)


def _load_json(path, errors, label):
    try:
        return json.loads(Path(path).read_text())
    except (OSError, ValueError):
        errors.append(label + ': missing or unreadable ' + Path(path).name)
        return None


def _fixture_errors(directory, lane, errors):
    fixture = _load_json(directory / 'fixture.json', errors, lane)
    if fixture is None:
        return None
    if fixture.get('lane') != lane:
        errors.append(f'{lane}: fixture reports lane {fixture.get("lane")!r}')
    if fixture.get('postgres_major') != POSTGRES_MAJOR:
        errors.append(f'{lane}: wrong PostgreSQL major {fixture.get("postgres_major")!r}')
    if fixture.get('exit_code') != 0:
        errors.append(f'{lane}: fixture exit {fixture.get("exit_code")!r}')
    if fixture.get('cleanup') != 'complete' or fixture.get('leftovers') != []:
        errors.append(f'{lane}: cleanup incomplete')
    return fixture


def _partition_document(directory, errors):
    document = _load_json(directory / PARTITION_ARTIFACT / 'partition.json', errors, 'partition')
    if document is None:
        return None
    if document.get('version') != 1 or document.get('module') != MODULE:
        errors.append('partition: wrong version or module')
    found = partition_errors(document.get('lanes')) + test_partition_errors(document.get('tests'))
    errors += ['partition: ' + e for e in found]
    return None if found else document


def _check_coexistence(directory, errors):
    if _fixture_errors(directory, 'coexistence', errors) is None:
        return
    summary = _load_json(directory / 'summary.json', errors, 'coexistence')
    if summary is not None:
        counts = {k: summary.get(k) for k in ('expected', 'passed', 'failed', 'skipped', 'pending')}
        want = dict(expected=COEXISTENCE_CASES, passed=COEXISTENCE_CASES, failed=0, skipped=0, pending=0)
        if summary.get('complete') is not True or counts != want:
            errors.append('coexistence: incomplete manifest result ' + json.dumps(counts, sort_keys=True))


def _check_lane_artifact(lane, lane_dir, fixture, document, errors):
    if lane == SUPPORTING_TEST_LANE:
        support = fixture.get('supporting_tests')
        if not support or not all(c.get('passed') is True for c in support):
            errors.append(lane + ': supporting test evidence missing or failed')
    if document is None:
        return None
    try:
        lines = (lane_dir / 'lane.log').read_text(errors='replace').splitlines()
    except OSError:
        errors.append(lane + ': missing lane.log')
        return None
    result = check_lane(document['lanes'][lane], lines, fixture.get('exit_code', 1))
    errors += [lane + ': ' + e for e in result['errors']]
    recorded = fixture.get('packages')
    if not isinstance(recorded, dict) or recorded.get('complete') is not True:
        errors.append(lane + ': fixture package evidence missing or incomplete')
    summary = {k: v for k, v in result.items() if k != 'errors'}
    if lane in SHARD_LANES:
        tests = check_tests(document['tests'][lane], document['lanes'][lane][0], lines)
        errors += [lane + ': ' + e for e in tests['errors']]
        recorded = fixture.get('tests')
        if not isinstance(recorded, dict) or recorded.get('complete') is not True:
            errors.append(lane + ': fixture test evidence missing or incomplete')
        summary['tests'] = {k: v for k, v in tests.items() if k != 'errors'}
    return summary


def check_artifacts(directory):
    """Validate every required lane artifact downloaded by mixed-schema-required."""
    directory = Path(directory)
    errors = []
    document = _partition_document(directory, errors)
    _check_coexistence(directory / COEXISTENCE_ARTIFACT, errors)
    results = {}
    for lane in REGRESSION_LANES:
        lane_dir = directory / ('mixed-schema-' + lane)
        fixture = _fixture_errors(lane_dir, lane, errors)
        if fixture is None or lane == 'vet':
            continue
        summary = _check_lane_artifact(lane, lane_dir, fixture, document, errors)
        if summary is not None:
            results[lane] = summary
    return dict(complete=not errors, lanes=results, errors=errors)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest='command', required=True)
    p = sub.add_parser('packages', help='print the packages owned by a lane')
    p.add_argument('lane', choices=EVIDENCE_LANES)
    p = sub.add_parser('run-pattern', help='print the -run pattern for a shard lane')
    p.add_argument('lane', choices=SHARD_LANES)
    p = sub.add_parser('partition', help='validate the partition and shard tests and print or write them')
    p.add_argument('--output')
    p = sub.add_parser('check-lane', help='check a go test -json lane log for package (and shard test) completeness')
    p.add_argument('lane', choices=EVIDENCE_LANES)
    p.add_argument('log')
    p = sub.add_parser('check-artifacts', help='check every required lane artifact (mixed-schema-required)')
    p.add_argument('directory')
    args = parser.parse_args(argv)
    if args.command == 'packages':
        print('\n'.join(lane_packages(args.lane)))
        return 0
    if args.command == 'run-pattern':
        print(run_pattern(lane_tests(args.lane)))
        return 0
    if args.command == 'partition':
        text = json.dumps(partition(with_tests=True), indent=2) + '\n'
        if args.output:
            Path(args.output).parent.mkdir(parents=True, exist_ok=True)
            Path(args.output).write_text(text)
        print(text, end='')
        return 0
    if args.command == 'check-lane':
        lines = Path(args.log).read_text(errors='replace').splitlines()
        result = check_lane(lane_packages(args.lane), lines)
        tests = lane_tests(args.lane)
        if tests is not None:
            shard = check_tests(tests, lane_packages(args.lane)[0], lines)
            result['tests'] = shard
            result['complete'] = result['complete'] and shard['complete']
    else:
        result = check_artifacts(args.directory)
    print(json.dumps(result, indent=2))
    return 0 if result['complete'] else 1


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (ValueError, RuntimeError, OSError, subprocess.TimeoutExpired) as exc:
        print(type(exc).__name__ + ': ' + str(exc), file=sys.stderr)
        sys.exit(1)
