#!/usr/bin/env python3
"""Root regression partition and package-completeness evidence (m0011 §16).

This file is the single checked-in definition of which required lane owns each
root-module package. The lane runner, the guards job and mixed-schema-required
all read it. A Go exit status alone never proves coverage: every expected
package must report exactly one start and one terminal pass (or skip, for a
package without test files) in its lane's `go test -json` stream.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
MODULE = 'github.com/mobiletoly/go-oversync'
MOBILE_DIR = 'integration/mobile_flow'
L2_PACKAGES = (MODULE + '/oversync', MODULE + '/oversqlite')
EVOLUTION = MODULE + '/integration/schema_evolution'
ROOT_LANES = ('package', 'root-evolution', 'root-rest')
# Lanes whose go test -json stream must account for every expected package.
EVIDENCE_LANES = ROOT_LANES + ('mobile', 'full')
# Hosted regression matrix legs, in addition to the coexistence job.
REGRESSION_LANES = ROOT_LANES + ('mobile', 'vet')
COEXISTENCE_ARTIFACT = 'mixed-schema-pg17'
PARTITION_ARTIFACT = 'mixed-schema-partition'
POSTGRES_MAJOR = 17
COEXISTENCE_CASES = 34


def lane_of(package):
    """Return the required root lane that owns an import path."""
    if package != MODULE and not package.startswith(MODULE + '/'):
        raise ValueError('package outside the root module: ' + package)
    if package.startswith(MODULE + '/' + MOBILE_DIR + '/') or package == MODULE + '/' + MOBILE_DIR:
        raise ValueError('nested mobile module package in root list: ' + package)
    if package in L2_PACKAGES:
        return 'package'
    if package == EVOLUTION or package.startswith(EVOLUTION + '/'):
        return 'root-evolution'
    # Fail-safe default: a new package always lands in a required lane.
    return 'root-rest'


def go_list(directory):
    env = dict(os.environ, GOTOOLCHAIN=os.environ.get('GOTOOLCHAIN', 'go1.25.12'))
    out = subprocess.run(['go', 'list', './...'], cwd=directory, env=env, text=True,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=180)
    if out.returncode:
        raise RuntimeError('go list failed: ' + out.stderr.strip())
    packages = out.stdout.split()
    if not packages:
        raise RuntimeError('go list returned no packages')
    return packages


def assign(root_packages, mobile_packages):
    lanes = {lane: [] for lane in ROOT_LANES}
    for package in sorted(set(root_packages)):
        lanes[lane_of(package)].append(package)
    lanes['mobile'] = sorted(set(mobile_packages))
    return lanes


def partition_errors(lanes):
    """Validate a partition document's lane map without consulting the toolchain."""
    errors = []
    if not isinstance(lanes, dict) or set(lanes) != set(ROOT_LANES) | {'mobile'}:
        return ['partition must define exactly the lanes ' + ', '.join(ROOT_LANES + ('mobile',))]
    seen = {}
    for lane, packages in lanes.items():
        if not isinstance(packages, list) or not packages or not all(isinstance(p, str) and p for p in packages):
            errors.append(lane + ': package list must be a non-empty list of import paths')
            continue
        if len(set(packages)) != len(packages):
            errors.append(lane + ': duplicate package')
        for package in packages:
            if package in seen:
                errors.append(f'{package}: assigned to {seen[package]} and {lane}')
            seen[package] = lane
            if lane in ROOT_LANES:
                try:
                    owner = lane_of(package)
                except ValueError as exc:
                    errors.append(str(exc))
                    continue
                if owner != lane:
                    errors.append(f'{package}: assigned to {lane}, owned by {owner}')
            elif not (package == MODULE + '/' + MOBILE_DIR or package.startswith(MODULE + '/' + MOBILE_DIR + '/')):
                errors.append('mobile: package outside the nested module: ' + package)
    if not errors and sorted(lanes['package']) != sorted(L2_PACKAGES):
        errors.append('package lane must own exactly ' + ', '.join(L2_PACKAGES))
    return errors


def partition(root=ROOT):
    root = Path(root)
    lanes = assign(go_list(root), go_list(root / MOBILE_DIR))
    errors = partition_errors(lanes)
    if errors:
        raise ValueError('; '.join(errors))
    return dict(version=1, module=MODULE, lanes=lanes)


def lane_packages(lane, root=ROOT):
    root = Path(root)
    if lane == 'full':
        return sorted(go_list(root))
    if lane not in EVIDENCE_LANES:
        raise ValueError('no package evidence for lane: ' + lane)
    return partition(root)['lanes'][lane]


def check_lane(expected, lines, exit_code=0):
    """Check one lane's go test -json stream against its expected packages."""
    expected = set(expected)
    starts, terminals, unexpected, errors = {}, {}, set(), []
    if not expected:
        errors.append('no expected packages')
    if exit_code:
        errors.append(f'lane exit {exit_code}')
    for line in lines:
        try:
            event = json.loads(line)
        except (ValueError, TypeError):
            continue  # go command diagnostics such as module downloads are not JSON
        if not isinstance(event, dict) or event.get('Test') or not isinstance(event.get('Package'), str):
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


def check_artifacts(directory):
    """Validate every required lane artifact downloaded by mixed-schema-required."""
    directory = Path(directory)
    errors = []
    document = _load_json(directory / PARTITION_ARTIFACT / 'partition.json', errors, 'partition')
    lanes = None
    if document is not None:
        if document.get('version') != 1 or document.get('module') != MODULE:
            errors.append('partition: wrong version or module')
        lane_errors = partition_errors(document.get('lanes'))
        errors += ['partition: ' + e for e in lane_errors]
        if not lane_errors:
            lanes = document['lanes']
    coexistence = directory / COEXISTENCE_ARTIFACT
    if _fixture_errors(coexistence, 'coexistence', errors) is not None:
        summary = _load_json(coexistence / 'summary.json', errors, 'coexistence')
        if summary is not None:
            counts = {k: summary.get(k) for k in ('expected', 'passed', 'failed', 'skipped', 'pending')}
            if summary.get('complete') is not True or counts != dict(expected=COEXISTENCE_CASES, passed=COEXISTENCE_CASES, failed=0, skipped=0, pending=0):
                errors.append('coexistence: incomplete manifest result ' + json.dumps(counts, sort_keys=True))
    results = {}
    for lane in REGRESSION_LANES:
        lane_dir = directory / ('mixed-schema-' + lane)
        fixture = _fixture_errors(lane_dir, lane, errors)
        if fixture is None or lane == 'vet':
            continue
        if lane == 'package':
            support = fixture.get('supporting_tests')
            if not support or not all(c.get('passed') is True for c in support):
                errors.append('package: supporting test evidence missing or failed')
        if lanes is None:
            continue
        try:
            lines = (lane_dir / 'lane.log').read_text(errors='replace').splitlines()
        except OSError:
            errors.append(lane + ': missing lane.log')
            continue
        result = check_lane(lanes[lane], lines, fixture.get('exit_code', 1))
        results[lane] = {k: v for k, v in result.items() if k != 'errors'}
        errors += [lane + ': ' + e for e in result['errors']]
        recorded = fixture.get('packages')
        if not isinstance(recorded, dict) or recorded.get('complete') is not True:
            errors.append(lane + ': fixture package evidence missing or incomplete')
    return dict(complete=not errors, lanes=results, errors=errors)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest='command', required=True)
    p = sub.add_parser('packages', help='print the packages owned by a lane')
    p.add_argument('lane', choices=EVIDENCE_LANES)
    p = sub.add_parser('partition', help='validate the partition against go list and print or write it')
    p.add_argument('--output')
    p = sub.add_parser('check-lane', help='check a go test -json lane log for package completeness')
    p.add_argument('lane', choices=EVIDENCE_LANES)
    p.add_argument('log')
    p = sub.add_parser('check-artifacts', help='check every required lane artifact (mixed-schema-required)')
    p.add_argument('directory')
    args = parser.parse_args(argv)
    if args.command == 'packages':
        print('\n'.join(lane_packages(args.lane)))
        return 0
    if args.command == 'partition':
        text = json.dumps(partition(), indent=2) + '\n'
        if args.output:
            Path(args.output).parent.mkdir(parents=True, exist_ok=True)
            Path(args.output).write_text(text)
        print(text, end='')
        return 0
    if args.command == 'check-lane':
        result = check_lane(lane_packages(args.lane), Path(args.log).read_text(errors='replace').splitlines())
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
