#!/usr/bin/env python3
"""Mixed-schema Go test execution and fail-closed coverage evidence."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / 'conformance/mixed-schema/v1/manifest.json'
PKG = 'github.com/mobiletoly/go-oversync/oversqlite_e2e'


def required_ids():
    result = {f'C{i:02}/main' for i in (1, 2, 3, 4, 5, 8)}
    result |= {f'C06/{o}_{d}' for o in ('A_first', 'B_first') for d in ('accept', 'local', 'merged')}
    result |= {f'C07/{o}' for o in ('A_first', 'B_first')}
    result |= {f'C09/{r}_{o}_{d}' for r in ('A_updates', 'B_updates') for o in ('delete_first', 'update_first') for d in ('accept', 'local', 'merged')}
    result |= {f'C10/{b}' for b in ('staged', 'committed')}
    result |= {f'C11/{g}_{b}' for g in ('G1', 'G2') for b in ('nonfinal', 'final')}
    result |= {f'C12/{d}' for d in ('pull', 'upload')}
    return result


def manifest(path=MANIFEST):
    m = json.loads(Path(path).read_text())
    support = m.get('supporting_tests', [])
    if len(support) != 1 or support[0].get('test') != 'TestGenerationVisibleDataPlane_OmittedVisibleFieldsPreserveValuesAndUseDefaults' or support[0].get('lane') != 'package' or support[0].get('package') != 'github.com/mobiletoly/go-oversync/oversync' or support[0].get('expected_pass_count') != 1:
        raise ValueError('missing required omitted-visible-wire supporting test')
    cases = m['cases']
    ids = [c['id'] for c in cases]
    tests = [c['test'] for c in cases]
    if m['version'] != 1 or len(ids) != 34 or set(ids) != required_ids() or len(set(tests)) != 34 or m['expected_pass_count'] != 34:
        raise ValueError('manifest must cover exactly the 34 unique mixed-schema cases')
    for c in cases:
        if c['package'] != PKG or c['expected_pass_count'] != 1 or c['required'] is not True:
            raise ValueError('invalid package/required/count')
        if not re.fullmatch(r'TestMixedSchema_C\d{2}_[A-Za-z]+(?:/[A-Za-z0-9_]+)?', c['test']):
            raise ValueError('empty or invalid test selector')
        if c['scenario'] != c['id'].split('/')[0] or not c['test'].startswith('TestMixedSchema_' + c['scenario'] + '_'):
            raise ValueError('scenario/test mismatch')
        suffix = c['id'].split('/')[1]
        if suffix != 'main' and not c['test'].endswith('/' + suffix):
            raise ValueError('expansion/test mismatch')
        expected = {'S', 'V'}
        if c['scenario'] in ('C06', 'C07', 'C08', 'C09', 'C10', 'C12'):
            expected.add('P')
        if c['scenario'] in ('C08', 'C10'):
            expected.add('R')
        if set(c['profiles']) != expected or c['runtime_roles'] != ['Go G1', 'Go G2']:
            raise ValueError('assertion profile/runtime roles mismatch')
        if c['status'] not in ('pending', 'implemented') or not (ROOT / c['owner']).is_file():
            raise ValueError('invalid status or missing test owner')
    return m


def check(m, lines, exit_code=0):
    expected = {c['test']: c for c in m['cases']}
    parents = {s.split('/')[0] for s in expected}
    starts, terminals, package_events = {}, {}, []
    errors = []
    if exit_code:
        errors.append(f'test process exit {exit_code}')
    for line in lines:
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except (ValueError, TypeError):
            errors.append('invalid/truncated JSON event')
            continue
        if not isinstance(event, dict):
            errors.append('event must be an object')
            continue
        action, test, package = event.get('Action'), event.get('Test'), event.get('Package')
        if action not in ('start', 'run', 'pause', 'cont', 'output', 'pass', 'fail', 'skip', 'bench'):
            errors.append('unknown or cancelled event')
        if action in ('fail', 'skip'):
            errors.append(f'{action}: {test or package}')
        if package != PKG:
            errors.append('unexpected package event')
        if test:
            if test not in expected and test not in parents:
                errors.append('unknown expansion: ' + str(test))
            if action == 'run':
                starts[test] = starts.get(test, 0) + 1
            if action in ('pass', 'fail', 'skip'):
                if test in terminals or starts.get(test) != 1:
                    errors.append('duplicate terminal or missing run: ' + test)
                terminals[test] = action
        else:
            if action in ('start', 'pass', 'fail', 'skip'):
                package_events.append(action)
    if package_events != ['start', 'pass']:
        errors.append('missing/truncated/failed package completion')
    for test, c in expected.items():
        if c['status'] != 'implemented':
            errors.append('pending: ' + c['id'])
        if starts.get(test) != 1 or terminals.get(test) != 'pass':
            errors.append('missing required pass: ' + c['id'])
    for parent in parents:
        if terminals.get(parent) != 'pass':
            errors.append('missing parent completion: ' + parent)
    return dict(complete=not errors, expected=34,
                executed=sum(starts.get(t, 0) for t in expected),
                passed=sum(terminals.get(t) == 'pass' for t in expected),
                failed=sum(terminals.get(t) == 'fail' for t in expected),
                skipped=sum(terminals.get(t) == 'skip' for t in expected),
                pending=sum(c['status'] != 'implemented' for c in expected.values()), errors=errors)


def redact(text):
    return re.sub(r'postgres(?:ql)?://[^\s"\']+', '[REDACTED_DATABASE_URL]', text)


def identity():
    paths = subprocess.check_output(['git', 'ls-files', '-co', '--exclude-standard'], cwd=ROOT, text=True).splitlines()
    hashes = {}
    for name in sorted(set(paths)):
        path = ROOT / name
        if path.is_file():
            hashes[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    return dict(head=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(), files=hashes)


def run(extra):
    m = manifest()
    if not os.environ.get('TEST_DATABASE_URL') or os.environ.get('M0011_PG_MAJOR') not in ('16', '17'):
        raise ValueError('explicit run-owned TEST_DATABASE_URL and M0011_PG_MAJOR=16/17 required')
    started = time.monotonic()
    output = Path(os.environ.get('M0011_REPORT_DIR', '/tmp/m0011-' + str(time.time_ns())))
    output.mkdir(parents=True, exist_ok=False)
    before = identity()
    env = dict(os.environ, GOTOOLCHAIN='go1.25.12', M0011_QUALIFY='1', OVERSYNC_TEST_DOCKER_VOLUME='1')
    command = ['go', 'test', '-json', '-count=1', './oversqlite_e2e', '-run', '^TestMixedSchema_C', '-timeout', '9m'] + extra
    # The enclosing test-lane timeout includes checking and artifact writing.
    # Leave time for bounded diagnostics when the package deadline fires.
    try:
        proc = subprocess.run(command, cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=550)
        raw, code = proc.stdout.decode(errors='replace'), proc.returncode
    except subprocess.TimeoutExpired as exc:
        raw, code = (exc.stdout or b'').decode(errors='replace'), 124
    raw = redact(raw)
    (output / 'test.jsonl').write_text(raw)
    result = check(m, raw.splitlines(), code)
    if extra:
        result['complete'] = False
        result['errors'].append('development arguments supplied; not a qualification result')
    after = identity()
    if before != after:
        result['complete'] = False
        result['errors'].append('source identity changed during execution')
    result.update(command=command, elapsed_seconds=time.monotonic()-started,
                  postgres_major=int(env['M0011_PG_MAJOR']),
                  runtime=subprocess.check_output(['go', 'version'], env=env, text=True).strip(),
                  manifest_sha256=hashlib.sha256(MANIFEST.read_bytes()).hexdigest(), identity=before)
    (output / 'summary.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({k: v for k, v in result.items() if k not in ('identity', 'errors')}, indent=2))
    for error in result['errors']:
        print(error, file=sys.stderr)
    print('Evidence: ' + str(output))
    return 0 if result['complete'] else 1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['run', 'check', 'manifest'])
    args, extra = parser.parse_known_args()
    if args.mode == 'run':
        return run(extra)
    m = manifest()
    if args.mode == 'manifest':
        print('\n'.join(c['test'] for c in m['cases']))
        return 0
    if len(extra) != 1:
        raise ValueError('check requires exactly one JSONL report')
    result = check(m, Path(extra[0]).read_text().splitlines())
    print(json.dumps(result, indent=2))
    return 0 if result['complete'] else 1

if __name__ == '__main__':
    try:
        sys.exit(main())
    except (ValueError, KeyError, OSError) as exc:
        print(redact(str(exc)), file=sys.stderr)
        sys.exit(1)
