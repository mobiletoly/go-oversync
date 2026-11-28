#!/usr/bin/env python3
"""Validate the portable transition supplement and execute every mapped Go test."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess


def validate_scenario_contracts(scenarios, artifacts):
    if [s['id'] for s in scenarios] != [f'M6-C{i:02}' for i in range(1, 7)]:
        raise ValueError('stable scenario IDs changed')
    cases = artifacts.get('cases.json')
    if not isinstance(cases, list):
        raise ValueError('cases.json must contain a case array')
    case_counts = {}
    for case in cases:
        case_id = case.get('ID') if isinstance(case, dict) else None
        if not case_id:
            raise ValueError('case without ID')
        case_counts[case_id] = case_counts.get(case_id, 0) + 1
    duplicates = sorted(case_id for case_id, count in case_counts.items() if count != 1)
    if duplicates:
        raise ValueError('duplicate case IDs: ' + ','.join(duplicates))
    groups = {}
    for scenario in scenarios:
        for field in ('initial', 'actions', 'exchanges', 'expected', 'requirements', 'go'):
            if not scenario[field]:
                raise ValueError('missing scenario contract: ' + field)
        initial_cases = scenario['initial'].get('cases')
        if initial_cases != scenario['expected'].get('cases') or not initial_cases:
            raise ValueError('scenario initial and expected cases differ: ' + scenario['id'])
        for case_id in initial_cases:
            if case_id in artifacts and case_id != 'cases.json':
                continue
            if case_counts.get(case_id) != 1:
                raise ValueError('unresolved scenario input: ' + str(case_id))
        for exchange in scenario['exchanges']:
            if 'artifact' not in exchange:
                if not exchange.get('status') or not exchange.get('body'):
                    raise ValueError('invalid inline scenario exchange: ' + scenario['id'])
                continue
            artifact = exchange.get('artifact') if isinstance(exchange, dict) else None
            if artifact not in artifacts:
                raise ValueError('unknown scenario artifact: ' + str(artifact))
            case_id = exchange.get('case')
            if artifact == 'cases.json':
                if not case_id or case_counts.get(case_id) != 1:
                    raise ValueError('unresolved scenario case: ' + str(case_id))
            elif case_id is not None:
                raise ValueError('standalone scenario artifact cannot select a case: ' + artifact)
        for workdir, package, test in scenario['go']:
            if workdir not in ('.', 'integration/mobile_flow') or not re.fullmatch(r'Test[A-Za-z0-9_]+', test):
                raise ValueError('invalid Go mapping')
            groups.setdefault((workdir, package), set()).add(test)
    return groups


def validate_runtime_upgrade_artifact(value):
    required = {
        'keys', 'old_generation', 'target_generation', 'queued_legacy',
        'after_target_write', 'after_old_write', 'runtime_rejection_status',
        'runtime_rejection_error', 'runtime_rejection_message', 'preserve_fields',
    }
    if not isinstance(value, dict) or set(value) != required:
        raise ValueError('runtime-upgrade.json fields changed')
    if len(value['keys']) != 2 or not all(isinstance(item, str) and re.fullmatch(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}', item) for item in value['keys']):
        raise ValueError('runtime-upgrade.json keys are invalid')
    if type(value['old_generation']) is not int or type(value['target_generation']) is not int or value['old_generation'] != 1 or value['target_generation'] != 3:
        raise ValueError('runtime-upgrade.json generations are invalid')
    if type(value['runtime_rejection_status']) is not int or value['runtime_rejection_status'] != 426:
        raise ValueError('runtime-upgrade.json rejection status is invalid')
    for field in ('queued_legacy', 'after_target_write', 'runtime_rejection_error', 'runtime_rejection_message'):
        if not isinstance(value[field], str) or not value[field]:
            raise ValueError('runtime-upgrade.json field is invalid: ' + field)
    if len(value['after_old_write']) != 2 or not all(isinstance(item, str) and item for item in value['after_old_write']):
        raise ValueError('runtime-upgrade.json after_old_write is invalid')
    if value['runtime_rejection_error'] != 'client_runtime_upgrade_required' or value['runtime_rejection_message'] != 'Update the application; keep local data and pending sync work':
        raise ValueError('runtime-upgrade.json rejection payload is invalid')
    expected_preserve = {'source_id', 'next_source_bundle_id', 'canonical_request_hash', 'frozen_rows', 'business_intent'}
    if set(value['preserve_fields']) != expected_preserve or len(value['preserve_fields']) != len(expected_preserve):
        raise ValueError('runtime-upgrade.json preserve_fields are invalid')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true')
    options = parser.parse_args()
    repo = Path(__file__).resolve().parents[1]
    corpus = repo / 'conformance/maintenance-conflicts/v2'
    manifest = json.loads((corpus / 'manifest.json').read_text())
    if manifest['format'] != 'maintenance-conflicts-v2':
        raise ValueError('unknown supplement format')
    for name, expected in manifest['artifacts'].items():
        if Path(name).name != name or hashlib.sha256((corpus / name).read_bytes()).hexdigest() != expected:
            raise ValueError('artifact identity mismatch: ' + name)
    scenarios = json.loads((corpus / 'scenarios.json').read_text())['scenarios']
    artifacts = {name: json.loads((corpus / name).read_text()) for name in manifest['artifacts'] if name.endswith('.json')}
    validate_runtime_upgrade_artifact(artifacts.get('runtime-upgrade.json'))
    groups = validate_scenario_contracts(scenarios, artifacts)
    if options.execute:
        env = dict(os.environ, GOTOOLCHAIN='go1.25.12')
        for (workdir, package), tests in sorted(groups.items()):
            command = ['go', 'test', package, '-json', '-count=1', '-timeout=120s', '-run', '^(' + '|'.join(sorted(tests)) + ')$']
            result = subprocess.run(command, cwd=repo / workdir, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=150)
            passed = set()
            for line in result.stdout.splitlines():
                try:
                    event = json.loads(line)
                except ValueError:
                    continue
                if event.get('Action') == 'pass' and event.get('Test') in tests:
                    passed.add(event['Test'])
            if result.returncode or passed != tests:
                print(result.stdout)
                raise RuntimeError(f'{package}: missing PASS for {sorted(tests - passed)}')
            print(f'{workdir} {package}: {len(passed)} mapped tests PASS')
    print('maintenance-conflicts-v2: six scenarios verified' + (' and executed' if options.execute else ''))


if __name__ == '__main__':
    main()
