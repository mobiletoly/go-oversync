#!/usr/bin/env python3
"""Independent rollout portable structure, identity, semantics, and consumer guard."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess


def unique_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError('duplicate JSON property: ' + key)
        value[key] = item
    return value


def read(path):
    return json.loads(path.read_text(), object_pairs_hook=unique_object)


def schema_check(value, schema):
    types = {'object': dict, 'array': list, 'string': str, 'integer': int, 'boolean': bool}
    if 'type' in schema and type(value) is not types[schema['type']]:
        raise ValueError('schema type: ' + schema['type'])
    if 'enum' in schema and value not in schema['enum']:
        raise ValueError('schema enum')
    if isinstance(value, dict):
        if not set(schema.get('required', [])).issubset(value):
            raise ValueError('schema required property')
        for key, rule in schema.get('properties', {}).items():
            if key in value:
                schema_check(value[key], rule)
    if isinstance(value, list):
        if not schema.get('minItems', 0) <= len(value) <= schema.get('maxItems', 1024):
            raise ValueError('schema array capacity')
        for item in value:
            schema_check(item, schema.get('items', {}))


def validate(corpus):
    manifest = read(corpus / 'manifest.json')
    if manifest['format'] != 'oversync.rollout-retirement.manifest.v1':
        raise ValueError('manifest format')
    files = {'retired-client.json', 'http.json', 'intent.json', 'boundaries.json', 'journeys.json'}
    if set(manifest['artifacts']) != files | {'schema.json'}:
        raise ValueError('artifact inventory')
    for name, expected in manifest['artifacts'].items():
        if hashlib.sha256((corpus / name).read_bytes()).hexdigest() != expected:
            raise ValueError('artifact hash: ' + name)
    schema = read(corpus / 'schema.json')
    ids = set()
    cases = {}
    for name in sorted(files):
        document = read(corpus / name)
        schema_check(document, schema['$defs'][name])
        cases[name] = document['cases']
        for case in document['cases']:
            if case['id'] in ids:
                raise ValueError('duplicate case identity')
            ids.add(case['id'])
    if ids != set(manifest['consumers']):
        raise ValueError('consumer identity coverage')
    if {c['operation'] for c in cases['retired-client.json']} != {'upload', 'download', 'watch', 'watch_auto'}:
        raise ValueError('loop coverage')
    for name in ('retired-client.json', 'http.json', 'boundaries.json'):
        for case in cases[name]:
            retired = case['status'] == 426
            if (case['error'], case['retryable'], case['remediation']) != (
                ('retired_schema_generation', False, 'upgrade_application') if retired else
                ('application_maintenance', True, 'retry')):
                raise ValueError('wire category semantics')
    if len(cases['http.json']) != 12 or any(c['unauthenticated_status'] != 401 for c in cases['http.json']):
        raise ValueError('HTTP and authentication coverage')
    if {(c['boundary'], c['status']) for c in cases['boundaries.json']} != {('push', 426), ('push', 503)}:
        raise ValueError('upload boundary coverage')
    if {c['initial_state'] for c in cases['intent.json']} != {'conflicted', 'uncertain'}:
        raise ValueError('preserved intent coverage')
    expected_intent = {
        'conflicted': ('push_conflict', 0),
        'uncertain': ('retired_schema_generation', 1),
    }
    for case in cases['intent.json']:
        if (case['outcome'], case['requests_after_retirement']) != expected_intent.get(case['initial_state']):
            raise ValueError('restart intent outcome')
    for case in cases['journeys.json']:
        if (case['old_generation'], case['pending_outcome'], case['empty_outcome']) != (1, 'old_generation_unavailable', 'ready'):
            raise ValueError('upgrade preservation semantics')
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true')
    options = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    manifest = validate(root / 'conformance/rollout-retirement/v1')
    groups = {}
    for consumers in manifest['consumers'].values():
        if not consumers:
            raise ValueError('case has no Go consumer')
        for workdir, package, test in consumers:
            if workdir not in ('.', 'integration/mobile_flow') or not re.fullmatch(r'\./[a-z_/]+', package) or not re.fullmatch(r'Test[A-Za-z0-9_]+', test):
                raise ValueError('invalid execution consumer')
            groups.setdefault((workdir, package), set()).add(test)
    if options.execute:
        for (workdir, package), tests in sorted(groups.items()):
            command = [str(root / 'scripts/run-with-timeout.pl'), '220', '--',
                       str(root / 'scripts/go-test-with-cleanup.sh'), package,
                       '-run', '^(' + '|'.join(sorted(tests)) + ')$', '-count=1', '-json', '-timeout=210s']
            result = subprocess.run(command, cwd=root / workdir, env={**os.environ, 'GOTOOLCHAIN': 'go1.25.12'},
                                    capture_output=True, text=True, timeout=225)
            print(result.stdout, end='')
            print(result.stderr, end='')
            result.check_returncode()
            events = [json.loads(line) for line in result.stdout.splitlines() if line.startswith('{')]
            passed = {e.get('Test') for e in events if e.get('Action') == 'pass'}
            if not tests.issubset(passed):
                raise ValueError('mapped consumer did not execute and pass')
    print('rollout conformance: valid; ' + ('Go consumers passed' if options.execute else 'execution not requested'))


if __name__ == '__main__':
    main()
