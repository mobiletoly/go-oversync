#!/usr/bin/env python3
"""Validate table-evolution portable inputs; optionally execute each named Go consumer."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true')
    options = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    corpus = root / 'conformance/table-evolution/v1'
    manifest = json.loads((corpus / 'manifest.json').read_text())
    if manifest['format'] != 'oversync.table-evolution.manifest.v1':
        raise ValueError('manifest format')
    for name, expected in manifest['artifacts'].items():
        if Path(name).name != name:
            raise ValueError('artifact path')
        if hashlib.sha256((corpus / name).read_bytes()).hexdigest() != expected:
            raise ValueError('artifact hash: ' + name)
    cases = json.loads((corpus / 'cases.json').read_text())['cases']
    ids = [case['id'] for case in cases]
    if len(ids) != len(set(ids)) or set(ids) != set(manifest['consumers']):
        raise ValueError('case identity or consumer mismatch')
    contract = json.loads((corpus / 'contract.json').read_text())
    proofs = {str(proof['ID']): proof['Fingerprint'] for proof in contract['proofs']}
    if set(proofs) != {'1', '2'}:
        raise ValueError('generation identity mismatch')
    for case in cases:
        if str(case['generation']) not in proofs:
            raise ValueError('unknown generation')
        if 'headers' in case:
            if case['headers'] != {'Oversync-Schema-Generation-ID': str(case['generation']),
                                   'Oversync-Schema-Generation-Fingerprint': proofs[str(case['generation'])]}:
                raise ValueError('generation headers mismatch')
    groups = {}
    for consumers in manifest['consumers'].values():
        if not consumers:
            raise ValueError('case lacks an execution consumer')
        for workdir, package, test in consumers:
            if workdir not in ('.', 'integration/mobile_flow') or not re.fullmatch(r'\./[a-z_/]+', package) or not re.fullmatch(r'Test[A-Za-z0-9_]+', test):
                raise ValueError('invalid consumer')
            groups.setdefault((workdir, package), set()).add(test)
    if options.execute:
        for (workdir, package), tests in sorted(groups.items()):
            command = [str(root / 'scripts/run-with-timeout.pl'), '220', '--',
                       str(root / 'scripts/go-test-with-cleanup.sh'), package,
                       '-run', '^(' + '|'.join(sorted(tests)) + ')$', '-count=1', '-json', '-timeout=210s']
            completed = subprocess.run(command, cwd=root / workdir,
                                       env={**os.environ, 'GOTOOLCHAIN': 'go1.25.12'},
                                       text=True, capture_output=True, timeout=225)
            print(completed.stdout, end='')
            print(completed.stderr, end='')
            completed.check_returncode()
            passed = set()
            for line in completed.stdout.splitlines():
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if event.get('Action') == 'pass' and event.get('Test') in tests:
                    passed.add(event['Test'])
            if passed != tests:
                raise ValueError('mapped test did not execute successfully: ' + str(tests - passed))
    print('table-evolution conformance: valid; ' + ('mapped Go tests passed' if options.execute else 'execution not requested'))


if __name__ == '__main__':
    main()
