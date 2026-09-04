#!/usr/bin/env python3
"""Run one bounded lane against a named, durable, run-owned PostgreSQL fixture."""
import json
import os
from pathlib import Path
import secrets
import signal
import subprocess
import sys
import time


def docker(*args, timeout=30):
    p = subprocess.run(['docker', *args], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
    if p.returncode:
        # Avoid surfacing command/environment data in failure diagnostics.
        raise RuntimeError('docker ' + ' '.join(args[:2]) + ' failed: ' + p.stderr.strip())
    return p.stdout.strip()


def run(major, lane, extra_args=()):
    if major not in ('16', '17') or lane not in ('coexistence', 'fast', 'package', 'full', 'audit', 'mobile', 'vet'):
        raise ValueError('usage: mixed-schema-fixture.py {16|17} {coexistence|fast|package|full|audit|mobile|vet}')
    root = Path(__file__).resolve().parents[1]
    run_id = 'm0011-' + secrets.token_hex(8)
    volume = run_id + '-data'
    password = secrets.token_hex(24)
    report = Path(os.environ.get('M0011_REPORT_DIR', '/tmp/' + run_id))
    # The lane creates its evidence directory; fixture metadata is kept beside it.
    report.parent.mkdir(parents=True, exist_ok=True)
    metadata = dict(run_id=run_id, postgres_major=int(major), lane=lane, cleanup='pending')
    created_volume = created_container = False
    status = 1
    started = time.monotonic()
    def interrupted(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    try:
        docker('volume', 'create', '--label', 'com.mobiletoly.go-oversync.test-run=' + run_id, volume)
        created_volume = True
        docker('run', '-d', '--name', run_id, '--label', 'com.mobiletoly.go-oversync.test-run=' + run_id,
               '-e', 'POSTGRES_PASSWORD=' + password, '-p', '127.0.0.1::5432', '-v', volume + ':/var/lib/postgresql/data', 'postgres:' + major)
        created_container = True
        deadline = time.monotonic() + 30
        while True:
            try:
                docker('exec', run_id, 'pg_isready', '-h', '127.0.0.1', '-U', 'postgres', timeout=3)
                break
            except RuntimeError:
                if time.monotonic() >= deadline:
                    raise RuntimeError('PostgreSQL startup deadline')
                time.sleep(.1)  # Readiness retry only, never scenario ordering.
        port = docker('port', run_id, '5432/tcp').rsplit(':', 1)[1]
        settings = docker('exec', run_id, 'psql', '-U', 'postgres', '-Atc', "SELECT current_setting('server_version_num')::int/10000,current_setting('fsync'),current_setting('synchronous_commit'),current_setting('full_page_writes')")
        if settings != major + '|on|on|on':
            raise RuntimeError('wrong PostgreSQL version/durability')
        metadata['settings'] = settings
        env = dict(os.environ, TEST_DATABASE_URL=f'postgres://postgres:{password}@127.0.0.1:{port}/postgres?sslmode=disable',
                   GOTOOLCHAIN='go1.25.12', OVERSYNC_TEST_DOCKER_VOLUME='1', OVERSYNC_TEST_ISOLATE_DATABASES='1', OVERSYNC_TEST_RUN_ID=run_id,
                   M0011_PG_MAJOR=major, ROLLOUT_PG_MAJOR=major, M0011_REPORT_DIR=str(report))
        command = ['scripts/test-lane.sh', lane]
        if lane not in ('coexistence', 'vet'):
            command.append('-json')
            command.extend(extra_args)
        if lane == 'vet':
            command = ['scripts/run-with-timeout.pl', '240', '--', 'env', 'GOTOOLCHAIN=go1.25.12', 'go', 'vet', './...']
        metadata['command'] = command
        process = subprocess.Popen(command, cwd=root, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            output, _ = process.communicate(timeout=620 if lane == 'coexistence' else 260)
            status = process.returncode
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            os.killpg(process.pid, signal.SIGTERM)
            try:
                output, _ = process.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                output, _ = process.communicate(timeout=10)
            status = 124
        text = output.decode(errors='replace').replace(password, '[REDACTED]')
        report.mkdir(parents=True, exist_ok=True)
        (report / 'lane.log').write_text(text)
        if lane == 'package':
            coverage = json.loads((root / 'conformance/mixed-schema/v1/manifest.json').read_text())
            supporting = coverage['supporting_tests']
            assert supporting, 'required supporting owners are missing'
            events = []
            for line in text.splitlines():
                try: events.append(json.loads(line))
                except ValueError: pass
            checks = []
            for test in supporting:
                actions = [e.get('Action') for e in events if e.get('Package') == test['package'] and e.get('Test') == test['test']]
                passed = actions.count('run') == 1 and actions.count('pass') == 1 and not any(a in actions for a in ('fail', 'skip'))
                checks.append(dict(test=test['test'], passed=passed))
            metadata['supporting_tests'] = checks
            if not all(c['passed'] for c in checks): status = 1
        print(text)
    except (RuntimeError, subprocess.TimeoutExpired, KeyboardInterrupt) as exc:
        metadata['failure'] = str(exc).replace(password, '[REDACTED]')
        status = 1
    finally:
        leftovers = []
        if created_container:
            try: docker('rm', '-fv', run_id)
            except (RuntimeError, subprocess.TimeoutExpired): leftovers.append(run_id)
        if created_volume:
            try: docker('volume', 'rm', volume)
            except (RuntimeError, subprocess.TimeoutExpired): leftovers.append(volume)
        metadata.update(exit_code=status, elapsed_seconds=time.monotonic()-started,
                        cleanup='failed' if leftovers else 'complete', leftovers=leftovers)
        report.mkdir(parents=True, exist_ok=True)
        (report / 'fixture.json').write_text(json.dumps(metadata, indent=2) + '\n')
        if leftovers: status = 1
    return status

if __name__ == '__main__':
    if len(sys.argv) < 3:
        sys.exit('major and lane required')
    try:
        sys.exit(run(sys.argv[1], sys.argv[2], sys.argv[3:]))
    except (ValueError, RuntimeError, subprocess.TimeoutExpired, KeyboardInterrupt) as exc:
        print(type(exc).__name__ + ': fixture failed; inspect fixture.json', file=sys.stderr)
        sys.exit(1)
