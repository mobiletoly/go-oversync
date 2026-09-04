#!/bin/sh

set -eu

if [ "$#" -eq 0 ]; then
	printf '%s\n' 'usage: scripts/go-test-with-cleanup.sh [go test arguments...]' >&2
	exit 2
fi

test_run_id=${OVERSYNC_TEST_RUN_ID:-go-oversync-$(date +%s)-$$}

cleanup() {
	container_ids=$(docker ps -aq \
		--filter "label=com.mobiletoly.go-oversync.test-run=${test_run_id}" \
		2>/dev/null || true)
	for container_id in $container_ids; do
		docker rm -fv "$container_id" >/dev/null 2>&1 || true
	done
}

cleanup_on_signal() {
	cleanup
	exit 143
}

trap cleanup EXIT
trap cleanup_on_signal HUP INT TERM

status=0
GOTOOLCHAIN="${GOTOOLCHAIN:-go1.25.12}" \
	OVERSYNC_TEST_RUN_ID="$test_run_id" go test "$@" || status=$?
exit "$status"
