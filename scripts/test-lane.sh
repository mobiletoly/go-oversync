#!/bin/sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
timeout_runner="$script_dir/run-with-timeout.pl"
test_runner="$script_dir/go-test-with-cleanup.sh"
lane=${1:-}

if [ "$#" -gt 0 ]; then
	shift
fi

cd "$repo_root"

case "$lane" in
coexistence)
	exec "$timeout_runner" 600 -- env GOTOOLCHAIN=go1.25.12 python3 "$script_dir/mixed-schema.py" run "$@"
	;;
fast)
	exec "$timeout_runner" 240 -- env GOTOOLCHAIN=go1.25.12 "$test_runner" ./oversync \
		-run 'TestRegisteredTableCatalog|TestBootstrap_.*(Catalog|Layout|Adoption)|TestManagedLayout' \
		-count=1 -timeout 3m30s "$@"
	;;
package)
	exec "$timeout_runner" 240 -- env GOTOOLCHAIN=go1.25.12 "$test_runner" ./oversync ./oversqlite -count=1 -timeout 3m30s "$@"
	;;
full)
	exec "$timeout_runner" 240 -- env GOTOOLCHAIN=go1.25.12 "$test_runner" ./... -count=1 -timeout 3m30s "$@"
	;;
audit)
	exec "$timeout_runner" 240 -- env GOTOOLCHAIN=go1.25.12 "$test_runner" -tags oversync_audit ./oversync -count=1 -timeout 3m30s "$@"
	;;
mobile)
	cd "$repo_root/integration/mobile_flow"
	exec "$timeout_runner" 240 -- env GOTOOLCHAIN=go1.25.12 "$test_runner" ./... -count=1 -timeout 3m30s "$@"
	;;
*)
	cat >&2 <<'EOF'
usage: scripts/test-lane.sh {fast|package|full|audit|mobile|coexistence} [extra go test arguments...]

  coexistence  complete manifest-checked mixed-schema matrix (600s)
  fast     focused schema/catalog/layout lane
  package  oversync and oversqlite package lane
  full     repository root ./... lane
  audit    opt-in oversync_audit build-tag lane
  mobile   nested integration/mobile_flow module lane
EOF
	exit 2
	;;
esac
