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

# Packages come from the checked-in partition (scripts/root-partition.py).
lane_packages() {
	packages=$(GOTOOLCHAIN=go1.25.12 python3 "$script_dir/root-partition.py" packages "$1")
	if [ -z "$packages" ]; then
		printf '%s\n' "no packages for lane $1" >&2
		exit 2
	fi
	printf '%s\n' "$packages"
}

case "$lane" in
coexistence)
	exec "$timeout_runner" 600 -- env GOTOOLCHAIN=go1.25.12 python3 "$script_dir/mixed-schema.py" run "$@"
	;;
fast)
	exec "$timeout_runner" 240 -- env GOTOOLCHAIN=go1.25.12 "$test_runner" ./oversync \
		-run 'TestRegisteredTableCatalog|TestBootstrap_.*(Catalog|Layout|Adoption)|TestManagedLayout' \
		-count=1 -timeout 3m30s "$@"
	;;
package | root-evolution | root-rest)
	packages=$(lane_packages "$lane")
	# shellcheck disable=SC2086 # import paths contain no whitespace
	exec "$timeout_runner" 240 -- env GOTOOLCHAIN=go1.25.12 "$test_runner" $packages -count=1 -timeout 3m30s "$@"
	;;
precompile)
	# Build and link test binaries without executing them or their TestMain.
	# Only the lane name is accepted: no selectors, overlays or test environment.
	if [ "$#" -ne 1 ]; then
		printf '%s\n' 'usage: scripts/test-lane.sh precompile {package|root-evolution|root-rest|full|mobile}' >&2
		exit 2
	fi
	case "$1" in
	package | root-evolution | root-rest | full)
		packages=$(lane_packages "$1")
		# shellcheck disable=SC2086 # import paths contain no whitespace
		exec "$timeout_runner" 230 -- env GOTOOLCHAIN=go1.25.12 go test -count=1 -exec true $packages
		;;
	mobile)
		cd "$repo_root/integration/mobile_flow"
		exec "$timeout_runner" 230 -- env GOTOOLCHAIN=go1.25.12 go test -count=1 -exec true ./...
		;;
	*)
		printf '%s\n' "no precompile step for lane $1" >&2
		exit 2
		;;
	esac
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
usage: scripts/test-lane.sh {fast|package|root-evolution|root-rest|full|audit|mobile|coexistence} [extra go test arguments...]
       scripts/test-lane.sh precompile {package|root-evolution|root-rest|full|mobile}

  coexistence  complete manifest-checked mixed-schema matrix (600s)
  fast     focused schema/catalog/layout lane
  package  oversync and oversqlite package lane (L2)
  root-evolution  integration/schema_evolution packages (L3a)
  root-rest  every other root package (L3b)
  full     repository root ./... lane (local aggregate of L2, L3a and L3b)
  precompile  build and link a lane's test binaries without running them
  audit    opt-in oversync_audit build-tag lane
  mobile   nested integration/mobile_flow module lane
EOF
	exit 2
	;;
esac
