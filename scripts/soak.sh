#!/bin/sh
# Each nextest case gets its own process, storage, and resource monitor.
set -eu

duration=${1:-600}
case $duration in
'' | 0* | *[!0-9]*)
	echo 'usage: scripts/soak.sh SECONDS [nextest arguments...]' >&2
	exit 1
	;;
esac
[ $# -eq 0 ] || shift

root=${ZZQ_SOAK_ROOT:-/mnt/ssd2/tmp/zzq-soak}
mount=${ZZQ_SOAK_MOUNT:-/mnt/ssd2}
if ! mountpoint -q "$mount"; then
	echo "storage is not mounted: $mount" >&2
	exit 1
fi
mkdir -p "$root"
root=$(cd "$root" && pwd)
run=$root/$(date -u +%Y%m%dT%H%M%S)-$$-${duration}s
mkdir -p "$run"

TMPDIR=$(dirname "$root")
ZZQ_SOAK_DIR=$run
ZZQ_SOAK_SECS=$duration
ZZQ_SOAK_SEED=${ZZQ_SOAK_SEED:-24301}
RUST_BACKTRACE=${RUST_BACKTRACE:-1}
export TMPDIR ZZQ_SOAK_DIR ZZQ_SOAK_SECS ZZQ_SOAK_SEED RUST_BACKTRACE

cd "$(dirname "$0")/.."
profile=${ZZQ_SOAK_PROFILE:-soak}
# Nextest keeps its reports here, whatever the Cargo target directory.
xml=target/nextest/$profile/soak.xml
# A build failure must not inherit a previous run's passing report.
rm -f "$xml"

# Nextest stops a job stuck well past its time, whatever that is.
config=$run/nextest.toml
periods=$(((duration * 3 / 2 + 300) / 30))
sed "s/terminate-after = [0-9]*/terminate-after = $periods/" \
	.config/nextest.toml >"$config"

set -- cargo nextest run --release -p zzq-broker --features soak \
	--config-file "$config" \
	--test zzq_soak --run-ignored ignored-only --profile "$profile" \
	--test-threads 6 --max-fail 1:immediate "$@"

{
	git rev-parse HEAD
	git diff HEAD --stat
} >"$run/revision.txt"
{
	git diff HEAD --binary
	# A difference makes `git diff --no-index` exit 1.
	git ls-files --others --exclude-standard -z |
		xargs -0 -r -I{} git diff --no-index --binary -- /dev/null {} || :
} >"$run/source.patch"

quote() {
	printf "'%s' " "$(printf '%s' "$1" | sed "s/'/'\\\\''/g")"
}
{
	for key in ZZQ_SOAK_SECS ZZQ_SOAK_SEED ZZQ_SOAK_PROFILE RUST_BACKTRACE; do
		eval "value=\${$key:-}"
		if [ -n "$value" ]; then
			printf '%s=' "$key"
			quote "$value"
		fi
	done
	for arg; do
		quote "$arg"
	done
	echo
} >"$run/command.txt"

echo "soak_dir=$run seconds=$duration nextest_workers=6"

# A pipe would lose nextest's exit status. Tee outlives the signals, to show
# and keep what nextest says while it stops.
output=$run/.output
mkfifo "$output"
(
	trap '' HUP TERM
	exec tee "$run/nextest.log"
) <"$output" &
tee_pid=$!

# A session of its own, which the signals below stop as one.
setsid "$@" >"$output" 2>&1 </dev/null &
pid=$!
trap 'kill -s TERM -- "-$pid" 2>/dev/null || :' HUP INT TERM

# A signal ends the wait, not nextest.
status=0
wait "$pid" || status=$?
while kill -0 "$pid" 2>/dev/null; do
	status=0
	wait "$pid" || status=$?
done
trap - HUP INT TERM
wait "$tee_pid" || :
rm -f "$output"

[ ! -f "$xml" ] || cp "$xml" "$run/soak.xml"
if [ "$status" -eq 0 ]; then
	result=PASS
else
	result=FAIL
fi
echo "soak_result=$result artifacts=$run"
exit "$status"
