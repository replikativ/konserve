#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
out=$(mktemp -d)
mkdir "$out/store"
jolt_pid=''
jvm_pid=''
cleanup() {
  if [ -n "$jolt_pid" ]; then kill "$jolt_pid" 2>/dev/null || true; fi
  if [ -n "$jvm_pid" ]; then kill "$jvm_pid" 2>/dev/null || true; fi
  rm -rf "$out"
}
trap cleanup EXIT
(cd test-jolt && exec "${JOLT:-jolt}" -M -m contention "$out/store" "$out") &
jolt_pid=$!
clojure -Sdeps '{:aliases {:race {:extra-paths ["test-jolt"]}}}' -M:race -m contention "$out/store" "$out" &
jvm_pid=$!
for i in $(seq 1 240); do
  if [ -f "$out/jolt" ] && [ -f "$out/jvm" ]; then break; fi
  kill -0 "$jolt_pid" && kill -0 "$jvm_pid" || exit 1
  sleep 1
done
test -f "$out/jolt" && test -f "$out/jvm"
touch "$out/go"
wait "$jolt_pid"
jolt_pid=''
wait "$jvm_pid"
jvm_pid=''
