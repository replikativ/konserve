#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
repo=$(pwd)
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
(cd test-jolt && "${JOLT:-jolt}" -M -m filestore-cases "$repo/interop/fixtures" "$out")
(cd test-jolt && "${JOLT:-jolt}" -M -m public-file "$out/lifecycle")
fixture='{:aliases {:fixture {:extra-paths ["test-jolt"]}}}'
clojure -Sdeps "$fixture" -M:fixture -m konserve.filestore-parity cross-check "$out"
clojure -J-Xss32m -Sdeps "$fixture" -M:cljs:fixture -m shadow.cljs.devtools.cli compile disk-parity
for codec in boring fressian; do
  fixture=filestore
  if [ "$codec" = boring ]; then fixture=filestore-boring; fi
  diff -r "interop/fixtures/$fixture" "$out/jolt-$codec"
  for version in initial updated; do
    suffix=$codec
    if [ "$version" = updated ]; then suffix=$codec-updated; fi
    node out/disk-parity.js write "$out/node-$suffix" "$codec"
    if [ "$version" = updated ]; then
      node out/disk-parity.js write "$out/node-$suffix" "$codec" updated
    fi
    node out/disk-parity.js read "$out/jolt-$suffix" "$codec" "$version"
    diff -r "$out/jvm-$suffix" "$out/jolt-$suffix"
    diff -r "$out/node-$suffix" "$out/jolt-$suffix"
  done
done
echo 'All three writers matched for Fressian and Boring before and after replacement.'
