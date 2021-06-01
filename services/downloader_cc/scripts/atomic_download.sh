#!/bin/bash

set -e

url="$2"
offset="$3"
length="$4"
output="$5"

tmpfile="$(mktemp ${output}.atom.XXXXXXXXXXXX)"
curl -sS --retry 100 --range $offset-$(($offset + $length - 1)) $url -o "$tmpfile";
( flock -x 200
cat "$tmpfile" >> "$output";
) 200>$output.lock
rm "$tmpfile"

echo atomic_download.sh $@

