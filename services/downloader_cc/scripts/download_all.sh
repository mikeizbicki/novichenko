#!/bin/bash

set -e

cdx_dir="$DATADIR/cdx"
curdir=$(pwd)
cd "$cdx_dir"
sorted_hosts=$(ls $cdx_dir | grep '[),]$' | xargs du | sort -n | cut -f2)
cd $curdir
echo $sorted_hosts

for host in $sorted_hosts; do
    cmd="scripts/download_warcs.sh '$host'"
    echo $cmd
    sem -j4 --id $$ "$cmd"
done
sem --wait --id $$


