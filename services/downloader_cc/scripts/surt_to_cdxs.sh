#!/bin/sh

set -e

crawls=$(ls $DATADIR/cc-index/collections/)
surt=$1

logdir=$DATADIR/logs/$surt
mkdir -p $logdir

for crawl in $crawls; do
    cmd="sh scripts/crawl_surt_to_cdx.sh $crawl $surt"
    echo $cmd
    sem -j100 --id $$ "time $cmd > $logdir/surt_to_cdxs.$surt.$crawl 2>&1"
done
sem --wait --id $$
