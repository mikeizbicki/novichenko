#!/bin/bash

set -e

crawls=$(ls $DATADIR/cc-index/collections/)
surt=$1

logdir=$DATADIR/logs/$surt
mkdir -p $logdir

for crawl in $crawls; do
    cmd="python3 downloader.py --crawl=$crawl --surt=$surt"
    #cmd="scripts/crawl_surt_to_warc.sh $crawl $surt"
    echo $cmd
    #$cmd > $logdir/surt_to_warcs.$surt.$crawl 2>&1 &
    sem -j2 --id $$ "time $cmd > $logdir/surt_to_warcs.$surt.$crawl 2>&1"
done
sem --wait --id $$

