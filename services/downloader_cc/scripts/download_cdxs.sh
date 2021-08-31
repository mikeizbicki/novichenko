#!/bin/sh

set -e

crawls=$(ls $DATADIR/cc-index/collections/)
surt=$1

logdir=$LOGDIR/$surt
mkdir -p $logdir

for crawl in $crawls; do
    cmd="sh scripts/crawl_surt_to_cdx.sh $crawl $surt"
    echo $cmd
    sem -j100 --id $$ "scripts/crawl_surt_to_cdx.sh '$crawl' '$surt' > '$logdir/crawl_surt_to_cdx.$crawl.$surt' 2>&1"
done
sem --wait --id $$
