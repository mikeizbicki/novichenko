#!/bin/sh

set -e

crawls=$(ls $DATADIR/cc-index/collections/)
surt=$1

logdir=$DATADIR/logs/$surt
mkdir -p $logdir

time scripts/surt_to_cdxs.sh $surt
time scripts/surt_to_warcs.sh $surt
