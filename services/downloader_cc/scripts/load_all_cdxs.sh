#!/bin/bash

logdir=$LOGDIR/$surt
mkdir -p $logdir

for surt_folder in $(ls "$DATADIR"/cdx); do
    surt=$(basename "$surt_folder")
    cmd="scripts/download_warcs.sh $surt 1000"
    echo $cmd
    #$cmd
    sem -j2 --id $$ "/usr/bin/time $cmd > $logdir/load_all_cdxs 2>&1"
done
#sem --wait --id $$
