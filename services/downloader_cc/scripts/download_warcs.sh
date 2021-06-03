#!/bin/bash

surt=$1
num_workers=20

logdir=$LOGDIR/$surt
mkdir -p $logdir

for worker in $(seq 0 $(( $num_workers - 1)) ); do
    cmd="python3 ./downloader.py $surt --load-pg --worker=$worker --num-workers=$num_workers"
    echo $cmd
    sem -j100 --id $$ "/usr/bin/time $cmd > $logdir/downloader.$surt.worker=$worker 2>&1"
done
sem --wait --id $$

