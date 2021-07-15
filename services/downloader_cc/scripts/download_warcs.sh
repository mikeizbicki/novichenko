#!/bin/bash

num_workers=20
surt=$1
if [ -z "$2" ]; then
    max_urls_to_download=''
else
    max_urls_to_download="--max-urls-to-download=$2"
fi

logdir=$LOGDIR/$surt
mkdir -p $logdir

for worker in $(seq 0 $(( $num_workers - 1)) ); do
    cmd="python3 ./downloader.py $surt --load-pg --worker=$worker --num-workers=$num_workers $max_urls_to_download"
    echo $cmd
    sem -j100 --id $$ "/usr/bin/time $cmd > $logdir/downloader.$surt.worker=$worker 2>&1"
done
sem --wait --id $$

