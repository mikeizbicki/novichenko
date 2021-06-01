#!/bin/bash

set -e

crawl=$1
surt=$2

outdir=$DATADIR/warc/$surt
mkdir -p $outdir
filename=${surt}-$crawl.warc.gz
outfile=$outdir/$filename
if [ -e $outfile ]; then
    echo "outfile already exists $outfile"
    exit
fi
cdx=$DATADIR/cdx/$surt/${surt}-$crawl.cdx.gz

tmpdir=$DATADIR/tmp/$surt
mkdir -p $tmpdir
tmpfile=$(mktemp $tmpdir/$(basename $outfile).XXXXXXXXXXXX)

zcat "$cdx" | while read cdx_line; do
    json=$(echo $cdx_line | sed 's/^.*{/{/')

    status=$(echo $json | jq '.status' -r)
    url=$(echo $json | jq '.url' -r)
    surt=$(echo $cdx_line)
    echo $status $url

    if [ "$status" = 200 ]; then
        filename=$(echo $json | jq '.filename' -r)
        offset=$(echo $json | jq '.offset' -r)
        length=$(echo $json | jq '.length' -r)

        cmd="./scripts/atomic_download.sh $$ https://commoncrawl.s3.amazonaws.com/$filename $offset $length $tmpfile"
        if (( $(ps -ef | grep "atomic_download.sh $$" | wc -l) > 20 )); then
            $cmd
        else
            $cmd &
        fi
        #sem -j10 --id $$ "scripts/atomic_download.sh https://commoncrawl.s3.amazonaws.com/$filename $offset $length $tmpfile"
    fi
done

while (( $(ps -ef | grep "atomic_download.sh $$" | wc -l) > 1 )); do
    sleep 1
done
mv $tmpfile $outfile
