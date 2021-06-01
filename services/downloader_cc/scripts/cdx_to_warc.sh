#!/bin/sh

set -e

cdx=$1
outfile=$cdx.warc.gz
if [ -e $outfile ]; then
    exit
fi

tmpdir=tmp
mkdir -p $tmpdir
tmpfile=$(mktemp $tmpdir/$(basename $outfile).XXXXXXXXXXXX)

zcat "$cdx" | while read cdx_line; do
    json=$(echo $cdx_line | sed 's/^.*{/{/')

    status=$(echo $json | jq '.status' | sed 's/"//g' )
    url=$(echo $json | jq '.url' | sed 's/"//g' )
    surt=$(echo $cdx_line | sed 's/ .*$//')
    echo $status $url

    if [ "$status" = 200 ]; then
        filename="$(echo $json | jq '.filename' | sed 's/"//g' )"
        offset=$(echo $json | jq '.offset' | sed 's/"//g' )
        length=$(echo $json | jq '.length' | sed 's/"//g' )
        curl -s --range $offset-$(($offset + $length - 1)) https://commoncrawl.s3.amazonaws.com/$filename >> $tmpfile
    fi
done

mv $tmpfile $outfile
