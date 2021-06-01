#!/bin/sh

set -e

# collinfo.json contains information about all the crawls that the common crawl has done
echo updating collinfo.json
curl -C - -L -sS http://index.commoncrawl.org/collinfo.json -o "$DATADIR/collinfo.json"
idxs=$(jq '.[].id' < "$DATADIR/collinfo.json" | tr '"' ' ')

# for each crawl, we download the corresponding index
for idx in $idxs; do
    echo downloading index for $idx
    path=cc-index/collections/$idx/indexes/cluster.idx
    mkdir -p "$DATADIR/$(dirname $path)"
    curl -C - -L -sS https://commoncrawl.s3.amazonaws.com/$path -o "$DATADIR/$path"
done
