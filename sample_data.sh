#!/bin/sh

for warcpath in $(awk 'NR % 10 == 2' warcpaths); do
    ./downloader_warc.sh $warcpath
done
