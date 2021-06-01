#!/bin/sh

set -e

crawl=$1
surt=$2
idx=$DATADIR/cc-index/collections/$crawl/indexes/cluster.idx 

outdir=$DATADIR/cdx/$surt
mkdir -p $outdir
filename=${surt}-$crawl.cdx.gz
outfile=$outdir/$filename
if [ -e $outfile ]; then
    echo "$outfile already exists... exiting"
    exit
fi

tmpdir=$DATADIR/tmp/$surt
mkdir -p $tmpdir
tmpfile=$(mktemp $tmpdir/$filename.XXXXXXXXXXXX)

line_start=$(awk "\$1<\"$surt\"{print NR}" $idx | tail -n1)
line_stop=$(awk "\$1>\"${surt}ZZZZZZZZZ\"{print NR}" $idx | head -n1)
line_stop=$(($line_stop + 1))
idx_lines=$(awk "NR>=$line_start && NR<=$line_stop{print \$0}" $idx | sort | uniq)
echo "$idx_lines" | while read idx_line; do
    file=$(echo "$idx_line" | awk '{print $3}')
    offset=$(echo "$idx_line" | awk '{print $4}')
    length=$(echo "$idx_line" | awk '{print $5}')
    tmpfile_small=$(mktemp $tmpdir/$filename.small.XXXXXXXXXXXX)
    echo $file $offset $length
    curl -sS --retry 100 --range $offset-$(($offset + $length - 1)) https://commoncrawl.s3.amazonaws.com/cc-index/collections/$crawl/indexes/$file -o $tmpfile_small
    zcat $tmpfile_small | grep "^$surt" | gzip >> $tmpfile
    rm $tmpfile_small
done

mv $tmpfile $outfile
