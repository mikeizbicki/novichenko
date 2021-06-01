## Common Crawl background information

The common crawl data is structured into three sets of files:

1. WARC files contain the actual crawl data, including the HTML.
   There are petabytes of this data,
   and the data is arranged in a non-deterministic fashion.

1. CDX files are indexes into the WARC files.
   Each web request in the WARC files has an entry in the CDX files,
   but the CDX files are arranged alphabetically by the SURT.
   This means that in order to find the requests for a SURT,
   we can do a binary search to find the entry in the CDX file.
   The CDX entries do not contain the actual crawl data,
   they only contain the position in the WARC file that the data can be accessed at.

1. Index files are summaries of the CDX files.
   CDX files are much smaller than WARC files,
   but they are still very large,
   since there is one entry for every request in the crawl.
   Loading the contents of a CDX file into memory to perform a binary search is infeasible in practice.
   The index files store only 1 out of every 10000ish entries, also in sorted order.
   So we can do binary search on the index files to find the location in the CDX files that correspond to the SURT we're looking for.

## Loading the crawl data for a host

Special techniques:
1. curl downloads only part of a file (networking class)
1. curl run concurrently using semaphores/locks (operating systems class)

### Step 1: Download the index files
```
$ scripts/download_indexes.sh
```
In total this takes about 10GB of disk space.

### Download the CDX files

```
$ scripts/surt_to_cdxs.sh $SURT
```
