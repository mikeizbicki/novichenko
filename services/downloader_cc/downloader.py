'''

'''

import itertools
import json
import gzip
import logging
import re
import psutil
import os
import time

from urllib.parse import urlparse
from collections import Counter

################################################################################
# async downloader
################################################################################

import aiohttp
import asyncio


async def get(url, offset, length):
    '''
    Downloads only the `length` bytes of the data at `url` starting at position `offset`.

    RFC2616 specifies how to use HTTP headers to specify which bytes to download from the file.
    For details, see: https://datatracker.ietf.org/doc/html/rfc2616#section-14.35

    For details on writing fast async http clients, see:
    https://julien.danjou.info/python-and-fast-http-clients/
    https://pawelmhm.github.io/asyncio/python/aiohttp/2016/04/22/asyncio-aiohttp.html

    '''
    async with aiohttp.ClientSession() as session:
        headers = { 'Range': f'bytes={offset}-{int(offset)+int(length)-1}' }
        async with session.get(url, headers=headers) as response:
            return await response.content.read()


"""
async def get(url, sections):
    '''
    Downloads only the `length` bytes of the data at `url` starting at position `offset`.

    RFC2616 specifies how to use HTTP headers to specify which bytes to download from the file.
    For details, see: https://datatracker.ietf.org/doc/html/rfc2616#section-14.35

    For details on writing fast async http clients, see:
    https://julien.danjou.info/python-and-fast-http-clients/
    https://pawelmhm.github.io/asyncio/python/aiohttp/2016/04/22/asyncio-aiohttp.html

    >>> asyncio.get_event_loop()
    '''
    async with aiohttp.ClientSession() as session:
        headers = { 'Range': f'bytes=' + ','.join([f'{offset}-{int(offset)+int(length)-1}' for offset, length in sections]) }
        async with session.get(url, headers=headers) as response:
            return await response.content.read()

loop = asyncio.get_event_loop()
coroutines = [get("http://example.com", [(0, 20) for _ in range(3)])]
results = loop.run_until_complete(asyncio.gather(*coroutines))
print("results=",results)
asd
"""

################################################################################
# common crawl functions
################################################################################

def mk_cdxiter(cdxfiles, filter_mime=True, filter_status=True, filter_duplicates=True):
    '''
    Generator function that loops over the lines in the cdxfile.
    Each yielded entry is the json dictionary corresponding a cdxfile entry,
    but many of these cdxentries may be filtered out based on the other parameters.
    These filters help reduce the file size of downloaded warc files.
    '''
    url_counts = Counter()
    hostpaths_all = set()
    logging.info(f"cdx_iter()")
    for cdxfile in cdxfiles:
        logging.info(f'cdxfile={cdxfile}')
        hostpaths_cdx = set()
        with gzip.open(cdxfile, 'rt') as f:
            for line in f:

                # remove non-json content from start of line
                i = line.find(' ')
                line = line[i+1:]
                i = line.find(' ')
                line = line[i+1:]

                # extract the json
                data = json.loads(line)

                # run the filters
                if filter_mime and data.get('mime') != 'text/html':
                    url_counts['filter_mime'] += 1
                    continue

                if filter_status and data.get('status') != '200':
                    url_counts['filter_status'] += 1
                    continue

                if filter_duplicates:
                    url_parsed = urlparse(data['url'])
                    hostpath = url_parsed.hostname + url_parsed.path
                    if hostpath in hostpaths_cdx:
                        url_counts['filter_duplicates_cdx'] += 1
                        continue
                    elif hostpath in hostpaths_all:
                        url_counts['filter_duplicates_all'] += 1
                        continue
                    else:
                        hostpaths_all.add(hostpath)
                        hostpaths_cdx.add(hostpath)

                url_counts['no_filter'] += 1
                yield data

    # we've iterated over the data,
    # now we log information about the iteration before returning
    total_urls = sum(url_counts.values())
    for k,v in sorted(url_counts.items()):
        logging.info(f"url_counts['{k}'] = {v}  or  {100*v/total_urls:0.2f}%")


def cdxiter_to_warcitr(cdxiter, previous_batch_counter=-1, semsize=400, batchsize=1000):
    '''
    Iterates over the data in cdxiter in order to download the warc entries from common crawl;
    then combines these warc entries into a single warc file.
    '''

    # the get_warcfile function is a wrapper around the get function defined above
    # that uses a semaphore to ensure that only `semsize` calls can happen simultaneously
    sem = asyncio.Semaphore(semsize)
    async def get_warcfile(data):
        async with sem:
            url = 'https://commoncrawl.s3.amazonaws.com/' + data['filename']
            content = await get(url, data['offset'], data['length'])
            logging.debug(f"get('{url}', {data['offset']}, {data['length']})")
            return content

    # use an infinite loop to process the input cdxiter generator;
    # internally to the infinite loop we will process the generator in batches,
    # and break out of the loop when the batch size is 0
    last_time = time.time()
    last_mb_downloaded = 0
    mb_downloaded = 0
    urls_downloaded = 0
    for batch_counter in itertools.count():

        # skip batches that have already been downloaded
        if batch_counter <= previous_batch_counter:
            continue

        # get the batch
        batch = list(itertools.islice(cdxiter, batchsize))
        if len(batch) == 0:
            break
        urls_downloaded += len(batch)

        # process the next batch
        loop = asyncio.get_event_loop()
        batch_tasks = asyncio.wait([ asyncio.ensure_future(get_warcfile(data)) for data in batch ])
        done, pending = loop.run_until_complete(batch_tasks)
        for future in done:
            downloaded_warc_entry = future.result()
            mb_downloaded += len(downloaded_warc_entry)/(1024**2)
            yield downloaded_warc_entry

        # generate logging info
        mem = psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2
        curtime = time.time()
        rate = (mb_downloaded-last_mb_downloaded)/(curtime-last_time)
        logging.info(f"batch_counter={batch_counter}; urls_downloaded={urls_downloaded}; mem={mem:.2f}MB; mb_downloaded={mb_downloaded:.2f}MB; rate={rate:.2f}MB/sec")
        last_time = curtime
        last_mb_downloaded = mb_downloaded


def warcitr_to_warcfile(warcitr, out_filename, force=False):
    '''
    '''
    # if the force flag is not set, then we use 'xb' permissions,
    # which will fail if the file already exists;
    # otherwise we use 'wb' permissions to open the file,
    # which will truncate the existing file without an error
    if not force:
        permissions = 'xb'
    else:
        if os.path.exists(out_filename):
            logging.warning(f'out_filename exists, truncating: {out_filename}')
        permissions = 'wb'

    # load out_filename and write the warc entries
    with open(out_filename, permissions) as fwarc:
        for warc_entry in warcitr:
            fwarc.write(warc_entry)
            fwarc.flush()


def cdxiter_to_warcfile(cdxiter, out_filename, semsize=400, batchsize=1000):
    '''
    Iterates over the data in cdxiter in order to download the warc entries from common crawl;
    then combines these warc entries into a single warc file.
    '''

    # the get_warcfile function is a wrapper around the get function defined above
    # that uses a semaphore to ensure that only `semsize` calls can happen simultaneously
    sem = asyncio.Semaphore(semsize)
    async def get_warcfile(data):
        async with sem:
            url = 'https://commoncrawl.s3.amazonaws.com/' + data['filename']
            content = await get(url, data['offset'], data['length'])
            logging.debug(f"get('{url}', {data['offset']}, {data['length']})")
            return content

    # batch_counter_filename stores the total number of batches seen so far;
    # it is used to restart downloads that have been interrupted without having to redownload everything
    # here we load the contents of the file
    try:
        batch_counter_filename = out_filename + '.counter'
        with open(batch_counter_filename, 'r') as fcounter:
            previous_batch_counter = int(fcounter.read())
            logging.info(f'resuming download')
    except FileNotFoundError:
        previous_batch_counter = -1
    logging.info(f'previous_batch_counter = {previous_batch_counter}')

    # if out_filename already exists, and batch_counter_filename does not exist,
    # this indicates that we've finished downloading and there's nothing to do
    if os.path.isfile(out_filename) and previous_batch_counter == -1:
        logging.info(f'out_filename = {out_filename}')
        logging.info(f'download already completed, doing nothing')
        return

    with open(out_filename, 'wb') as fwarc:
        # use an infinite loop to process the input cdxiter generator;
        # internally to the infinite loop we will process the generator in batches,
        # and break out of the loop when the batch size is 0
        last_time = time.time()
        last_filesize = 0
        for batch_counter in itertools.count():

            # skip batches that have already been downloaded
            if batch_counter <= previous_batch_counter:
                continue

            # generate logging info
            mem = psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2
            filesize = os.path.getsize(out_filename) / 1024**2
            curtime = time.time()
            rate = (filesize-last_filesize)/(curtime-last_time)
            logging.info(f"batch_counter={batch_counter}; urls_downloaded={batchsize*batch_counter}; mem={mem:.2f}MB; filesize={filesize:.2f}MB; rate={rate:.2f}MB/sec")
            last_time = curtime
            last_filesize = filesize

            # process the next batch
            batch = list(itertools.islice(cdxiter, batchsize))
            if len(batch) == 0:
                break
            loop = asyncio.get_event_loop()
            batch_tasks = asyncio.wait([ asyncio.ensure_future(get_warcfile(data)) for data in batch ])
            done, pending = loop.run_until_complete(batch_tasks)
            for future in done:
                downloaded_warc_entry = future.result()
                fwarc.write(downloaded_warc_entry)
            fwarc.flush()

            # update the batch counter file
            # NOTE:
            # ideally, this update and the writes to fwarc should be done atomically;
            # unfortunately, that's not easily doable,
            # so if the program crashes between writing to fwarc and fcounter,
            # then future invocations of the program will possibly miss this batch,
            # or the gzipped output is likely to be corrupted
            with open(batch_counter_filename, 'wt') as fcounter:
                fcounter.write(str(batch_counter))

    # we've finished downloading, so we should remove the counter file
    os.remove(batch_counter_filename)


def download_warc(surt, *, crawl=None, data_dir='/data/common-crawl', force=False, dryrun=False):
    '''
    Constructs a warc file that contains all useful urls from the given surt/crawl combination.

    crawl:
        If crawl is None, then it will use the entirety of the common crawl;
        If crawl is a specific crawl name, then it will only generate a warc file for that crawl.
    '''

    # compute the output filename
    warcfile = data_dir + f'/warc_new/{surt}-{crawl}.warc.gz'

    # make output directory if it doesn't exist
    try:
        os.mkdir(os.path.dirname(warcfile))
    except FileExistsError:
        pass

    # create an iterator over the cdx file(s)
    if crawl:
        cdx_paths = [data_dir + f'/cdx/{surt}/{surt}-{crawl}.cdx.gz']
    else:
        # when no crawl is specified, we search the data dir for all crawls;
        # we sort the crawls from oldest to newest;
        # this way, when if discard duplicates in downstream steps,
        # we are discarding the older crawls
        dirpath = data_dir + f'/cdx/{surt}/'
        cdx_paths = [ dirpath + filename for filename in os.listdir(dirpath) ]
        cdx_paths.sort(reverse=True)
    cdxiter = mk_cdxiter(cdx_paths)

    # in a dryrun, we'll just process the cdxiter;
    # this will log statistics about what the actual run would compute,
    # but will not download the data
    if dryrun:
        list(cdxiter)

    # not a dryrun, so actually download the data
    else:
        #cdxiter_to_warcfile(cdxiter, warcfile)
        warcitr = cdxiter_to_warcitr(cdxiter)
        warcitr_to_warcfile(warcitr, warcfile, force)


################################################################################
# standalone executable code
################################################################################

if __name__ == '__main__':

    # setup logging
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        )

    # run the downloader
    from clize import run
    run(download_warc)

