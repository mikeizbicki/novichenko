#!/usr/bin/python3
'''

'''

import aiohttp
import asyncio

import itertools
import json
import gzip
import logging
import re
import psutil
import os
import psycopg2
import sqlalchemy
import time
import traceback

import chajda.tsvector
import metahtml
import metahtml.adblock

from urllib.parse import urlparse
from collections import Counter

################################################################################
# async downloader
################################################################################

async def get(url, offset, length):
    '''
    Downloads only the `length` bytes of the data at `url` starting at position `offset`.

    RFC2616 specifies how to use HTTP headers to specify which bytes to download from the file.
    For details, see: https://datatracker.ietf.org/doc/html/rfc2616#section-14.35

    For details on writing fast async http clients, see:
    https://julien.danjou.info/python-and-fast-http-clients/
    https://pawelmhm.github.io/asyncio/python/aiohttp/2016/04/22/asyncio-aiohttp.html

    '''

    for failures in itertools.count():
        try:
            async with aiohttp.ClientSession() as session:
                headers = { 'Range': f'bytes={offset}-{int(offset)+int(length)-1}' }
                async with session.get(url, headers=headers) as response:
                    return await response.content.read()
        except (aiohttp.client_exceptions.ClientConnectorError, asyncio.exceptions.TimeoutError) as e:
            sleep_time = 2**failures
            logging.warning(f'exception={e}; sleep_time={sleep_time}')
            await asyncio.sleep(sleep_time)


################################################################################
# postgres functions
################################################################################

from warcio.recordloader import ArcWarcRecordLoader
import io

warcio_loader = ArcWarcRecordLoader()

def warcitr_to_recorditr(warcitr):
    for warc_entry in warcitr:
        try:
            stream = io.BytesIO(warc_entry)
            with gzip.open(stream) as f:
                record = warcio_loader.parse_record_stream(f)
                yield record
        except gzip.BadGzipFile:
            logging.warning('gzip.BadGzipFile')


def recorditr_to_pg(recorditr, connection, source_name, batch_size=100):
    '''
    Insert each record in recorditr into the database.
    This function will create a new entry in the source table if source_name does not already exist.
    If source_name already exists,
    then the existing entry will be used to skip the first records in recorditr to prevent duplicates from being inserted.

    FIXME:
    WARC entries are already downloaded from the common crawl by the time we are skipping them in the recorditr here.
    This means that we don't actually save any time/bandwidth by doing the skip.
    '''
    
    # create a new entry in the source table for this warc file if no entry exists
    try:
        sql = sqlalchemy.sql.text('''
        INSERT INTO source (name) VALUES (:name) RETURNING id;
        ''')
        res = connection.execute(sql,{'name':source_name})
        id_source = res.first()['id']
        finished_at = None
        urls_inserted = 0

    # if an entry already exists in source
    except sqlalchemy.exc.IntegrityError:

        logging.info(f"name='{source_name}' exists in source")

        # get info from the source table about previous runs
        sql = sqlalchemy.sql.text('''
        SELECT id,urls_inserted,finished_at FROM source WHERE name=:name;
        ''')
        res = connection.execute(sql,{'name':source_name})
        row = res.first()
        id_source = row['id']
        finished_at = row['finished_at']
        urls_inserted = row['urls_inserted']

        # if finished_at has a timestamp, then we've already fully processed the file and can skip it
        if finished_at is not None:
            logging.info(f'finished_at is {finished_at}, skipping')
            return

    logging.debug(f'id_source={id_source}')

    # for efficiency, we will not insert items into the db one at a time;
    # instead, we add them to the batch list,
    # and then bulk insert the batch list when it reaches len(batch)==batch_size
    batch = []
    for record_i,record in enumerate(recorditr):

        '''
        # skip WARC entries that are not responses
        if record.rec_type != 'response':
            logging.debug(f'skip record.rec_type={record.rec_type}')
            continue

        # skip WARC responses that are not successful (status code 2XX)
        if record.http_headers.statusline[0] != '2':
            logging.debug(f'skip statusline={record.http_headers.statusline} url={record.rec_headers.get_header("WARC-Target-URI")}')
            continue

        # skip WARC responses that are not text/html
        headers = dict(record.http_headers.headers)
        content_type = headers.get('Content-Type','')
        if 'html' not in content_type and 'text' not in content_type and len(content_type)>0:
            logging.debug(f'skip content_type={content_type} url={record.rec_headers.get_header("WARC-Target-URI")}')
            continue
        '''

        # extract the contents of the WARC record
        html = record.content_stream().read()
        url = record.rec_headers.get_header('WARC-Target-URI')
        accessed_at = record.rec_headers.get_header('WARC-Date')
        if html is None or url is None or accessed_at is None:
            logging.error(f'invalid values found in WARC record; html is None={html is None}, url={url}, accessed_at={accessed_at}')
            continue

        # skip responses that have already been added
        if record_i < urls_inserted:
            logging.debug(f'skip already inserted record_i={record_i} url={record.rec_headers.get_header("WARC-Target-URI")}')
            continue

        # we're now committed to processing this url, and we log that fact
        logging.debug(f'processing url={url}')

        # extract the meta
        try:
            meta = metahtml.parse(html, url)

        # if there was an error in metahtml, log it
        except Exception as e:
            logging.exception(f'exception when calling metahtml.parse() on url={url}')
            meta = { 
                'exception' : {
                    'str(e)' : str(e),
                    'type' : type(e).__name__,
                    'location' : 'metahtml',
                    'traceback' : traceback.format_exc()
                    }
                }

        # add the results to the batch
        meta_json = json.dumps(meta, default=str)
        batch.append({
            'accessed_at' : accessed_at,
            'id_source' : id_source,
            'url' : url,
            'jsonb' : meta_json,
            })

        # bulk insert the batch
        if len(batch)>=batch_size:
            bulk_insert(connection, id_source, batch)
            batch = []

    # we have finished looping over the recorditr;
    # we should bulk insert everything in the batch list that hasn't been inserted
    if len(batch)>0:
        bulk_insert(connection, id_source, batch)

    # finished loading the file, so update the source table
    sql = sqlalchemy.sql.text('''
    UPDATE source SET finished_at=now() where id=:id;
    ''')
    res = connection.execute(sql,{'id':id_source})


def bulk_insert(connection, id_source, batch):

    # compute the entries for the metahtml_view table
    batch_view = []
    for item in batch:
        meta = json.loads(item['jsonb'])
        url = item['url']
        try:
            lang_iso = meta['language']['best']['value'][:2]
            batch_view.append({
                'url' : url,
                'language' :  meta['language']['best']['value'],
                'timestamp_published' : meta['timestamp.published']['best']['value']['lo'],
                'title' : meta['title']['best']['value'],
                'description' : meta['description']['best']['value'],
                'content' : meta['content']['best']['value']['html'],
                'tsv_title' : chajda.tsvector.lemmatize(lang_iso, meta['title']['best']['value']),
                'tsv_content' : chajda.tsvector.lemmatize(lang_iso, meta['content']['best']['value']['text']),
                })
        except (TypeError,KeyError):
            logging.debug(f'no lang/title/content for url={url}')

    # wrap the actual insert in an infinite loop;
    # the insert code can deadlock due to unique constraints,
    # and we will keep attempting to insert with exponential backoff until the insert actually works
    for attempt_count in itertools.count():
        try:
            # enter a transaction so that we update both the metahtml tables and the source table consistently
            with connection.begin():

                # update urls_inserted in the source table
                sql = sqlalchemy.sql.text('''
                SELECT urls_inserted FROM source WHERE id=:id_source FOR UPDATE;
                ''')
                res = connection.execute(sql,{'id_source':id_source})
                urls_inserted = res.first()['urls_inserted']

                sql = sqlalchemy.sql.text('''
                UPDATE source SET urls_inserted=:urls_inserted WHERE id=:id_source;
                ''')
                res = connection.execute(sql,{'id_source':id_source, 'urls_inserted':urls_inserted+len(batch)})

                # log our update
                logging.info(f'bulk_insert: id_source={id_source}, urls_inserted={urls_inserted}, len(batch)={len(batch)}, len(batch_view)={len(batch_view)}')

                # insert into metahtml
                keys = ['accessed_at', 'id_source', 'url', 'jsonb']
                sql = sqlalchemy.sql.text(f'''
                    INSERT INTO metahtml ({','.join(keys)}) VALUES'''+
                    ','.join(['(' + ','.join([f':{key}{i}' for key in keys]) + ')' for i in range(len(batch))])
                    )
                res = connection.execute(sql,{
                    key+str(i) : d[key]
                    for key in keys
                    for i,d in enumerate(batch)
                    })

                # insert into metahtml_view
                if len(batch_view) > 0:
                    sql = sqlalchemy.sql.text(f'''
                        INSERT INTO metahtml_view (timestamp_published, hostpath_surt, language, title, description, content, tsv_title, tsv_content) VALUES'''+
                        ','.join([f'(:timestamp_published{i}, url_hostpath_surt(:url{i}), language_iso639(:language{i}), :title{i}, :description{i}, :content{i}, :tsv_title{i}, :tsv_content{i})' for i in range(len(batch_view))])
                        + 'ON CONFLICT DO NOTHING'
                        )
                    res = connection.execute(sql,{
                        key+str(i) : d[key]
                        for i,d in enumerate(batch_view)
                        for key in d.keys()
                        })

                # if we've made it to this point in the code,
                # the insert was successful,
                # and we return from the function
                return

        # in the event of deadlock, perform the exponential backoff
        except psycopg2.errors.DeadlockDetected:
            sleep_time = 2**attempt_count
            logging.error(f'psycopg2.errors.DeadlockDetected, sleep_time={sleep_time}')
            time.sleep(sleep_time)


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


def cdxiter_to_warcitr(cdxiter, semsize=400, batchsize=1000):
    '''
    Iterates over the data in cdxiter in order to download the warc entries from common crawl;
    then combines these warc entries into a single warc file.
    '''

    # the get_warcfile function is a wrapper around the get function defined above
    # that uses a semaphore to ensure that only `semsize` calls can happen simultaneously
    import asyncio
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
            yield warc_entry


def download_warc(surt, *, worker=0, num_workers=1, write_warcfile=False, load_pg=False, crawl=None, data_dir='/data/common-crawl', force=False, dryrun=False):
    '''
    Constructs a warc file that contains all useful urls from the given surt/crawl combination.

    crawl:
        If crawl is None, then it will use the entirety of the common crawl;
        If crawl is a specific crawl name, then it will only generate a warc file for that crawl.
    '''

    # compute the output filename
    warcfile = data_dir + f'/warc_new2/{surt}-{crawl}-{worker:04}-of-{num_workers:04}.warc.gz'

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

    # filter the cdxiter so that only the entries applicable to the current worker are traversed
    cdxiter = (x for i,x in enumerate(cdxiter) if i%num_workers == worker)

    # in a dryrun, we'll just process the cdxiter;
    # this will log statistics about what the actual run would compute,
    # but will not download the data
    if dryrun:
        list(cdxiter)

    # not a dryrun, so actually download the data
    else:

        # stream the iterators
        warcitr = cdxiter_to_warcitr(cdxiter)

        if write_warcfile:
            warcitr = warcitr_to_warcfile(warcitr, warcfile, force)

        # load into the database
        if load_pg:
            # create database connection
            import sqlalchemy
            dburl = f'postgresql://{os.environ["POSTGRES_USER"]}:{os.environ["POSTGRES_PASSWORD"]}@pg:5432/{os.environ["POSTGRES_NAME"]}'
            engine = sqlalchemy.create_engine(dburl, connect_args={
                'application_name': 'metahtml',
                'connect_timeout': 60*60
                })  
            connection = engine.connect()

            # load the records
            recorditr = warcitr_to_recorditr(warcitr)
            recorditr_to_pg(recorditr, connection, warcfile)

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

