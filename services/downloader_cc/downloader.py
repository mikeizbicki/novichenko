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
import math
import os
import psycopg
import random
#import sqlalchemy
#import sqlalchemy.exc
import time
import traceback

import chajda.tsvector
import chajda.embeddings
import metahtml
import metahtml.adblock

from urllib.parse import urlparse
from collections import Counter

################################################################################
# surt helper functions
################################################################################

# FIXME:
# these functions are directly adapted from the schema.sql file;
# it's super annoying that we are duplicating this functionality in two languages;
# probably we should make python's surt library the definitive choice,
# and then just create SQL bindings to this library

def host_simplify(host):
    '''
    >>> host_simplify('cnn.com')
    'cnn.com'
    >>> host_simplify('www.cnn.com')
    'cnn.com'
    >>> host_simplify('www2.cnn.com')
    'cnn.com'
    >>> host_simplify('www5.cnn.com')
    'cnn.com'
    >>> host_simplify('www577.cnn.com')
    'cnn.com'
    >>> host_simplify('bbc.co.uk')
    'bbc.co.uk'
    >>> host_simplify('www.bbc.co.uk')
    'bbc.co.uk'
    >>> host_simplify('en.wikipedia.org')
    'en.wikipedia.org'
    >>> host_simplify('m.wikipedia.org')
    'wikipedia.org'
    >>> host_simplify('naenara.com.kp')
    'naenara.com.kp'
    '''
    m = re.match(r'^www\d*\.(.*)', host)
    if m and m.group(1):
        return m.group(1)
    m = re.match(r'^m\.(.*)', host)
    if m and m.group(1):
        return m.group(1)
    return host


def url_host_surt(url):
    '''
    >>> url_host_surt('https://example.com')
    'com,example)'
    >>> url_host_surt('https://example.com/')
    'com,example)'
    >>> url_host_surt('https://example.com/#test')
    'com,example)'
    >>> url_host_surt('https://example.com/?param=12')
    'com,example)'
    >>> url_host_surt('https://example.com/path/to')
    'com,example)'
    >>> url_host_surt('https://example.com/path/to/')
    'com,example)'
    >>> url_host_surt('https://example.com/path/to/#test')
    'com,example)'
    >>> url_host_surt('https://example.com/path/to/?param=12')
    'com,example)'
    >>> url_host_surt('https://Example.com/Path/To/?Param=12')
    'com,example)'
    '''
    url_parsed = urlparse(url)
    host_surt = ','.join(reversed(host_simplify(url_parsed.hostname).split('.'))).lower() + ')'
    return host_surt


def url_hostpath_surt(url):
    '''
    >>> url_hostpath_surt('https://example.com')
    'com,example)'
    >>> url_hostpath_surt('https://example.com/')
    'com,example)'
    >>> url_hostpath_surt('https://example.com/#test')
    'com,example)'
    >>> url_hostpath_surt('https://example.com/?param=12')
    'com,example)'
    >>> url_hostpath_surt('https://example.com/path/to')
    'com,example)/path/to'
    >>> url_hostpath_surt('https://example.com/path/to/')
    'com,example)/path/to'
    >>> url_hostpath_surt('https://example.com/path/to/#test')
    'com,example)/path/to'
    >>> url_hostpath_surt('https://example.com/path/to/?param=12')
    'com,example)/path/to'
    >>> url_hostpath_surt('https://Example.com/Path/To/?Param=12')
    'com,example)/path/to'
    '''
    url_parsed = urlparse(url)
    host_surt = ','.join(reversed(host_simplify(url_parsed.hostname).split('.'))).lower() + ')'
    path = url_parsed.path.lower()
    if len(path)>0 and path[-1] == '/':
        path = path[:-1]
    return host_surt + path


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


def warcitr_to_recorditr(warcitr):
    warcio_loader = ArcWarcRecordLoader()
    for warc_entry in warcitr:
        try:
            stream = io.BytesIO(warc_entry)
            with gzip.open(stream) as f:
                record = warcio_loader.parse_record_stream(f)
                yield record
        except gzip.BadGzipFile:
            logging.warning('gzip.BadGzipFile')


def pg_sourceinfo(connection, source_name):
    '''
    Returns information from the database's source table about source_name.
    This information can then be used to skip previously completed downloads,
    or resume unfinished downloads.
    '''
    with connection.cursor() as cursor:

        # create a new entry in the source table for this warc file if no entry exists
        try:
           #sql = sqlalchemy.sql.text('''
           #INSERT INTO source (name) VALUES (:name) RETURNING id;
           #''')
           #res = connection.execute(sql,{'name':source_name})
            sql = '''
            INSERT INTO source (name) VALUES (%(name)s) RETURNING id;
            '''
            cursor.execute(sql, {'name':source_name})
            row = cursor.fetchone()
            sourceinfo = {
                'id_source': row[0],
                'urls_inserted': 0,
                'finished_at': None,
                }

        # if an entry already exists in source
        except psycopg.errors.UniqueViolation: # FIXME: make this error more specific?

            logging.debug(f"name='{source_name}' exists in source")

            # get info from the source table about previous runs
            sql = '''
            SELECT id,urls_inserted,finished_at FROM source WHERE name=%(name)s;
            '''
            cursor.execute(sql, {'name':source_name})
            row = cursor.fetchone()
            sourceinfo = {
                'id_source': row[0],
                'urls_inserted': row[1],
                'finished_at': row[2],
                }

    logging.debug(f'source_name={source_name}, id_source={sourceinfo["id_source"]}')
    return sourceinfo


def recorditr_to_pg(recorditr, connection, id_source, metahtml_max_recall=False, batch_size=100):
    '''
    Insert each record in recorditr into the database.
    This function will create a new entry in the source table if source_name does not already exist.
    If source_name already exists,
    then the existing entry will be used to skip the first records in recorditr to prevent duplicates from being inserted.

    FIXME:
    WARC entries are already downloaded from the common crawl by the time we are skipping them in the recorditr here.
    This means that we don't actually save any time/bandwidth by doing the skip.
    '''
    
    # for efficiency, we will not insert items into the db one at a time;
    # instead, we add them to the batch list,
    # and then bulk insert the batch list when it reaches len(batch)==batch_size
    batch = []
    for record_i,record in enumerate(recorditr):

        # extract the contents of the WARC record
        html = record.content_stream().read()
        url = record.rec_headers.get_header('WARC-Target-URI')
        accessed_at = record.rec_headers.get_header('WARC-Date')
        if html is None or url is None or accessed_at is None:
            logging.error(f'invalid values found in WARC record; html is None={html is None}, url={url}, accessed_at={accessed_at}')
            continue

        # we're now committed to processing this url, and we log that fact
        logging.debug(f'processing url={url}')

        # extract the meta
        try:
            if metahtml_max_recall:
                meta = metahtml.parse(html, url, extractor_config=metahtml.ExtractorConfig_recall)
            else:
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
    with connection.cursor() as cursor:
        sql = '''
        UPDATE source SET finished_at=now() where id=%(id)s;
        '''
        cursor.execute(sql, {'id':id_source})


def bulk_insert(connection, id_source, batch):
    '''
    Converts batch (a list of dictionaries containing information for the metahtml table) into a single insert statement;
    then, connects to the db and runs this insert.
    This is marginally more efficient than performing multiple inserts independently
    (it results in fewer updates of the source table),
    but I'm honestly not sure if it is worth the additional code complexity.
    The batching was done for historical reasons;
    old versions of the code base benefited dramatically from batching.
    '''

    # compute the entries for the metahtml_view table
    batch_view = []
    batch_contextvector = []
    for item in batch:
        meta = json.loads(item['jsonb'])
        url = item['url']
        try:
            lang_iso = meta['language']['best']['value'][:2]
            timestamp_published = meta['timestamp.published']['best']['value']['lo']
            tsv_title = chajda.tsvector.lemmatize(lang_iso, meta['title']['best']['value'])
            tsv_content = chajda.tsvector.lemmatize(lang_iso, meta['content']['best']['value']['text'])
            title = meta['title']['best']['value']
            description = meta['description']['best']['value']
            content = meta['content']['best']['value']['html']
            meta_has_info = True
        # whenever the url does not correspond to an article, the code above throws an exception;
        # in this case, we won't be adding to the batch_view or batch_contextvector variables
        except (TypeError,KeyError):
            logging.debug(f'no lang/title/content for url={url}')
            meta_has_info = False

        if meta_has_info:
            batch_view.append({
                'host_surt': url_host_surt(url),
                'hostpath_surt': url_hostpath_surt(url),
                'language': lang_iso,
                'timestamp_published': timestamp_published,
                'title': title,
                'description': description,
                'content': content,
                'tsv_title': tsv_title,
                'tsv_content': tsv_content,
                })
            embedding = chajda.embeddings.get_embedding(lang=lang_iso, max_n=400000, max_d=None, storage_dir='./embeddings')
            contextvectors = chajda.tsvector.tsvector_to_contextvectors(embedding, tsv_content, n=2, normalize=False)
            for focus,[context,count] in contextvectors.items():
                batch_contextvector.append({
                    'host_surt': url_host_surt(url),
                    'hostpath_surt': url_hostpath_surt(url),
                    'timestamp_published': timestamp_published,
                    'context': context / math.sqrt(count),
                    'count': 1, # FIXME: this is the number of urls not the number of words; should we fix that?
                    'focus': focus,
                    'language': lang_iso,
                    })

    # wrap the actual insert in an infinite loop;
    # the insert code can deadlock due to unique constraints,
    # and we will keep attempting to insert with exponential backoff until the insert actually works
    for attempt_count in itertools.count():
        try:
            # enter a transaction so that we update both the metahtml tables and the source table consistently
            with connection.transaction():
              with connection.cursor() as cursor:

                # update urls_inserted in the source table
                sql = '''
                SELECT urls_inserted FROM source WHERE id=%(id_source)s FOR UPDATE;
                '''
                cursor.execute(sql, {'id_source':id_source})
                row = cursor.fetchone()
                urls_inserted = row[0]

                sql = '''
                UPDATE source SET urls_inserted=%(urls_inserted)s WHERE id=%(id_source)s;
                '''
                cursor.execute(sql,{'id_source':id_source, 'urls_inserted':urls_inserted+len(batch)})

                # log our update
                logging.info(f'bulk_insert: id_source={id_source}, urls_inserted={urls_inserted}, len(batch)={len(batch)}, len(batch_view)={len(batch_view)}, len(batch_contextvector)={len(batch_contextvector)}')

                def copy_from_batch(tablename, batch):
                    if len(batch) > 0:
                        keys = sorted(batch[0].keys())
                        sql = 'COPY '+tablename+'('+','.join(keys)+') FROM STDIN'
                        with cursor.copy(sql) as copy:
                            for record in batch:
                                copy.write_row([record[key] for key in keys])

                from psycopg.types.string import StrDumper
                import numpy as np
                class NumpyDumper(StrDumper):
                    def dump(self, obj):
                        return super().dump('['+str(obj.tolist())[1:-1]+']')
                cursor.adapters.register_dumper(np.ndarray, NumpyDumper)

                # insert into metahtml
                copy_from_batch('metahtml', batch)
                copy_from_batch('metahtml_view', batch_view)
                copy_from_batch('contextvector', batch_contextvector)
                """
                with cursor.copy('COPY metahtml(accessed_at,id_source,url,jsonb) FROM STDIN') as copy:
                    for record in batch:
                        copy.write_row([record['accessed_at'],record['id_source'],record['url'],record['jsonb']])

                # insert into metahtml_view
                if len(batch_view) > 0:
                    sql = sqlalchemy.sql.text(f'''
                        INSERT INTO metahtml_view (timestamp_published, host_surt, hostpath_surt, language, title, description, content, tsv_title, tsv_content) VALUES'''+
                        ','.join([f'(:timestamp_published{i}, url_host_surt(:url{i}), url_hostpath_surt(:url{i}), language_iso639(:language{i}), :title{i}, :description{i}, :content{i}, :tsv_title{i}, :tsv_content{i})' for i in range(len(batch_view))])
                        + 'ON CONFLICT DO NOTHING'
                        )
                    res = connection.execute(sql,{
                        key+str(i) : d[key]
                        for i,d in enumerate(batch_view)
                        for key in d.keys()
                        })

                # insert into contextvector
                if len(batch_contextvector) > 0:
                    sql = sqlalchemy.sql.text(f'''
                        INSERT INTO contextvector (context, timestamp_published, count, host_surt, hostpath_surt, language, focus) VALUES'''+
                        ','.join([f'(:context{i}, :timestamp_published{i}, 1, url_host_surt(:url{i}), url_hostpath_surt(:url{i}), language_iso639(:language{i}), :focus{i})' for i in range(len(batch_contextvector))])
                        + 'ON CONFLICT DO NOTHING'
                        )
                    res = connection.execute(sql,{
                        key+str(i) : d[key]
                        for i,d in enumerate(batch_contextvector)
                        for key in d.keys()
                        })
                """

                # if we've made it to this point in the code,
                # the insert was successful,
                # and we return from the function
                return

        # in the event of deadlock, perform the exponential backoff
        # FIXME:
        # I don't understand why the InternalError_ gets thrown sometimes
        except (psycopg.errors.DeadlockDetected,psycopg.errors.InternalError_) as e:
            sleep_time = min(2**attempt_count, 60*5) + random.random()*10
            logging.exception(f'{type(e)}, sleep_time={sleep_time}')
            time.sleep(sleep_time)


################################################################################
# common crawl functions
################################################################################

def mk_cdxiter(cdxfiles, filter_mime=True, filter_status=True, filter_duplicates=True, max_urls_to_download=None):
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
                   #url_parsed = urlparse(data['url'])
                   #url_parsed.hostname + url_parsed.path
                    hostpath = url_hostpath_surt(data['url'])
                    if hostpath in hostpaths_cdx:
                        url_counts['filter_duplicates_cdx'] += 1
                        continue
                    elif hostpath in hostpaths_all:
                        url_counts['filter_duplicates_all'] += 1
                        continue
                    else:
                        hostpaths_all.add(hostpath)
                        hostpaths_cdx.add(hostpath)

                # the data was not filtered, so yield it
                url_counts['no_filter'] += 1
                yield data

                logging.debug(f'url_counts["no_filter"]={url_counts["no_filter"]} max_urls_to_download={max_urls_to_download}')

                # check if we should early stop
                if max_urls_to_download is not None and url_counts['no_filter'] > max_urls_to_download:
                    logging.info('stopped cdx_iter() due to reaching max_urls_to_download')
                    return

    # we've iterated over the data,
    # now we log information about the iteration before returning
    total_urls = sum(url_counts.values())
    for k,v in sorted(url_counts.items()):
        logging.info(f"url_counts['{k}'] = {v}  or  {100*v/total_urls:0.2f}%")


def cdxiter_to_warcitr(cdxiter, semsize=100, batchsize=100):
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
            return url,content

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
        warc_records = []
        for future in done:
            url,warc_record = future.result()
            mb_downloaded += len(warc_record)/(1024**2)
            warc_records.append([url,warc_record])
        warc_records.sort()
        for url,warc_record in warc_records:
            yield warc_record

        # generate logging info
        mem = psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2
        curtime = time.time()
        rate = (mb_downloaded-last_mb_downloaded)/(curtime-last_time)
        logging.info(f"batch_counter={batch_counter}; urls_downloaded={urls_downloaded}; mem={mem:.2f}MB; mb_downloaded={mb_downloaded:.2f}MB; rate={rate:.2f}MB/sec")
        last_time = curtime
        last_mb_downloaded = mb_downloaded


def warcitr_to_warcfile(warcitr, out_filename, force=False):
    '''
    Saves all the entries in warcitr to a file.

    If the force flag is not set, then we use 'xb' permissions on the loaded file,
    which will fail if the file already exists.
    Otherwise, we use 'wb' permissions to open the file,
    which will truncate the existing file without an error.
    '''
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


def download_warc(surt, *, worker=0, num_workers=1, max_urls_to_download:int=None, write_warcfile=False, load_pg=False, crawl=None, data_dir='/data/common-crawl', force=False, dryrun=False, metahtml_max_recall=False):
    '''
    Constructs a warc file that contains all useful urls from the given surt/crawl combination.

    crawl:
        If crawl is None, then it will use the entirety of the common crawl;
        If crawl is a specific crawl name, then it will only generate a warc file for that crawl.
    '''
    
    # create database connection
    import sqlalchemy
    dburl = f'postgresql://{os.environ["POSTGRES_USER"]}:{os.environ["POSTGRES_PASSWORD"]}@pg:5432/{os.environ["POSTGRES_NAME"]}'
   #engine = sqlalchemy.create_engine(dburl, connect_args={
   #    'application_name': 'metahtml',
   #    'connect_timeout': 60*60
   #    })  
   #connection = engine.connect()
    connection = psycopg.connect(dburl, autocommit=True)

    # download sourceinfo from db
    warcfile = data_dir + f'/warc_new2/{surt}-{crawl}-{worker:04}-of-{num_workers:04}.warc.gz'
    sourceinfo = pg_sourceinfo(connection, warcfile)

    # if finished_at has a timestamp, then we've already fully processed the file and can skip it
    if sourceinfo['finished_at'] is not None:
        logging.info(f'finished_at is {sourceinfo["finished_at"]}, skipping')
        return

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
        # this way, when we discard duplicates in downstream steps,
        # we are discarding the older crawls
        dirpath = data_dir + f'/cdx/{surt}/'
        cdx_paths = [ dirpath + filename for filename in os.listdir(dirpath) ]

        # NOTE:
        # the sort order here determines the order that urls will be added to the database;
        # this is important because duplicate urls will be skipped;
        # there are two options:
        # 1. sorting from lowest to highest (reverse=False) causes the first crawl of a url to be added;
        #    the advantage here is that we know that no information from the "future" (e.g. ads, recent article links) 
        #    will leak into the past;
        # 2. sorting from highest to lowest (reverse=True) causes the most recent crawl to be added;
        #    the advantage here is that websites occasionally change styles,
        #    and the more recent styles are more likely to follow standards that make extracting content more reliable;
        #    also, it can simplify testing since we'll get a broad range of dates in the db quickly
        #
        cdx_paths.sort(reverse=False)
    cdxiter = mk_cdxiter(cdx_paths, max_urls_to_download=max_urls_to_download)

    # filter the cdxiter so that only the entries applicable to the current worker are traversed
    cdxiter = (x for i,x in enumerate(cdxiter) if i%num_workers == worker)

    # skip urls from cdxiter that have already been added
    cdxiter = itertools.islice(cdxiter, sourceinfo['urls_inserted'], None)

    # in a dryrun, we'll just process the cdxiter;
    # this will log statistics about what the actual run would compute,
    # but will not download the data
    if dryrun:
        list(cdxiter)

    # not a dryrun, so actually download the data
    else:
        warcitr = cdxiter_to_warcitr(cdxiter)
        if write_warcfile:
            warcitr = warcitr_to_warcfile(warcitr, warcfile, force)
        if load_pg:
            recorditr = warcitr_to_recorditr(warcitr)
            recorditr_to_pg(recorditr, connection, sourceinfo['id_source'], metahtml_max_recall=metahtml_max_recall)


################################################################################
# standalone executable code
################################################################################

if __name__ == '__main__':

    # setup logging
    logging.basicConfig(
        format='%(asctime)s:%(levelname)s:%(name)s:%(message)s',
        level=logging.INFO,
        force=True,
        )

    # run the downloader
    from clize import run
    run(download_warc)

