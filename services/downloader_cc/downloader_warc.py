#!/usr/bin/python3
'''
'''

# the sys import is needed so that we can import from the current project
import sys
sys.path.append('.')
import metahtml

# load imports
import datetime
import gzip
import json
import logging
import os
import sqlalchemy
import traceback
from warcio.archiveiterator import ArchiveIterator

import chajda.tsvector

def lemmas_to_ngrams(n, lemmas):
    '''
    lemmas is expected to be the output of chajda.tsvector.lemmatize

    >>> lemmas_to_ngrams(3, 'trouble:1 in:2 pyongyang:3   38:5 north:6 informed:7 analysis:8 of:9 north:10 korea:11')
    ['trouble in', 'in pyongyang', 'trouble in pyongyang', '38 north', 'north informed', '38 north informed', 'informed analysis', 'north informed analysis', 'analysis of', 'informed analysis of', 'of north', 'analysis of north', 'north korea', 'of north korea']
    '''
    if lemmas is None:
        return None

    from collections import deque

    # grams will store all of the ngrams that we have found so far
    grams = []

    # prevs will store the previous n lemmas in the input lemmas;
    # maintaining this deque separately allows us to compute ngrams in a single pass
    prevs = deque()

    # loop over each lemma in the input
    for lemma in lemmas.split():

        # extract the token and position
        token,position_str = lemma.split(':')
        position = int(position_str)

        # compute the ngrams
        gram = token
        prev_position = position
        for i in range(min(n,len(prevs))):
            if prevs[i][1] == prev_position - 1:
                prev_position = prevs[i][1]
                gram = prevs[i][0] + ' ' + gram
                grams.append(gram)
            else:
                continue

        # maintain the prevs deque
        prevs.appendleft((token,position))
        if len(prevs) >= n:
            prevs.pop()
    
    return grams


def insert_warc(connection, warc_path, batch_size=100):
    '''
    '''
    
    logging.info(f'insert_warc(warc_path={warc_path})')

    # create a new entry in the source table for this warc file if no entry exists
    try:
        sql = sqlalchemy.sql.text('''
        INSERT INTO source (name) VALUES (:name) RETURNING id;
        ''')
        res = connection.execute(sql,{'name':warc_path})
        id_source = res.first()['id']
        finished_at = None
        urls_inserted = 0

    # if an entry already exists in source
    except sqlalchemy.exc.IntegrityError:

        logging.info(f'warc_path exists in source')

        # get info from the source table about previous runs
        sql = sqlalchemy.sql.text('''
        SELECT id,urls_inserted,finished_at FROM source WHERE name=:name;
        ''')
        res = connection.execute(sql,{'name':warc_path})
        row = res.first()
        id_source = row['id']
        finished_at = row['finished_at']
        urls_inserted = row['urls_inserted']

        # if finished_at has a timestamp, then we've already fully processed the file and can skip it
        if finished_at is not None:
            logging.info(f'finished_at is {finished_at}, skipping')
            return

    logging.debug(f'id_source={id_source}')

    # load the warc file
    with open(warc_path, 'rb') as stream:

        # for efficiency, we will not insert items into the db one at a time;
        # instead, we add them to the batch list,
        # and then bulk insert the batch list when it reaches len(batch)==batch_size
        batch = []

        # loop over each record in the WARC file;
        # if the input is an ARC file (e.g. older common crawl archives), convert to WARC implicitly
        for record_i,record in enumerate(ArchiveIterator(stream, arc2warc=True)):

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

        # we have finished looping over the archive;
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
                'content' : lang_iso, meta['content']['best']['value']['html'],
                'tsv_title' : chajda.tsvector.lemmatize(lang_iso, meta['title']['best']['value']),
                'tsv_content' : chajda.tsvector.lemmatize(lang_iso, meta['content']['best']['value']['text']),
                })
        except (TypeError,KeyError):
            logging.debug(f'no lang/title/content for url={url}')

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

if __name__ == '__main__':
    # process command line args
    import argparse
    parser = argparse.ArgumentParser(description='''
    Insert the warc file into the database.
    ''')
    parser.add_argument('--warc', help='path to warc file(s) to insert into db', nargs='+', required=True)
    args = parser.parse_args()

    # configure logging
    logging.getLogger().setLevel(os.environ.get('LOGLEVEL','INFO'))

    # create database connection
    dburl = f'postgresql://{os.environ["POSTGRES_USER"]}:{os.environ["POSTGRES_PASSWORD"]}@db:5432/{os.environ["POSTGRES_NAME"]}'
    engine = sqlalchemy.create_engine(dburl, connect_args={
        'application_name': 'metahtml',
        'connect_timeout': 60*60
        })  
    connection = engine.connect()

    # process all warc files
    for warc in args.warc:
        insert_warc(connection, warc)
