#!/usr/bin/python3

# the sys import is needed so that we can import from the current project
import sys
sys.path.append('.')
import metahtml

# load imports
import cdx_toolkit
import json
import os
import re
import sqlalchemy
import traceback

import pspacy

# initialize logging
import logging
log = logging.getLogger(__name__)

def process_cdx_url(connection, url, batch_size=100, source='cc', **kwargs):
    '''
    FIXME:
    ideally, this function would be wrapped in a transaction;
    but this causes deadlocks when it is run concurrently with other instances of itself
    '''
    cdx = cdx_toolkit.CDXFetcher(source)

    # create a new entry in the source table for this bulk insertion
    name = 'process_cdx_url(url="'+str(url)+'", source="'+str(source)+'", **kwargs='+str(kwargs)+')'
    log.info("name="+str(name.replace('"', r'\"')))
    try:
        sql = sqlalchemy.sql.text('''
        INSERT INTO source (name) VALUES (:name) RETURNING id;
        ''')
        res = connection.execute(sql,{'name':name})
        id_source = res.first()['id']
        log.info('id_source='+str(id_source))

    # if an entry already exists in source,
    # then this bulk insertion has already happened (although may not be complete),
    # and so we skip this insertion
    except sqlalchemy.exc.IntegrityError:
        logging.warning('skipping name='+name)
        return

    # ensure that we search all records, and not just records from the last year
    if 'from_ts' not in kwargs:
        kwargs['from_ts'] = '19000101000000'
    
    # the cc archive supports filtering by status code, but the ia archive does not;
    # since we only care about status=200, add this filter if possible
    if 'filter' not in kwargs and source=='cc':
        kwargs['filter'] = 'status:200'

    # estimate the total number of matching urls
    estimated_urls = cdx.get_size_estimate(url, kwargs)
    log.info("estimated_urls="+str(estimated_urls))

    # loop through each matching url
    batch = []
    for i,result in enumerate(cdx.iter(url,**kwargs)):

        # process only urls with 200 status code (i.e. successful)
        if result['status']=='200':
            log.info('fetching result; progress='+str(i)+'/'+str(estimated_urls)+'={:10.4f}'.format(i/estimated_urls)+' url='+result['url'])
            record = result.fetch_warc_record()

            # extract the information from the warc record
            url = record.rec_headers.get_header('WARC-Target-URI')
            accessed_at = record.rec_headers.get_header('WARC-Date')
            html = record.content_stream().read()
            log.debug("url="+url)

            # extract the meta
            try:
                meta = metahtml.parse(html, url)

                try:
                    pspacy_title = pspacy.lemmatize(meta['language']['best']['value'], meta['title']['best']['value'])
                except TypeError:
                    pspacy_title = None
                try:
                    pspacy_content = pspacy.lemmatize(meta['language']['best']['value'], meta['content']['best']['value']['text'])
                except TypeError:
                    pspacy_content = None

            # if there was an error in metahtml, log it
            except Exception as e:
                log.warning('url='+url+'type='+type(e).__name__+' exception='+str(e))
                meta = { 
                    'exception' : {
                        'str(e)' : str(e),
                        'type' : type(e).__name__,
                        'location' : 'metahtml',
                        'traceback' : traceback.format_exc()
                        }
                    }
                pspacy_title = None
                pspacy_content = None

            # append to the batch
            meta_json = json.dumps(meta, default=str)
            batch.append({
                'accessed_at' : accessed_at,
                'id_source' : id_source,
                'url' : url,
                'jsonb' : meta_json,
                'pspacy_title' : pspacy_title,
                'pspacy_content' : pspacy_content
                })

        if len(batch)>=batch_size:
            bulk_insert(connection,batch)
            batch = []

    # finished loading urls,
    # so insert the last batch and update the source table
    if len(batch)>0:
        bulk_insert(connection,batch)
        batch = []
    sql = sqlalchemy.sql.text('''
    UPDATE source SET finished_at=now() where id=:id;
    ''')
    res = connection.execute(sql,{'id':id_source})


def bulk_insert(connection,batch):
    try:
        logging.info('bulk_insert '+str(len(batch))+' rows')
        keys = ['accessed_at', 'id_source', 'url', 'jsonb']
        sql = sqlalchemy.sql.text(
            'INSERT INTO metahtml ('+','.join(keys)+',title,content) VALUES'+
            ','.join(['(' + ','.join([f':{key}{i}' for key in keys]) + f",:pspacy_title{i},:pspacy_content{i}" + ')' for i in range(len(batch))])
            )
        res = connection.execute(sql,{
            key+str(i) : d[key]
            for key in keys + ['pspacy_title','pspacy_content']
            for i,d in enumerate(batch)
            })
    except Exception as e:
        logging.error('failed to insert:'+str(e))
        pass


if __name__=='__main__':
    # process command line args
    import argparse
    parser = argparse.ArgumentParser(description='''
    Insert the warc file into the database.
    ''')
    parser.add_argument('--url_pattern', default='cnn.com/*')
    parser.add_argument('--db', default='postgresql:///')
    args = parser.parse_args()

    # set logging
    logging.getLogger().setLevel(os.environ.get('LOGLEVEL','INFO'))

    # create database connection
    engine = sqlalchemy.create_engine(args.db, connect_args={
        'application_name': sys.argv[0].split('/')[-1],
        'connect_timeout': 60*60,
        })  
    connection = engine.connect()

    # FIXME: ensure that the url_pattern will not overload the server
    if not re.match(r'.+\..+/', args.url_pattern):
        print('args.url_pattern must contain at least a full hostname in order to not overload the cdx servers')
        sys.exit(1)

    # process the query
    process_cdx_url(connection, args.url_pattern)
