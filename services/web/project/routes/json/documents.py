from project import app
from project.utils import do_query, render_template, debug_timer
import chajda
import chajda.tsquery
from flask import request
import itertools
import logging
import datetime

def get_documents(tsquery, lang, filter_hosts, time_lo, time_hi, orderby, granularity, limit=20):
    #if len(tsquery) == 0:
    #    orderby = None

    sql_search = (f'''
    WITH results AS (
        SELECT 
            id,
            unsurt(hostpath_surt) as url,
            url_host(unsurt(hostpath_surt)) AS host,
            title,
            description,
            language,
            date(timestamp_published) AS date_published,
            -- ts_rank_cd(tsv_content, :tsquery) AS rank
            tsv_content <=> (:tsquery :: tsquery) AS rank
        FROM metahtml_view
        WHERE ( :tsquery = ''OR tsv_content @@ (:tsquery :: tsquery) )
          '''
          +
          (
          '''
          AND (
              '''
          +   ''' OR
              '''.join([
              #f"split_part(hostpath_surt,')',1) || ')' like url_host_surt(:host{i})"
              f"host_surt like url_host_surt(:host{i})"
              for i,host in enumerate(filter_hosts)
              ])
          + ')'
          if filter_hosts else ''
          )
          +
          '''
          AND timestamp_published >= :time_lo
          AND timestamp_published <  :time_hi
          AND language = :lang
          '''
        +
        ( "ORDER BY timestamp_published <=> '1000-01-01' DESC" if orderby=='time_desc' else '')
        +
        ( "ORDER BY timestamp_published <=> '1000-01-01' ASC" if orderby=='time_asc' else '')
        +
        ( "ORDER BY tsv_content <=> (:tsquery :: tsquery)" if orderby=='rank' else '')
        +'''
        OFFSET 0
        LIMIT :limit
    )
    SELECT * FROM (
        SELECT DISTINCT ON (title) * FROM RESULTS ORDER BY title,length(url) ASC
    ) t
        '''
        +
        ( "ORDER BY date_published <=> '1000-01-01' DESC" if orderby=='time_desc' else '')
        +
        ( "ORDER BY date_published <=> '1000-01-01' ASC" if orderby=='time_asc' else '')
        +
        ( "ORDER BY rank" if orderby=='rank' else '')
        +'''
        ;
    ''')
    binds = {
        'tsquery': tsquery,
        'time_lo': time_lo,
        'time_hi': time_hi,
        'lang': lang,
        'limit': limit,
        }
    for i,host in enumerate(filter_hosts):
        binds[f'host{i}'] = host
    return do_query('search', sql_search, binds)

