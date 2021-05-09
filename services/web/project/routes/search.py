from project import app

# imports
import copy
import datetime
import time
import pspacy
import re
from sqlalchemy.sql import text
from flask import request, g, render_template


def parse_query(query):
    '''
    '''
    regex = r'site: *([^ ]+)'
    host = None
    for host in re.findall(regex, query):
        pass

    query = re.sub(regex,'',query)
    ts_query = pspacy.lemmatize_query('en', query)

    return {'host':host,'ts_query':ts_query}


@app.route('/search')
def search():

    query = request.args.get('query')
    if query is None:
        return index()

    time_lo_def = '1980-01-01'
    time_hi_def = '2022-01-01'
    time_lo = request.args.get('time_lo', time_lo_def)
    time_hi = request.args.get('time_hi', time_hi_def)

    #ts_query = pspacy.lemmatize_query('en', query)
    parsed_query = parse_query(query)
    ts_query = parsed_query['ts_query']

    terms = [ term for term in ts_query.split() if term != '&' ]

    if len(terms)<1:
        # FIXME:
        # if there are no query terms in the query parameter after filtering, we should probably display an error message
        return render_template(
            'search.html',
            )

    sql=text(f'''
    select  
        extract(epoch from x.time ) as x,
        '''+
        ''',
        '''.join([f'''
        coalesce(y{i} /* /total.total */,0) as y{i}
        ''' for i,term in enumerate(terms) ])
        +'''
    from (
        select generate_series(:time_lo, :time_hi, '1 month'::interval) as time
    ) as x
    left outer join (
        select
            hostpath_surt as total,
            timestamp_published_month as time
        from metahtml_rollup_langmonth
        where 
                "metahtml_view.language" = 'en'
            and timestamp_published_month >= :time_lo
            and timestamp_published_month <= :time_hi
    ) total on total.time=x.time
    '''
    +'''
    '''.join([f'''
    left outer join (
        select
            hostpath_surt as y{i},
            timestamp_published_month as time
        from metahtml_rollup_content_textlangmonth
        where 
            alltext = :term{i}
            and "metahtml_view.language" = 'en'
            and timestamp_published_month >= :time_lo
            and timestamp_published_month <= :time_hi
    ) y{i} on x.time=y{i}.time
    ''' for i,term in enumerate(terms) ])
    +
    '''
    order by x asc;
    ''')
    bind_params = {
        f'term{i}':term
        for i,term in enumerate(terms)
        }
    bind_params['time_lo'] = time_lo_def
    bind_params['time_hi'] = time_hi_def
    res = list(g.connection.execute(sql,bind_params))
    x = [ row.x for row in res ]
    ys = [ [ row[i+1] for row in res ] for i,term in enumerate(terms) ] 
    colors = ['red','green','blue','black','purple','orange','pink','aqua']

    sql_search = text(f'''
    SELECT 
        id,
        host_unsurt(hostpath_surt) AS url,
        host_unsurt(url_host_surt(hostpath_surt)) AS host,
        title,
        description,
        language,
        date(timestamp_published) AS date_published
    FROM metahtml_view
    WHERE to_tsquery('simple', :ts_query) @@ tsv_content 
      '''
      +
      ('AND url_host_surt(url) like url_host_surt(:host)' if parsed_query['host'] is not None else '')
      +
      '''
      --AND jsonb->'type'->'best'->>'value' = 'article' 
      --AND language_iso639(jsonb->'language'->'best'->>'value') = 'en'
      --AND jsonb->'timestamp.published' is not null
      AND timestamp_published <= :time_hi
      AND timestamp_published >= :time_lo
    ORDER BY timestamp_published <=> '2040-01-01'
    OFFSET 0
    LIMIT 10;
    ''')
    bind_params = copy.copy(parsed_query)
    bind_params['time_lo'] = time_lo
    bind_params['time_hi'] = time_hi
    res = g.connection.execute(sql_search, bind_params)

    return render_template(
        'search.html',
        query = query,
        results = res,
        x = x,
        ys = ys,
        terms = zip(terms,colors),
        comments = str(sql_search),
        time_lo_def = time_lo_def,
        time_hi_def = time_hi_def,
        )


