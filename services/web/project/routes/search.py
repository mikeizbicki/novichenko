from project import app
from project.utils import do_query, render_template
import chajda
import chajda.tsquery
from flask import request
import itertools
import logging


@app.route('/search')
def search():

    # extract time filters from query
    time_lo_def = '1980-01-01'
    time_hi_def = '2022-01-01'
    time_lo = request.args.get('time_lo', time_lo_def)
    time_hi = request.args.get('time_hi', time_hi_def)

    # extract the key information from the query
    query = request.args.get('query')
    parse = chajda.tsquery.parse('en', query)
    tsquery = parse['tsquery']
    try:
        filters = list(parse['filtertree'].find_data('filter'))
        filter_hosts = [ t.children[1] for t in filters if t.children[0] == 'site' ]
    except:
        filter_hosts = []
    terms = parse['terms']

    # extract other query params
    orderby = request.args.get('orderby','rank')
    normalize = request.args.get('normalize','none')

    # get normalization terms
    terms_combinations = []
    for i in range(1,len(terms)):
        terms_combinations.extend(itertools.combinations(terms, i))
    logging.info('terms_combinations='+str(terms_combinations))
    terms_combinations_pretty = []
    for term_combination in terms_combinations:
        term_pretty = []
        for term in term_combination:
            if ' ' in term:
                term = '"'+term+'"'
            term_pretty.append(term)
        term_pretty = ' '.join(term_pretty)
        selected = term_pretty in normalize
        terms_combinations_pretty.append((selected,term_pretty))
    if 'query:' in normalize:
        normalize_terms_raw = normalize.split(':')[1]
        parse = chajda.tsquery.parse('en', normalize_terms_raw)
        terms_normalize = parse['terms']
    else:
        terms_normalize = None

    # access the database
    neg_words = ['sad','misery','dissatisfaction','bore']
    pos_words = ['happy','joy','pleasure','delight']
    sentiment_data = get_sentiment(time_lo_def, time_hi_def, terms, pos_words, neg_words)
    timeplot_data = get_timeplot_data(time_lo_def, time_hi_def, terms, normalize, terms_normalize)
    search_results = get_search_results(tsquery, filter_hosts, time_lo, time_hi, orderby)

    # return the generated HTML
    return render_template(
        'search.html',
        query = query,
        orderby = orderby,
        normalize = normalize,
        search_results = search_results,
        timeplot_data = timeplot_data,
        sentiment_data = sentiment_data,
        terms_combinations_pretty = terms_combinations_pretty,
        )
    

def get_term_counts(time_lo_def, time_hi_def, terms):
    sql_term_counts = (f'''
    select
        x,
        total,
        term_counts[1] as term_counts,
        term_counts[2] as term_counts_lo,
        term_counts[3] as term_counts_hi
    from (
        select
            extract(epoch from x.time ) as x,
            coalesce(total, 0) as total,
            coalesce(term_counts, ARRAY[0,0,0]) as term_counts
        from (
            select generate_series(:time_lo, :time_hi, '1 month'::interval) as time
        ) as x
        left outer join (
            select
                hostpath_surt as total,
                timestamp_published_month as time
            from metahtml_rollup_langmonth
            where "metahtml_view.language" = 'en'
              and timestamp_published_month >= :time_lo
              and timestamp_published_month <  :time_hi
        ) total using(time)'''
        +
        ('''
        left outer join (
            select
                time,
                theta_sketch_get_estimate_and_bounds(theta_sketch_intersection(coalesce("theta_sketch_distinct(metahtml_view.hostpath_surt)", theta_sketch_empty())), 3) as term_counts
            from (
        '''
        +
        '''
        union all
        '''.join([f'''
            select *
            from (
                select generate_series(:time_lo, :time_hi, '1 month'::interval) as time
            ) as x
            full outer join (
                select
                    "theta_sketch_distinct(metahtml_view.hostpath_surt)",
                    timestamp_published_month as time
                from metahtml_rollup_textlangmonth_theta_raw
                where "metahtml_view.language" = 'en'
                  and timestamp_published_month >= :time_lo
                  and timestamp_published_month <  :time_hi
                  and alltext = :term{i}
              ) as y using (time)
        '''
        for i,term in enumerate(terms) ])
        +'''
            ) t_inner
            group by time
        ) t using(time)'''
        if len(terms)>0 else '')+
        '''
        order by x asc
    ) t;
    ''')

    bind_params = {
        f'term{i}':term
        for i,term in enumerate(terms)
        }
    bind_params['time_lo'] = time_lo_def
    bind_params['time_hi'] = time_hi_def
    return do_query('timeplot_term_counts', sql_term_counts, bind_params)


def get_timeplot_data(time_lo_def, time_hi_def, terms, normalize, terms_normalize):
    '''
    FIXME:
    This should obey the property that the term_counts with multiple terms is always <= term_counts with the individual terms,
    but this is a bit difficult to write a doctest for.

    FIXME:
    does not support OR or NOT clauses
    '''
    sql_total = (f'''
    select
        x,
        total
    from (
        select
            extract(epoch from x.time ) as x,
            coalesce(total, 0) as total
        from (
            select generate_series(:time_lo, :time_hi, '1 month'::interval) as time
        ) as x
        left outer join (
            select
                hostpath_surt as total,
                timestamp_published_month as time
            from metahtml_rollup_langmonth
            where "metahtml_view.language" = 'en'
              and timestamp_published_month >= :time_lo
              and timestamp_published_month <  :time_hi
        ) total using(time)
        order by x asc
    ) t;
    ''')

    bind_params = {}
    bind_params['time_lo'] = time_lo_def
    bind_params['time_hi'] = time_hi_def
    res = do_query('timeplot_total', sql_total, bind_params)
    timeplot_data = {
        'xs': [ row.x for row in res ],
        'totals': [ row.total for row in res ],
        'term_counts': [ row.total for row in res ],
        'term_counts_lo': [ row.total for row in res ],
        'term_counts_hi': [ row.total for row in res ],
        }

    if len(terms) > 0:
        res = get_term_counts(time_lo_def, time_hi_def, terms)
        timeplot_data = {
            'xs': [ row.x for row in res ],
            'totals': [ row.total for row in res ],
            'term_counts': [ row.term_counts for row in res ],
            'term_counts_lo': [ row.term_counts_lo for row in res ],
            'term_counts_hi': [ row.term_counts_hi for row in res ],
            }

    normalize_values = timeplot_data['totals'] 
    normalize_values_lo = timeplot_data['totals'] 
    normalize_values_hi = timeplot_data['totals'] 
    if 'query' in normalize:
        res = get_term_counts(time_lo_def, time_hi_def, terms_normalize)
        normalize_values = [ row.term_counts for row in res ]
        normalize_values_lo = [ row.term_counts_lo for row in res ]
        normalize_values_hi = [ row.term_counts_hi for row in res ]

    if normalize == 'total' or 'query' in normalize:
        timeplot_data['term_counts']    = [ a/(b+1e-10) for a,b in zip(timeplot_data['term_counts']   , normalize_values) ]
        timeplot_data['term_counts_lo'] = [ a/(b+1e-10) for a,b in zip(timeplot_data['term_counts_lo'], normalize_values_lo) ]
        timeplot_data['term_counts_hi'] = [ a/(b+1e-10) for a,b in zip(timeplot_data['term_counts_hi'], normalize_values_hi) ]

    return timeplot_data

    
def get_search_results(tsquery, filter_hosts, time_lo, time_hi, orderby):
    sql_search = (f'''
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
      --AND host_surt = 'com,nytimes)' '''
      +
      (
      '''
      AND (
          '''
      +   ''' OR
          '''.join([
          f"split_part(hostpath_surt,')',1) || ')' like url_host_surt(:host{i})"
          for i,host in enumerate(filter_hosts)
          ])
      + ')'
      if filter_hosts else ''
      )
      +
      '''
      AND timestamp_published >= :time_lo
      AND timestamp_published <  :time_hi
      '''
    +
    ( "ORDER BY timestamp_published <=> '1000-01-01' DESC" if orderby=='time_desc' else '')
    +
    ( "ORDER BY timestamp_published <=> '1000-01-01' ASC" if orderby=='time_asc' else '')
    +
    ( "ORDER BY tsv_content <=> (:tsquery :: tsquery)" if orderby=='rank' else '')
    +'''
    OFFSET 0
    LIMIT 10;
    ''')
    binds = {
        'tsquery': tsquery,
        'time_lo': time_lo,
        'time_hi': time_hi,
        }
    for i,host in enumerate(filter_hosts):
        binds[f'host{i}'] = host
    return do_query('search', sql_search, binds)


def get_sentiment(time_lo_def, time_hi_def, terms, pos_words, neg_words):
    '''
    FIXME:
    does not support OR or NOT clause
    '''
    projectionvector = (
        [ 0.02957594,  0.50386024, -2.6844428 ,  0.6105701 ,  2.1159103 ,
         -1.84395   , -2.579988  , -0.03845882,  0.581807  ,  2.0521002 ,
          0.57888997,  2.045848  ,  1.2586529 ,  0.05062222,  0.77035975,
         -0.07423502,  2.436877  ,  2.097585  , -2.9644618 , -3.0558    ,
          2.4757    ,  2.79049   , -0.35434967,  1.3205721 ,  1.9617298 ,
         -0.07862997,  0.5482297 , -1.35866   , -0.1098249 , -2.9464078 ,
          0.35745955,  2.06783   , -2.087224  ,  1.266543  ,  0.19132555,
         -1.54833   ,  0.66981   ,  2.02947   , -0.32838506, -0.588573  ,
          2.154276  , -0.06101902, -1.401586  ,  1.176679  ,  0.7171189 ,
         -0.91807485, -0.21014398, -2.991881  , -0.5503142 ,  2.348786  ])
    #projectionvector = chajda.tsvector.make_projectionvector(pos_words, neg_words)
    logging.info('projectionvector='+str(projectionvector))

    sql = (f'''
    select
        extract(epoch from x.time ) as x,
        sentiment
    from (
        select generate_series(:time_lo, :time_hi, '1 month'::interval) as time
    ) as x
    left outer join (
        select
            timestamp_published_month as time,
            "avg(context)" <#> :projectionvector :: vector as sentiment
        from contextvector_month
        where timestamp_published_month >= :time_lo
          and timestamp_published_month <  :time_hi
          and "contextvector.focus" = :focus
    ) sentiment using(time)
    order by x asc
    ''')

    bind_params = {}
    bind_params['time_lo'] = time_lo_def
    bind_params['time_hi'] = time_hi_def
    bind_params['projectionvector'] = list(projectionvector)
    bind_params['focus'] = terms[0]
    res = do_query('sentiments', sql, bind_params)
    data = {
        'xs': [ row.x for row in res ],
        'sentiments': [ row.sentiment for row in res ],
        }
    logging.info('data='+str(data))

    return data


