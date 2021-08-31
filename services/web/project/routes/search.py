from project import app
from project.utils import do_query, render_template, debug_timer
import chajda
import chajda.tsquery
from flask import request
import itertools
import logging
import datetime

from project.routes.json.count import get_count_data
from project.routes.json.projection import get_projection


@app.route('/search')
def search():

    with debug_timer('parse query'):
        # extract mentions axis
        mentions_axis = request.args.get('mentions_axis', 'hostpath')
        if mentions_axis not in ['host','hostpath']:
            mentions_axis = 'hostpath'

        # extract time filters from query
        time_lo_def = '1960-01-01'
        time_hi_def = str(datetime.datetime.now()).split()[0][:7]+'-01'
        time_lo_args = request.args.get('time_lo')
        time_hi_args = request.args.get('time_hi')
        time_lo = time_lo_args if time_lo_args else time_lo_def
        time_hi = time_hi_args if time_hi_args else time_hi_def

        # adjust times based on granularity
        granularity = request.args.get('granularity','month')
        if granularity not in ['year','month','day']:
            granularity = 'month'
        if granularity == 'day':
            time_lo = time_lo[:10]
            time_hi = time_hi[:10]
        if granularity == 'month':
            time_lo = time_lo[:7]+'-01'
            time_hi = time_hi[:7]+'-01'
        if granularity == 'year':
            time_lo = time_lo[:4]+'-01-01'
            time_hi = time_hi[:4]+'-01-01'

        # extract other query params
        orderby = request.args.get('orderby','rank')
        normalize = request.args.get('normalize','none')
        lang = request.args.get('lang','en')

        # FIXME:
        # normalization is currently hard-coded to mentions_axis='hostpath'
        if mentions_axis=='host':
            normalize = 'none'

    # extract the key information from the query
    with debug_timer('parse query string'):
        query = request.args.get('query', '')
        parse = chajda.tsquery.parse(lang, query)
        tsquery = parse['tsquery']
        try:
            filters = list(parse['filtertree'].find_data('filter'))
            filter_hosts = [ t.children[1] for t in filters if t.children[0] in ['site','host'] ]
        except:
            filter_hosts = []
        terms = parse['terms']

    # get normalization terms
    with debug_timer('normalization terms'):
        terms_combinations = []
        for i in range(1,len(terms)):
            terms_combinations.extend(itertools.combinations(terms, i))
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
            parse = chajda.tsquery.parse(lang, normalize_terms_raw)
            terms_normalize = parse['terms']
        else:
            terms_normalize = None

        # get projection words
        pos_words = request.args.get('pos_words','sad misery dissatisfaction bore').split()
        neg_words = request.args.get('neg_words','happy joy pleasure delight').split()

    # access the database
    with debug_timer('db: projection'):
        projection_data = get_projection(time_lo, time_hi, terms, lang, filter_hosts, pos_words, neg_words, granularity)
    with debug_timer('db: timeplot'):
        count_data = get_count_data(time_lo, time_hi, terms, lang, filter_hosts, normalize, terms_normalize, granularity, mentions_axis)
    with debug_timer('db: search'):
        if tsquery != '':
            search_results = get_search_results(tsquery, lang, filter_hosts, time_lo, time_hi, orderby, granularity)
        else:
            search_results = None

    # return the generated HTML
    return render_template(
        'search.html',
        query = query,
        time_lo_args = time_lo_args,
        time_hi_args = time_hi_args,
        pos_words = ' '.join(pos_words),
        neg_words = ' '.join(neg_words),
        lang = lang,
        orderby = orderby,
        normalize = normalize,
        granularity = granularity,
        mentions_axis = mentions_axis,
        search_results = search_results,
        count_data = count_data,
        projection_data = projection_data,
        terms_combinations_pretty = terms_combinations_pretty,
        )
    

################################################################################

    
def get_search_results(tsquery, lang, filter_hosts, time_lo, time_hi, orderby, granularity, limit=20):
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
