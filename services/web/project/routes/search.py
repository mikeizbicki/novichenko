from project import app
from project.utils import do_query, render_template
import chajda
import chajda.tsquery
from flask import request


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

    # generate the timeplot data
    sql_timeplot = (f'''
    select
        extract(epoch from x.time ) as x,
        coalesce(total, 0) as total
        '''+
        (
        ''.join([f''',
        coalesce(y{i}, 0) as y{i}''' for i,term in enumerate(terms) ])
        )
        +'''
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
          and timestamp_published_month <= :time_hi
    ) total on total.time=x.time'''
    +'''
    '''.join([f'''
    left outer join (
        select
            hostpath_surt as y{i},
            timestamp_published_month as time
        from metahtml_rollup_textlangmonth
        where "metahtml_view.language" = 'en'
          and timestamp_published_month >= :time_lo
          and timestamp_published_month <= :time_hi
          and alltext = :term{i}
    ) y{i} on x.time=y{i}.time'''
    for i,term in enumerate(terms) ])
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
    res = do_query('timeplot', sql_timeplot, bind_params)
    timeplot_data = {
        'xs': [ row.x for row in res ],
        'totals': [ row.total for row in res ],
        'yss': [ [ row[i+2] for row in res ] for i,term in enumerate(terms) ],
        'terms': list(zip(terms, ['red','green','blue','black','purple','orange','pink','aqua'])),
        }

    # get the search results
    sql_search = (f'''
    SELECT 
        id,
        unsurt(hostpath_surt) as url,
        -- url_host(unsurt(hostpath_surt)) AS host,
        -- split_part(hostpath_surt,')',1) AS host,
        url_host(unsurt(hostpath_surt)) AS host,
        title,
        description,
        language,
        date(timestamp_published) AS date_published,
        -- ts_rank_cd(tsv_content, :tsquery) AS rank
        tsv_content <=> (:tsquery :: tsquery) AS rank
    FROM metahtml_view
    WHERE ( :tsquery = ''OR tsv_content @@ (:tsquery :: tsquery) )'''
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
      AND timestamp_published <= :time_hi
      AND timestamp_published >= :time_lo
    ORDER BY timestamp_published <=> '2040-01-01'
    -- ORDER BY tsv_content <=> (:tsquery :: tsquery)
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
    search_results = do_query('search', sql_search, binds)

    # return the generated HTML
    return render_template(
        'search.html',
        query = query,
        search_results = search_results,
        timeplot_data = timeplot_data,
        )


