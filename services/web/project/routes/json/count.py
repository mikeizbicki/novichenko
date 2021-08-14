from project import app
from project.utils import do_query, render_template, debug_timer
import chajda
import chajda.tsquery
from flask import request
import itertools
import logging
import datetime


def get_term_counts(time_lo_def, time_hi_def, terms, lang, filter_hosts, granularity, mentions_axis):
    if len(filter_hosts) == 0:
        host = ''
        clause_host = ''
    else:
        host = '_host'
        #clause_host = 'and unsurt("metahtml_view.host_surt") = ANY(:filter_hosts)' 
        clause_host = 'and "metahtml_view.host_surt" = host_surt(:filter_host1)' 

    sql_term_counts = (f'''
    select
        x,
        total,
        term_counts[1] as term_counts,
        term_counts[2] as term_counts_lo,
        term_counts[3] as term_counts_hi
    from (
        select
            x,
            coalesce(total, 0) as total,
            coalesce(term_counts, ARRAY[0,0,0]) as term_counts
        from (
            select generate_series(:time_lo, :time_hi, '1 {granularity}'::interval) as x
        ) as x
        left outer join (
            select
                sum({mentions_axis}_surt) as total,
                timestamp_published_{granularity} as x 
            from metahtml_rollup_lang{granularity}{host}_theta
            where "metahtml_view.language" = :lang
              and timestamp_published_{granularity} >= :time_lo
              and timestamp_published_{granularity} <  :time_hi
              {clause_host}
            group by x
        ) as t_total using (x)
        '''
        +
        (f'''
        left outer join (
            select
                x,
                theta_sketch_get_estimate_and_bounds(theta_sketch_intersection(coalesce("theta_sketch", theta_sketch_empty())), 3) as term_counts
            from (
        '''
        +
        '''
        union all
        '''.join([f'''
                select
                    theta_sketch_union("theta_sketch_distinct(metahtml_view.{mentions_axis}_surt)") as theta_sketch,
                    timestamp_published_{granularity} as x
                from metahtml_rollup_textlang{granularity}{host}_theta_raw
                where "metahtml_view.language" = :lang
                  and timestamp_published_{granularity} >= :time_lo
                  and timestamp_published_{granularity} <  :time_hi
                  and alltext = :term{i}
                  {clause_host}
                group by x
        '''
        for i,term in enumerate(terms) ])
        +'''
            ) t_inner
            group by x
        ) t using(x)'''
        if len(terms)>0 else '')+
        '''
        order by x asc
    ) t;
    ''')
    sql_term_counts = (f'''
    with results as (
        select
            x,
            total,
            term_counts[1] as term_counts,
            term_counts[2] as term_counts_lo,
            term_counts[3] as term_counts_hi
        from (
            select
                x.time as x,
                coalesce(total, 0) as total,
                coalesce(term_counts, ARRAY[0,0,0]) as term_counts
            from (
                select generate_series(:time_lo, :time_hi, '1 {granularity}'::interval) as time
            ) as x
            left outer join (
                select
                    sum({mentions_axis}_surt) as total,
                    timestamp_published_{granularity} as time
                from metahtml_rollup_lang{granularity}{host}_theta
                where "metahtml_view.language" = :lang
                  and timestamp_published_{granularity} >= :time_lo
                  and timestamp_published_{granularity} <  :time_hi
                  {clause_host}
                group by time
            ) total using(time)'''
            +
            (f'''
            left outer join (
                select
                    time,
                    theta_sketch_get_estimate_and_bounds(theta_sketch_intersection(coalesce("theta_sketch", theta_sketch_empty())), 3) as term_counts
                from (
            '''
            +
            '''
            union all
            '''.join([f'''
                select *
                from (
                    select generate_series(:time_lo, :time_hi, '1 {granularity}'::interval) as time
                ) as x
                full outer join (
                    select
                        theta_sketch_union("theta_sketch_distinct(metahtml_view.{mentions_axis}_surt)") as theta_sketch,
                        timestamp_published_{granularity} as time
                    from metahtml_rollup_textlang{granularity}{host}_theta_raw
                    where "metahtml_view.language" = :lang
                      and timestamp_published_{granularity} >= :time_lo
                      and timestamp_published_{granularity} <  :time_hi
                      and alltext = :term{i}
                      {clause_host}
                    group by time
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
        ) t
    )
    select *
    from results
    where x >= (select min(x) from results where term_counts>0)
    ''')

    bind_params = {
        f'term{i}':term
        for i,term in enumerate(terms)
        }
    bind_params['time_lo'] = time_lo_def
    bind_params['time_hi'] = time_hi_def
    bind_params['filter_hosts'] = filter_hosts
    bind_params['filter_host1'] = filter_hosts[0] if len(filter_hosts)>0 else None
    bind_params['lang'] = lang
    return do_query('count_term_counts', sql_term_counts, bind_params)


def get_count_data(time_lo_def, time_hi_def, terms, lang, filter_hosts, normalize, terms_normalize, granularity, mentions_axis):
    '''
    FIXME:
    This should obey the property that the term_counts with multiple terms is always <= term_counts with the individual terms,
    but this is a bit difficult to write a doctest for.

    FIXME:
    does not support OR or NOT clauses
    '''
    if len(filter_hosts) == 0:
        host = ''
        clause_host = ''
    else:
        host = '_host'
        #clause_host = 'and unsurt("metahtml_view.host_surt") = ANY(:filter_hosts)' 
        clause_host = 'and "metahtml_view.host_surt" = host_surt(:filter_host1)' 


    bind_params = {}
    bind_params['time_lo'] = time_lo_def
    bind_params['time_hi'] = time_hi_def
    bind_params['filter_hosts'] = filter_hosts
    bind_params['filter_host1'] = filter_hosts[0] if len(filter_hosts)>0 else None
    bind_params['lang'] = lang

    # get default data when there's no terms present
    if len(terms) == 0:
        sql_total = (f'''
        select
            {mentions_axis}_surt as total,
            timestamp_published_{granularity} as x
        from metahtml_rollup_lang{granularity}{host}_theta
        where "metahtml_view.language" = :lang
          and timestamp_published_{granularity} >= :time_lo
          and timestamp_published_{granularity} <  :time_hi
          {clause_host}
        order by x asc
        ''')
        sql_total = (f'''
        with results as (
            select
                x,
                total
            from (
                select
                    x.time as x,
                    coalesce(total, 0) as total
                from (
                    select generate_series(:time_lo, :time_hi, '1 {granularity}'::interval) as time
                ) as x
                left outer join (
                    select
                        sum({mentions_axis}_surt) as total,
                        timestamp_published_{granularity} as time
                    from metahtml_rollup_lang{granularity}{host}_theta
                    where "metahtml_view.language" = :lang
                      and timestamp_published_{granularity} >= :time_lo
                      and timestamp_published_{granularity} <  :time_hi
                      {clause_host}
                    group by timestamp_published_{granularity}
                ) total using(time)
                order by x asc
            ) t
        )
        select *
        from results
        where
            x >= (select min(x) from results where total > 0)
        ''')
        sql_total = (f'''
        with results as (
            select
                sum({mentions_axis}_surt) as total,
                timestamp_published_{granularity} as x 
            from metahtml_rollup_lang{granularity}{host}_theta
            where "metahtml_view.language" = :lang
              and timestamp_published_{granularity} >= :time_lo
              and timestamp_published_{granularity} <  :time_hi
              {clause_host}
            group by timestamp_published_{granularity}
            order by x asc
        )
        select
            coalesce(total,0) as total,
            x
        from results
        right outer join ( 
            select generate_series((select min(x) from results), (select max(x) from results), '1 {granularity}'::interval) as x
        ) as xs using (x)
        ''')
        res = do_query('count_total', sql_total, bind_params)
        count_data = {
            'xs': [ row.x for row in res ],
            'totals': [ row.total for row in res ],
            'term_counts': [ row.total for row in res ],
            'term_counts_lo': [ row.total for row in res ],
            'term_counts_hi': [ row.total for row in res ],
            }

    # get results for a query with terms
    else:
        res = get_term_counts(time_lo_def, time_hi_def, terms, lang, filter_hosts, granularity, mentions_axis)
        count_data = {
            'xs': [ row.x for row in res ],
            'totals': [ row.total for row in res ],
            'term_counts': [ row.term_counts for row in res ],
            'term_counts_lo': [ row.term_counts_lo for row in res ],
            'term_counts_hi': [ row.term_counts_hi for row in res ],
            }

    normalize_values = count_data['totals'] 
    normalize_values_lo = count_data['totals'] 
    normalize_values_hi = count_data['totals'] 
    if 'query' in normalize:
        res = get_term_counts(time_lo_def, time_hi_def, terms_normalize, lang, filter_hosts, granularity, mentions_axis)
        normalize_values = [ row.term_counts for row in res ]
        normalize_values_lo = [ row.term_counts_lo for row in res ]
        normalize_values_hi = [ row.term_counts_hi for row in res ]

    if normalize == 'total' or 'query' in normalize:
        count_data['term_counts']    = [ a/(b+1e-10) for a,b in zip(count_data['term_counts']   , normalize_values) ]
        count_data['term_counts_lo'] = [ a/(b+1e-10) for a,b in zip(count_data['term_counts_lo'], normalize_values_lo) ]
        count_data['term_counts_hi'] = [ a/(b+1e-10) for a,b in zip(count_data['term_counts_hi'], normalize_values_hi) ]

    return count_data


