from project import app

# imports
import pspacy
from sqlalchemy.sql import text
from flask import request, g, render_template


@app.route('/ngrams')
def ngrams():

    query = request.args.get('query')
    if query is None:
        return index()

    ts_query = pspacy.lemmatize_query('en', query)

    terms = [ term for term in ts_query.split() if term != '&' ]

    if len(terms)<1:
        # FIXME:
        # if there are no query terms in the query parameter after filtering, we should probably display an error message
        return render_template(
            'ngram.html',
            )

    sql=text(f'''
    select  
        extract(epoch from x.time ) as x,
        '''+
        ''',
        '''.join([f'''
        coalesce(y{i}/total.total,0) as y{i}
        ''' for i,term in enumerate(terms) ])
        +'''
    from (
        select generate_series('2000-01-01', '2020-12-31', '1 month'::interval) as time
    ) as x
    left outer join (
        select
            hostpath as total,
            timestamp_published as time
        from metahtml_rollup_langmonth
        where 
                language = 'en'
            and timestamp_published >= '2000-01-01 00:00:00' 
            and timestamp_published <= '2020-12-31 23:59:59'
    ) total on total.time=x.time
    '''
    +'''
    '''.join([f'''
    left outer join (
        select
            hostpath as y{i},
            timestamp_published as time
        from metahtml_rollup_textlangmonth
        where 
            alltext = :term{i}
            and language = 'en'
            and timestamp_published >= '2000-01-01 00:00:00' 
            and timestamp_published <= '2020-12-31 23:59:59'
    ) y{i} on x.time=y{i}.time
    ''' for i,term in enumerate(terms) ])
    +
    '''
    order by x asc;
    ''')
    res = list(g.connection.execute(sql,{
        f'term{i}':term
        for i,term in enumerate(terms)
        }))
    x = [ row.x for row in res ]
    ys = [ [ row[i+1] for row in res ] for i,term in enumerate(terms) ] 
    colors = ['red','green','blue','black','purple','orange','pink','aqua']


    sql=text(f'''
    SELECT 
        id,
        jsonb->'title'->'best'->>'value' AS title,
        jsonb->'description'->'best'->>'value' AS description
    FROM metahtml
    WHERE
        to_tsquery('simple', :ts_query) @@ content AND
        jsonb->'type'->'best'->>'value' = 'article'
    OFFSET 0
    LIMIT 10
    ''')
    res=g.connection.execute(sql,{
        'ts_query':ts_query
        })
    return render_template(
        'ngrams.html',
        query=query,
        results=res,
        x = x,
        ys = ys,
        terms = zip(terms,colors) ,
        )

