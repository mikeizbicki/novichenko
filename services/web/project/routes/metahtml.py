from project import app
from project.utils import do_query, render_template
from flask import request, abort
import logging


@app.route('/metahtml')
def metahtml():
    url = request.args.get('url')
    id = request.args.get('id')

    sql = (f'''
    SELECT 
        accessed_at,
        inserted_at,
        url,
        jsonb
    FROM metahtml
    WHERE ''' + ( 'url_hostpath_surt(url)=url_hostpath_surt(:url)' if url else 'id=:id' ) + '''
    ORDER BY accessed_at DESC
    LIMIT 1
    ''')
    res = do_query('metahtml', sql, {'url':url, 'id':id})

    try:
        row = res[0]
    except IndexError:
        abort(404)

    jsonb = {}
    for key in ['author','timestamp.published','timestamp.modified','url.canonical','language','version']:
        try:
            value = row['jsonb'][key]['best']['value']
        except (TypeError,KeyError):
            value = ''
        jsonb[key] = value
    jsonb_html = dict2html(jsonb)
    try:
        title = row['jsonb']['title']['best']['value']
        content = row['jsonb']['content']['best']['value']['html']
    except KeyError:
        title = None
        content = None

    meta = simplify_meta(row['jsonb'])
    logging.info('meta='+str(meta))

    return render_template(
        'metahtml.html',
        row = row,
        title = title,
        content = content,
        jsonb_html = jsonb_html
        )


def dict2html(d):
    html='<table>'
    for k in d:
        html+=f'<tr><td>{k}</td><td>{d[k]}</td></tr>'
    html+='</table>'
    return html


def simplify_meta(meta):
    '''
    removes the verbose/debug information from the meta dictionary

    FIXME: this is copy/pasted from the metahtml library
    '''
    import json
    ret = {}
    for k in meta:
        try:
            if meta[k]['best'] is not None:
                ret[k] = meta[k]['best']['value']
        except (TypeError,KeyError) as e:
            ret[k] = meta[k]

    del ret['version']
    del ret['process_time']

    # convert datetime objects into strings
    ret = json.loads(json.dumps(dict(ret), default=str))

    return ret
