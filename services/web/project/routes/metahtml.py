from project import app

from sqlalchemy.sql import text
from flask import request, g, render_template


@app.route('/metahtml')
def metahtml():
    id = request.args.get('id')
    if id is None:
        return render_template(
            'metahtml',
            )
    else:
        sql=text(f'''
        SELECT 
            accessed_at,
            inserted_at,
            url,
            jsonb
        FROM metahtml
        WHERE id=:id
        ''')
        res = g.connection.execute(sql,{
            'id':id
            }).first()

        jsonb = {}
        for key in ['author','timestamp.published','timestamp.modified','url.canonical','language','version']:
            try:
                value = res['jsonb'][key]['best']['value']
            except (TypeError,KeyError):
            #except KeyError:
                value = ''
            jsonb[key] = value
        jsonb_html = dict2html(jsonb)
        try:
            title = res['jsonb']['title']['best']['value']
            content = res['jsonb']['content']['best']['value']['html']
        except KeyError:
            title = None
            content = None

        return render_template(
            'metahtml.html',
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


