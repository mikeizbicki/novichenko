from project import app
from project.utils import do_query, render_template

from sqlalchemy.sql import text
from flask import request, g

@app.route('/host/<host>')
def host(host):
    if host is None:
        return render_template(
            'index',
            )
    else:

        def table2html(table):
            sql=text(f'''
            SELECT 
                *
            FROM {table} 
            WHERE
                host like :host_pattern
            /*
            ORDER BY
                count desc
                */
            LIMIT 20
            ''')
            res = g.connection.execute(sql,{
                'host_pattern':'%'+host
                })
            return f'<h3>{table}</h3>{res2html(res)}'
        tables = [
            'metahtml_exceptions_host',
            #'metahtml_rollup_host',
            #'metahtml_rollup_hosttype',
            #'metahtml_rollup_hostinsert',
            #'metahtml_rollup_hostpub',
            #'metahtml_rollup_texthostpub'
            ]
        return render_template(
            'host.html',
            host = host,
            #html_tables = { table:table2html(table) for table in tables }
            html_tables = { 't1': (get_related_hosts(host)) }
            )





def res2html(res,col_formatter=None,transpose=False,click_headers=False):
    rows=[list(res.keys())]+list(res)
    if transpose:
        rows=list(map(list, zip(*rows)))
    html='<table>'
    for i,row in enumerate(rows):
        html+='<tr>'
        if i==0 and not transpose:
            td='th'
            html+=f'<{td}></{td}>'
        else:
            td='td'
            html+=f'<td>{i}</td>'
        for j,col in enumerate(row):
            val = None
            try:
                val = col_formatter(res.keys()[j],col,i==0)
            except:
                if i>0 and col_formatter is not None:
                    val = col_formatter(res.keys()[j],col)
            if val is None:
                val = col
            if type(col) == int or type(col) == float:
                td_class='numeric'
            else:
                td_class='text'
            html+=f'<{td} class={td_class}>{val}</td>'
        html+='</tr>'
    html+='</table>'
    return html


def get_related_hosts(host):
    sql=f'''
    select unsurt(dest) as dest,src_hostpath
    from metahtml_linksall_host
    where src=url_host_surt(:host)
    order by src_hostpath desc;
    '''
    sql=f'''
    select unsurt(src) as dest,src_hostpath
    from metahtml_linksall_host
    where dest=url_host_surt(:host)
    order by src_hostpath desc;
    '''
    bind_params = {'host':host}
    return do_query('get_related_hosts:outlinks', sql, bind_params)
