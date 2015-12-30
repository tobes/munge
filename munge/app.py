import re
from numbers import Number
import urllib

from sqlalchemy.engine import reflection

from flask import Flask, render_template, url_for, abort, request, escape, Markup
from flask.ext.sqlalchemy import SQLAlchemy
from werkzeug import url_encode

import sa_common
import config


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = config.CONNECTION_STRING
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


def html_format(html, *args, **kw):
    # escape any values
    args = [escape(arg) for arg in args]
    kw = {k: escape(v) for k, v in kw.items()}
    return Markup(unicode(html).format(*args, **kw))


def add_yes(arg):
    if arg:
        return 'YES'
    return ''


def make_html_span(name, cls):
    return '<span class="{cls}">{{{name}}}</span>'.format(name=name, cls=cls)


def code_desc(arg, code_type):
    codes = codes_data.get(code_type)
    if not codes:
        return arg
    if arg:
        desc = codes.get(arg)
        html = make_html_span('code', 'code') + ' '
        if desc:
            html += make_html_span('desc', 'code_desc')
        else:
            html += make_html_span('desc', 'code_unknown')
            desc = 'UNKNOWN'
        return html_format(html, code=arg, desc=desc)

    html = make_html_span('code', 'code_missing')
    return html_format(html, code='MISSING')


@app.template_global()
def modify_query(**new_values):
    args = request.args.copy()

    for key, value in new_values.items():
        args[key] = value

    return '{}?{}'.format(urllib.quote(request.path), url_encode(args))


@app.template_global()
def cell_function(info, value):
    if len(info) == 1:
        return info[0](value)
    return info[0](value, *info[1:])


@app.template_global()
def make_td_class(arg):
    if isinstance(arg, Number):
        return Markup(' class="numeric"')
    return ''


def run_sql(*args, **kw):
    return sa_common.run_sql(db.engine, *args, **kw)


def get_indexes(*args, **kw):
    return sa_common.get_indexes(db.engine, *args, **kw)


def get_primary_keys(*args, **kw):
    return sa_common.get_primary_keys(db.engine, *args, **kw)


def table_list(*args, **kw):
    return sa_common.table_list(db.engine, *args, **kw)


codes_data = {}
for table in table_list():
    if table.startswith('c_'):
        codes = {}
        sql = 'SELECT code, "desc" FROM "{table}"'.format(table=table)
        result = run_sql(sql)
        for row in result:
            codes[row[0]] = row[1]
        codes_data[table[2:]] = codes


def auto_links(fields):
    links = {}
    for i, field in enumerate(fields):
        if field.get('name') == 'uarn':
            links[i] = ('premises', 'uarn', i)
    return links


def auto_functions(fields):
    functions = {}
    for i, field in enumerate(fields):
        if field.get('name').endswith('_code'):
            functions[i] = (code_desc, field.get('name')[:-5])
    return functions


def show_result(sql, table=None, data=None):
    offset = int(request.args.get('offset', 0))
    # We need to have a result to get the field types
    if data is None:
        data = {}
    result = run_sql(sql + ' LIMIT 1', data)
    fields = sa_common.get_result_fields(db.engine, result, table)
    # Now run query
    result = run_sql(sql + (' LIMIT 1000 OFFSET %s' % offset), data)
    output = {
        'fields': fields,
        'data': result,
        'offset': offset,
    }
    if not 'raw' in request.args:
        output['links'] = auto_links(fields)
        output['functions'] = auto_functions(fields)
    return output


def show_table(table):
    sql = 'SELECT * FROM "%s"' % table
    return show_result(sql, table)


@app.route('/')
def home():
    return render_template('home.html')


@app.route('/table/')
def tables():
    match = '[cls]\_.*'
    out = []
    for table in sorted(table_list()):
        if re.match(match, table):
            out.append(table)
    return render_template('tables.html', data=out)

@app.route('/table/<table>')
def table(table=None):
    match = '[cls]\_.*'
    if not re.match(match, table) or table not in table_list():
        abort(404)
    data = show_table(table)
    data['title'] = table

    return render_template('table_output.html', data=data)


@app.route('/ba/')
def ba_list():
    sql = 'SELECT code, "desc" FROM c_ba ORDER BY "desc"'
    output = show_result(sql)
    output['links'][1] = ('ba_premises_list', 'ba_code', 0)
    return render_template('table_output.html', data=output)


@app.route('/ba/<ba_code>')
def ba_premises_list(ba_code):
    data = {'ba_code': ba_code}
    sql = '''
    SELECT v.uarn, b.uarn, s.code as scat_code
    FROM vao_list v
    LEFT OUTER JOIN v_vao_base b
    ON b.uarn = v.uarn
    LEFT JOIN c_scat s ON s.code = v.scat_code
    WHERE v.ba_code = :ba_code
    ORDER BY s.desc
    '''
    output = show_result(sql, data=data)
    del output['links'][1]
    output['functions'][1] = (add_yes,)
    output['fields'][1]['name'] = 'summary'
    return render_template('table_output.html', data=output)


@app.route('/scat/')
def scat_list():
    sql = 'SELECT code, "desc" FROM c_scat ORDER BY "desc"'
    result = run_sql(sql)
    output = show_result(sql)
    output['links'][1] = ('scat_premises_list', 'scat_code', 0)
    return render_template('table_output.html', data=output)


@app.route('/scat/<scat_code>')
def scat_premises_list(scat_code):
    data = {'scat_code': scat_code}
    sql = '''
    SELECT v.uarn, b.uarn, c.code as ba_code
    FROM vao_list v
    LEFT OUTER JOIN v_vao_base b
    ON b.uarn = v.uarn
    LEFT JOIN c_ba c ON c.code = v.ba_code
    WHERE v.scat_code = :scat_code
    ORDER BY c.desc
    '''
    output = show_result(sql, data=data)
    del output['links'][1]
    output['fields'][1]['name'] = 'summary'
    output['functions'][1] = (add_yes,)
    return render_template('table_output.html', data=output)


@app.route('/ba_areas/')
def ba_areas_list():
    sql = 'SELECT code, "desc" FROM c_ba ORDER BY "desc"'
    result = run_sql(sql)
    output = show_result(sql)
    output['links'][1] = ('ba_areas', 'ba_code', 0)
    return render_template('table_output.html', data=output)


@app.route('/ba_areas/<ba_code>')
def ba_areas(ba_code):
    data = {'ba_code': ba_code}
    sql = '''
    SELECT
    s.code as scat_code,
    count,
    total_m2,
    total_value,
    total_area_price
    FROM s_vao_base_areas t
    LEFT JOIN c_scat s ON s.code = t.scat_code
    WHERE ba_code = :ba_code
    ORDER BY s.desc
    '''
    output = show_result(sql, data=data)
    return render_template('table_output.html', data=output)


@app.route('/scat_areas/')
def scat_areas_list():
    sql = 'SELECT code, "desc" FROM c_scat ORDER BY "desc"'
    output = show_result(sql)
    output['links'][1] = ('scat_areas', 'scat_code', 0)
    return render_template('table_output.html', data=output)

@app.route('/scat_areas/<scat_code>')
def scat_areas(scat_code):
    data = {'scat_code': scat_code}
    sql = '''
    SELECT
    s.desc,
    count,
    total_m2,
    total_value,
    total_area_price
    FROM s_vao_base_areas t
    LEFT JOIN c_ba s ON s.code = t.ba_code
    WHERE scat_code = :scat_code
    ORDER BY s.desc
    '''
    output = show_result(sql, data=data)
    return render_template('table_output.html', data=output)


@app.route('/premises/<uarn>')
def premises(uarn):
    data = {'uarn': uarn}
    output = []

    tables = [
        'vao_list',
        'v_vao_base',
        'vao_line',
        'vao_additions',
        'vao_plant',
        'vao_parking',
        'vao_adj',
        'vao_adj_totals',
    ]

    single_row_tables = [
        'vao_list',
        'v_vao_base',
    ]

    for table in tables:
        sql = 'SELECT * FROM %s WHERE uarn = :uarn' % table
        out = show_result(sql, table, data=data)
        out['offset'] = ''
        out['title'] = table
        output.append((out, table in single_row_tables))

    return render_template('premises.html', output=output)


@app.route('/postcode/<postcode>')
def postcode_premises_list(postcode):
    postcode = postcode.upper().replace(' ', '') + '%'
    print postcode
    data = {'postcode': postcode}
    sql = '''
    SELECT v.uarn uarn, v.pc, v.town, b.uarn summary, v.scat_code
    FROM vao_list v
    LEFT OUTER JOIN v_vao_base b ON b.uarn = v.uarn
    LEFT JOIN c_scat s ON s.code = v.scat_code
    WHERE v.pcc like :postcode
    ORDER BY s.desc, v.pc
    '''
    output = show_result(sql, data=data)
    output['functions'][3] = (add_yes,)
    return render_template('table_output.html', data=output)
