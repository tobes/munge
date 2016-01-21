import re
import urllib

from flask import (
    Flask, render_template, abort, request,
    escape, Markup, redirect, url_for
)
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
    if arg is not None:
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
def make_td_class(arg, title):
    if (arg in sa_common.NUMERIC_TYPES and
            not (title.endswith('_code'))):
        return Markup(' class="numeric"')
    return ''


@app.template_global()
def format_table_value(arg, value):
    if value is None:
        return Markup('<span class="null">&lt;Null&gt;</span>')
    if arg in sa_common.FLOAT_TYPES:
        return format_number(value)
    return value


@app.template_global()
def format_number(value):
    formatted = '{:20,.2f}'.format(value)
    if value < 0:
        html = make_html_span('value', 'negative')
        return html_format(html, value=formatted)
    return formatted


def run_sql(*args, **kw):
    return sa_common.run_sql(db.engine, *args, **kw)


def get_indexes(*args, **kw):
    return sa_common.get_indexes(db.engine, *args, **kw)


def get_primary_keys(*args, **kw):
    return sa_common.get_primary_keys(db.engine, *args, **kw)


def table_list():
    return sa_common.table_list(db.engine)


def table_view_list():
    return sa_common.table_view_list(db.engine)


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
    if 'raw' not in request.args:
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
    out = []
    for table in sorted(table_view_list()):
        out.append(table)
    return render_template('tables.html', data=out)


@app.route('/table/<table>')
def table(table=None):
    if table not in table_view_list():
        abort(404)
    data = show_table(table)
    data['title'] = table

    return render_template('table_output.html', data=data)


@app.route('/la/')
def la_list():
    sql = '''
    SELECT code, "desc" FROM c_la
    WHERE LEFT(code, 1) IN ('E', 'w')
    ORDER BY "desc"
    '''
    output = show_result(sql)
    output['links'][1] = ('la_premises_list', 'la_code', 0)
    return render_template('table_output.html', data=output)


@app.route('/la/<la_code>')
def la_premises_list(la_code):
    data = {'la_code': la_code}
    sql = '''
    SELECT v.uarn, b.uarn, s.code as scat_code
    FROM vao_list v
    LEFT OUTER JOIN vao_base b
    ON b.uarn = v.uarn
    LEFT JOIN c_scat s ON s.code = v.scat_code
    WHERE v.la_code = :la_code
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
    output = show_result(sql)
    output['links'][1] = ('scat_premises_list', 'scat_code', 0)
    return render_template('table_output.html', data=output)


@app.route('/scat/<scat_code>')
def scat_premises_list(scat_code):
    data = {'scat_code': scat_code}
    sql = '''
    SELECT v.uarn, b.uarn, c.code as ba_code
    FROM vao_list v
    LEFT OUTER JOIN vao_base b
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


@app.route('/la_areas/')
def la_areas_list():
    sql = '''
    SELECT code, "desc" FROM c_la
    WHERE LEFT(code, 1) IN ('E', 'w')
    ORDER BY "desc"
    '''
    output = show_result(sql)
    output['links'][1] = ('la_areas', 'la_code', 0)
    return render_template('table_output.html', data=output)


@app.route('/la_areas/<la_code>')
def la_areas(la_code):
    data = {'la_code': la_code}
    sql = '''
    SELECT
    scat_code,
    count,
    total_m2,
    total_value,
    median_m2,
    median_price_per_m2,
    national_price_per_m2
    FROM s_vao_area_la_by_scat t
    WHERE la_code = :la_code
    ORDER BY scat_code
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
    la_code,
    count,
    total_m2,
    total_value,
    median_m2,
    median_price_per_m2,
    national_price_per_m2
    FROM s_vao_area_la_by_scat t
    LEFT JOIN c_la s ON s.code = t.la_code
    WHERE scat_code = :scat_code
    ORDER BY s.desc
    '''
    output = show_result(sql, data=data)
    return render_template('table_output.html', data=output)


@app.route('/scat_diff/')
def scat_diff():
    sql = '''
    SELECT l.uarn, l.scat_code, b.scat_code, l.la_code
    FROM vao_list l
    LEFT JOIN vao_base b ON b.uarn = l.uarn
    WHERE l.scat_code != b.scat_code
    '''
    output = show_result(sql)
    return render_template('table_output.html', data=output)


@app.route('/spending_nuts1/')
def spending_nuts1_list():
    sql = '''
    SELECT code, "desc" FROM c_nuts1
    ORDER BY "desc"
    '''
    output = show_result(sql)
    output['links'][1] = ('spending_nuts1', 'nuts1', 0)
    return render_template('table_output.html', data=output)


@app.route('/spending_nuts1/<nuts1>')
def spending_nuts1(nuts1):
    nuts1_desc = codes_data.get('nuts1').get(nuts1)
    if not nuts1_desc:
        abort(404)
    sql = '''
    SELECT c.desc, c.ct_level, s.ct_code,
    s.adj_spend_per_capita, s.percent_from_national
    FROM c_ct c
    LEFT JOIN s_consumer_spend_by_nuts1 s
    ON c.code = s.ct_code
    WHERE nuts1_code = :nuts1
    ORDER BY
    c.l1 NULLS FIRST,
    c.l2 NULLS FIRST,
    c.l3 NULLS FIRST
    '''
    output = run_sql(sql, nuts1=nuts1)
    return render_template(
        'ct_spending.html', data=output, nuts1_desc=nuts1_desc
    )


@app.route('/premises/<uarn>')
def premises(uarn):
    data = {'uarn': uarn}
    output = []

    tables = [
        'vao_list',
        's_vao_premises_area',
        'vao_base',
        'vao_line',
        'vao_additions',
        'vao_plant',
        'vao_parking',
        'vao_adj',
        'vao_adj_totals',
    ]

    single_row_tables = [
        'vao_list',
        'vao_base',
    ]

    for table in tables:
        sql = 'SELECT * FROM %s WHERE uarn = :uarn' % table
        out = show_result(sql, table, data=data)
        out['offset'] = ''
        out['title'] = table
        output.append((out, table in single_row_tables))

    return render_template('premises.html', output=output)


@app.route('/premises/')
def premises_search():
    uarn = request.args.get('uarn')
    if uarn:
        data = {'uarn': uarn.strip()}
        sql = '''
        SELECT uarn
        FROM vao_list
        WHERE uarn = :uarn
        '''
        results = run_sql(sql, data)
        for result in results:
            return redirect(url_for('premises', uarn=uarn))

    return render_template('uarn.html', uarn=uarn)


@app.route('/ba_ref/')
def ba_ref_premises():
    ba_ref = request.args.get('ba_ref')
    ba_code = request.args.get('ba_code')
    uarn = None
    if ba_ref:
        data = {'ba_ref': ba_ref.strip(), 'ba_code': ba_code}
        sql = '''
        SELECT uarn
        FROM vao_list
        WHERE ba_ref = :ba_ref
        AND ba_code = :ba_code
        '''
        results = run_sql(sql, data)
        for result in results:
            uarn = result[0]
            break
        if uarn:
            return redirect(url_for('premises', uarn=uarn))

    codes = codes_data.get('ba')
    ba_codes = []
    for k, v in codes.iteritems():
        ba_codes.append((v, k))
    ba_codes = [('Select Billing authority', None)] + sorted(ba_codes)
    return render_template('ba_ref.html', ba_ref=ba_ref, ba_codes=ba_codes, ba_code=ba_code)


@app.route('/postcode/')
def postcode_premises():
    outcode = None
    postcode = request.args.get('postcode')
    sql = None
    if postcode:
        postcode = postcode.upper().strip()
        if re.match('^[A-Z]{1,2}\d{1,2}[A-Z]?$', postcode):
            outcode = postcode
        else:
            outcode = None
    if outcode:
        print('outcode %s' % outcode)
        data = {'outcode': outcode}
        sql = '''
        SELECT v.uarn uarn, v.ba_ref, v.pc, v.town, b.uarn summary, v.scat_code
        FROM vao_list v
        LEFT OUTER JOIN vao_base b ON b.uarn = v.uarn
        LEFT JOIN c_scat s ON s.code = v.scat_code
        WHERE v.outcode = :outcode
        ORDER BY s.desc, v.pc
        '''
    elif postcode:
        print('postcode %s' % postcode)
        data = {'postcode': postcode + '%'}
        sql = '''
        SELECT v.uarn uarn, v.ba_ref, v.pc, v.town, b.uarn summary, v.scat_code
        FROM vao_list v
        LEFT OUTER JOIN vao_base b ON b.uarn = v.uarn
        LEFT JOIN c_scat s ON s.code = v.scat_code
        WHERE v.pc LIKE :postcode
        ORDER BY s.desc, v.pc
        '''
    if sql:
        output = show_result(sql, data=data)
        output['functions'][3] = (add_yes,)
    else:
        output = None
        postcode = ''
    return render_template('postcode.html', data=output, postcode=postcode)


@app.route('/scat_group_area_graph/')
def scat_group_area_graph():
    output = {}

    sql = '''
    SELECT code, "desc", median_m2, m.count
    FROM c_scat_group sg
    LEFT JOIN s_vao_scat_group_median_areas m on m.scat_group_code = sg.code
    '''

    scat_groups = run_sql(sql)

    for scat_group, desc, area, sg_count in scat_groups:
        sql = '''
        SELECT s.desc, m.median_m2, m.count
        FROM s_vao_scat_median_areas m
        LEFT JOIN c_scat s ON s.code = m.scat_code
        WHERE m.median_m2 > 0 and s.scat_group_code = :scat_group
        ORDER BY m.median_m2
        '''
        scat_info = []
        results = run_sql(sql, scat_group=scat_group)
        for row in results:
            scat_info.append([row[0], row[1], row[2]])

        sql = '''
        SELECT round(s.median_m2/5)*5, count(*)
        FROM s_vao_base_areas_scat_group s
        WHERE scat_group_code = :scat_group
        GROUP BY round(s.median_m2/5)*5
        ORDER BY round(s.median_m2/5)*5
        '''

        results = run_sql(sql, scat_group=scat_group)
        counts = ['count']
        values = []
        for value, count in results:
            values.append(value)
            counts.append(int(count))
        output[scat_group] = {
            'values': values,
            'counts': counts,
            'desc': desc,
            'area': area,
            'count': sg_count,
            'scat_info': scat_info,
        }

    return render_template('base_c3.html', output=output)
