import re

from sqlalchemy.engine import reflection

from flask import Flask, render_template, url_for, abort, request
from flask.ext.sqlalchemy import SQLAlchemy
from werkzeug import url_encode

import sa_common
import config


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = config.CONNECTION_STRING
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


@app.template_global()
def modify_query(**new_values):
    args = request.args.copy()

    for key, value in new_values.items():
        args[key] = value

    return '{}?{}'.format(request.path, url_encode(args))


def run_sql(*args, **kw):
    return sa_common.run_sql(db.engine, *args, **kw)


def get_indexes(*args, **kw):
    return sa_common.get_indexes(db.engine, *args, **kw)


def get_primary_keys(*args, **kw):
    return sa_common.get_primary_keys(db.engine, *args, **kw)


def table_list(*args, **kw):
    return sa_common.table_list(db.engine, *args, **kw)


def auto_links(fields):
    links = {}
    for i, field in enumerate(fields):
        if field.get('name') == 'uarn':
            links[i] = ('premises', 'uarn', i)
    return links


def show_result(sql, table, data=None, offset=0):
    # We need to have a result to get the field types
    if data is None:
        data = {}
    result = run_sql(sql + ' LIMIT 1', data)
    fields = sa_common.get_result_fields(db.engine, result, table)
    # Now run query
    result = run_sql(sql + (' LIMIT 1000 OFFSET %s' % offset), data)
    return {
        'fields': fields,
        'data': result,
        'offset': offset,
        'links': auto_links(fields),
    }


def show_table(table, offset=0):
    sql = 'SELECT * FROM "%s"' % table
    return show_result(sql, table, offset=offset)


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
    offset = int(request.args.get('offset', 0))
    match = '[cls]\_.*'
    if not re.match(match, table) or table not in table_list():
        abort(404)
    data = show_table(table, offset)
    data['title'] = 'TABLE %s' % table

    return render_template('table_output.html', data=data)


@app.route('/ba/')
def ba_list():
    sql = 'SELECT code, "desc" FROM c_ba ORDER BY "desc"'
    result = run_sql(sql)
    fields = [
        {'name': 'code'},
        {'name': 'desc'},
    ]
    output = {'fields': fields,
              'data': result,
              'offset': '',
              'links': {1: ('ba_premises_list', 'ba_code', 0)},
              }
    return render_template('table_output.html', data=output)


def add_yes(arg):
    if arg:
        return 'YES'
    return ''


@app.route('/ba/<ba_code>')
def ba_premises_list(ba_code):
    data = {'ba_code': ba_code}
    sql = '''
    SELECT v.uarn, b.uarn, s.desc
    FROM vao_list v
    LEFT OUTER JOIN vao_base b
    ON b.uarn = v.uarn
    LEFT JOIN c_scat s ON s.code = v.scat_code
    WHERE v.ba_code = :ba_code
    ORDER BY s.desc
    '''
    result = run_sql(sql, data)
    fields = [
        {'name': 'uarn'},
        {'name': 'summary'},
        {'name': 'scat code'},
    ]
    output = {'fields': fields,
              'data': result,
              'offset': '',
              'links': {0: ('premises', 'uarn', 0)},
              'functions': {1: add_yes},
              }
    return render_template('table_output.html', data=output)


@app.route('/scat/')
def scat_list():
    sql = 'SELECT code, "desc" FROM c_scat ORDER BY "desc"'
    result = run_sql(sql)
    fields = [
        {'name': 'code'},
        {'name': 'desc'},
    ]
    output = {'fields': fields,
              'data': result,
              'offset': '',
              'links': {1: ('scat_premises_list', 'scat_code', 0)},
              }
    return render_template('table_output.html', data=output)


@app.route('/scat/<scat_code>')
def scat_premises_list(scat_code):
    data = {'scat_code': scat_code}
    sql = '''
    SELECT v.uarn, b.uarn, c.desc
    FROM vao_list v
    LEFT OUTER JOIN vao_base b
    ON b.uarn = v.uarn
    LEFT JOIN c_ba c ON c.code = v.ba_code
    WHERE v.scat_code = :scat_code
    ORDER BY c.desc
    '''
    result = run_sql(sql, data)
    fields = [
        {'name': 'uarn'},
        {'name': 'summary'},
        {'name': 'billing authouity'},
    ]
    output = {'fields': fields,
              'data': result,
              'offset': '',
              'links': {0: ('premises', 'uarn', 0)},
              'functions': {1: add_yes},
              }
    return render_template('table_output.html', data=output)


@app.route('/ba_areas/')
def ba_areas_list():
    sql = 'SELECT code, "desc" FROM c_ba ORDER BY "desc"'
    result = run_sql(sql)
    fields = [
        {'name': 'code'},
        {'name': 'desc'},
    ]
    output = {'fields': fields,
              'data': result,
              'offset': '',
              'links': {1: ('ba_areas', 'ba_code', 0)},
              }
    return render_template('table_output.html', data=output)


@app.route('/ba_areas/<ba_code>')
def ba_areas(ba_code):
    data = {'ba_code': ba_code}
    sql = '''
    SELECT
    s.desc,
    count,
    total_m2,
    total_value,
    total_area_price
    FROM s_vao_base_areas t
    LEFT JOIN c_scat s ON s.code = t.scat_code
    WHERE ba_code = :ba_code
    ORDER BY s.desc
    '''
    result = run_sql(sql, data)
    fields = [
        {'name': 'scat code'},
        {'name': 'number of premises'},
        {'name': 'total m2'},
        {'name': 'total_value'},
        {'name': 'total_area_price'},
    ]
    output = {'fields': fields,
              'data': result,
              'offset': '',
              'links': {},
              }
    return render_template('table_output.html', data=output)


@app.route('/scat_areas/')
def scat_areas_list():
    sql = 'SELECT code, "desc" FROM c_scat ORDER BY "desc"'
    result = run_sql(sql)
    fields = [
        {'name': 'code'},
        {'name': 'desc'},
    ]
    output = {'fields': fields,
              'data': result,
              'offset': '',
              'links': {1: ('scat_areas', 'scat_code', 0)},
              }
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
    result = run_sql(sql, data)
    fields = [
        {'name': 'billing authority'},
        {'name': 'number of premises'},
        {'name': 'total m2'},
        {'name': 'total_value'},
        {'name': 'total_area_price'},
    ]
    output = {'fields': fields,
              'data': result,
              'offset': '',
              'links': {},
              }
    return render_template('table_output.html', data=output)



@app.route('/premises/<uarn>')
def premises(uarn):
    data = {'uarn': uarn}
    output = []

    tables = [
        'vao_list',
        'vao_base',
        'vao_line',
        'vao_additions',
        'vao_plant',
        'vao_parking',
        'vao_adj',
        'vao_adj_totals',
    ]

    for table in tables:
        sql = 'SELECT * FROM %s WHERE uarn = :uarn' % table
        out = show_result(sql, table, data=data)
        out['offset'] = ''
        out['title'] = table
        output.append(out)

    return render_template('premises.html', output=output)


# select uarn, scat_code from vao_list where ba_code='0335';
