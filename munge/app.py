import re

from flask import Flask, render_template, url_for, abort
app = Flask(__name__)


from sa_util import run_sql, get_result_fields, table_list


def show_result(sql, table, data=None):
    # We need to have a result to get the field types
    if data is None:
        data = {}
    print sql
    result = run_sql(sql + ' LIMIT 1', data)
    fields = get_result_fields(result, table)
    # Now run query
    result = run_sql(sql, data)
    return {'fields': fields,
            'data': result}


def show_table(table):
    sql = 'SELECT * FROM "%s"' % table
    return show_result(sql, table)


@app.route('/')
def home():
    return render_template('home.html')


@app.route('/table/')
def tables():
    match = '[cl]\_.*'
    out = []
    for table in sorted(table_list()):
        if re.match(match, table):
            out.append(table)
    return render_template('tables.html', data=out)

@app.route('/table/<table>')
def table(table=None):
    match = '[cl]\_.*'
    if not re.match(match, table) or table not in table_list():
        abort(404)
    data = show_table(table)
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
              'links': {1: ('ba_premises_list', 'ba_code', 0)},
              }
    return render_template('table_output.html', data=output)


@app.route('/ba/<ba_code>')
def ba_premises_list(ba_code):
    data = {'ba_code': ba_code}
    sql = '''
    SELECT uarn, s.desc
    FROM vao_list v
    LEFT JOIN c_scat s ON s.code = v.scat_code
    WHERE ba_code = :ba_code
    ORDER BY s.desc
    '''
    result = run_sql(sql, data)
    fields = [
        {'name': 'uarn'},
        {'name': 'scat code'},
    ]
    output = {'fields': fields,
              'data': result,
              'links': {0: ('premises', 'uarn', 0)},
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
              'links': {1: ('scat_premises_list', 'scat_code', 0)},
              }
    return render_template('table_output.html', data=output)


@app.route('/scat/<scat_code>')
def scat_premises_list(scat_code):
    data = {'scat_code': scat_code}
    sql = '''
    SELECT uarn, b.desc
    FROM vao_list v
    LEFT JOIN c_ba b ON b.code = v.ba_code
    WHERE scat_code = :scat_code
    ORDER BY b.desc
    '''
    result = run_sql(sql, data)
    fields = [
        {'name': 'uarn'},
        {'name': 'billing authouity'},
    ]
    output = {'fields': fields,
              'data': result,
              'links': {0: ('premises', 'uarn', 0)},
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
        out = show_result(sql, table, data)
        out['title'] = table
        output.append(out)

    return render_template('premises.html', output=output)


# select uarn, scat_code from vao_list where ba_code='0335';
