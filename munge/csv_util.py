import csv
import pprint
import os.path
import glob
import re

import config
from sa_util import swap_tables, run_sql, table_list, get_result_fields
import import_fns

# FIXME add logging of imports


def create_table(table, fields):
    sql = 'DROP TABLE IF EXISTS "%s"' % table
    run_sql(sql)
    sql = ['CREATE TABLE "%s" (' % table]
    sql_fields = []
    for field in fields:
        # Skipped field
        if field['type'] is None:
            continue
        col = '\t"%s"\t%s' % (
            field['name'],
            field['type']
        )
        if field['pk']:
            col += ' PRIMARY KEY'

        sql_fields.append(col)
    sql.append(',\n'.join(sql_fields))
    sql.append(')')
    sql = '\n'.join(sql)
    run_sql(sql)


def insert_rows(table, fields):
    sql = ['INSERT INTO "%s" (' % table]
    sql_fields = []
    data_fields = []
    for field in fields:
        if field['type'] is None:
            continue
        if field['missing'] is True and field['fn'] is None:
            continue
        sql_fields.append('"%s"' % (
            field['name']
        ))
        data_fields.append(':%s' % field['name'])
    sql.append(', '.join(sql_fields))
    sql.append(') VALUES ({data})')
    sql = '\n'.join(sql)
    sql = sql.format(data=', '.join(data_fields))
    return sql



def process_header(row):
    fields = []
    pk = None
    for col in row:
        # null ignored fields
        if col == '' or col[0] == '-':
            col = {'name': '', 'type': None}
            fields.append(col)
            continue
        # field datatype
        if ':' in col:
            field, type_ = col.split(':')
        else:
            field = col
            type_ = 'text'
        # primary key
        if field[0] == '*':
            field = field[1:]
            pk = True
        else:
            pk = False
        # index
        if field[0] == '+':
            field = field[1:]
            index = True
        else:
            index = False
        # field not supplied in data
        if field[0] == '@':
            field = field[1:]
            missing = True
        else:
            missing = False
        # conversion function
        if '~' in type_:
            type_, fn = type_.split('~')
            if '|' in fn:
                fn, fn_field = fn.split('|')
            else:
                fn_field = None
        else:
            fn = None
            fn_field = None

        col = {
            'name': field,
            'type': type_,
            'pk': pk,
            'index': index,
            'fn': fn,
            'fn_field': fn_field,
            'missing': missing,
        }
        fields.append(col)
    return fields


def unicode_csv_reader(filename, encoding='utf-8', dialect=csv.excel, **kw):
    with open(filename, 'rb') as f:
        reader = csv.reader(f, dialect=dialect, **kw)
        for row in reader:
            yield [unicode(cell, encoding) for cell in row]


def get_fns(fields):
    fns = {}
    for field in fields:
        if field.get('fn'):
            fn_field = field.get('fn_field')
            fns[field['name']] = (getattr(import_fns, field['fn']), fn_field)
        elif field.get('missing'):
            continue
        elif field['type'] in import_fns.AUTO_FNS:
            fns[field['name']] = (getattr(import_fns, import_fns.AUTO_FNS[field['type']]), None)
    return fns


def build_indexes(table_name, t_fields, verbose=False):
    index_fields = [f['name'] for f in t_fields if f.get('index')]
    sql_list = []
    for field in index_fields:
        if verbose:
            print 'creating index of %s' % field
        sql = 'CREATE INDEX "{idx_name}" ON "{table}" ("{field}");'
        sql = sql.format(
            idx_name='%s_idx_%s' % (table_name, field),
            table=table_name,
            field=field,
        )
        sql_list.append(sql)
    if sql_list:
        run_sql('\n'.join(sql_list))


def import_csv(reader, table_name, fields=None, verbose=False):
    temp_table = u'#' + table_name
    count = 0
    t_fields = []
    data = []
    has_header_row = fields is None
    for row in reader:
        skip = False
        if count == 0:
            if fields is None:
                fields = row
            t_fields = process_header(fields)
            t_fns = get_fns(t_fields)
            create_table(temp_table, t_fields)
            f = [field['name'] for field in t_fields if not field.get('missing')]
            insert_sql = insert_rows(temp_table, t_fields)
        if not (has_header_row and count == 0):
            row_data = dict(zip(f, row))
            for fn in t_fns:
                fn_info = t_fns[fn]
                if fn_info[1]:
                    fn_field = fn_info[1]
                else:
                    fn_field = fn
                try:
                    row_data[fn] = fn_info[0](row_data[fn_field])
                except Exception as e:
                    # FIXME log error
                    print str(e)
                    print row
                    print row_data
                    skip = True
            if not skip:
                data.append(row_data)
        # FIXME only count imported rows
        count += 1
        if count % config.BATCH_SIZE == 0:
            run_sql(insert_sql, data)
            data = []
            if verbose:
                print(count)
    if data:
        run_sql(insert_sql, data)

    if verbose:
        print('%s rows imported' % (count - 1))
    # Add indexes
    build_indexes(temp_table, t_fields, verbose=verbose)


def import_all(verbose=False):
    files = glob.glob(os.path.join(config.DATA_PATH, 'output', '*.csv'))
    for f in files:
        table_name = os.path.splitext(os.path.basename(f))[0]
        if verbose:
            print 'importing', table_name
        reader = unicode_csv_reader(f)
        import_csv(reader, table_name, verbose=verbose)
    swap_tables(verbose=verbose)


def import_drop_code_tables(verbose=False):
    files = glob.glob(os.path.join(config.DATA_PATH, 'output', 'c_*.csv'))
    tables = [t for t in table_list() if t.startswith('c_')]
    files = [os.path.splitext(os.path.basename(f))[0] for f in files]
    for table in tables:
        if table not in files:
            if verbose:
                print 'Drop table %s' % table
            sql = 'DROP TABLE "{table}";'.format(table=table)
            run_sql(sql)


def import_drop_lookup_tables(verbose=False):
    files = glob.glob(os.path.join(config.DATA_PATH, 'output', 'l_*.csv'))
    tables = [t for t in table_list() if t.startswith('l_')]
    files = [os.path.splitext(os.path.basename(f))[0] for f in files]
    for table in tables:
        if table not in files:
            if verbose:
                print 'Drop table %s' % table
            sql = 'DROP TABLE "{table}";'.format(table=table)
            run_sql(sql)


def make_headers(result, table_name):
    fields = get_result_fields(result, table_name)
    headers = []
    for field in fields:
        v = u'%s:%s' % (field['name'], field['type'])
        if field['pk']:
            v = u'*%s' % v
        headers.append(v)
    return headers


def make_csv(filename, sql, **kw):
    table = kw.get('table')
    verbose = kw.get('verbose')

    if not table:
        table = os.path.splitext(os.path.basename(filename))[0]
    if verbose:
        print 'Processing', table
    filename = os.path.join(config.DATA_PATH, 'output', filename)
    with open(filename, 'w') as f:
        a = csv.writer(f, delimiter=',', dialect=csv.excel)

        result = run_sql(sql)
        wrote_headers = False
        count = 0
        for row in result:
            row = [
                x.strip().encode('utf-8')
                if isinstance(x, basestring)
                else x
                for x in row
            ]
            if not wrote_headers:
                # do here so we only need to execute the sql once
                headers = kw.get('headers')
                if not headers:
                    # no headers so get from sqlalchemy
                    headers = make_headers(result, table)
                a.writerows([headers])
                if verbose:
                    print '\nFields:'
                    for h in headers:

                        print '\t%s  \t%s' % tuple(h.split(':'))
                    print
                wrote_headers = True
            a.writerows([row])
            count += 1
        if verbose:
            print '%s rows written' % (count - 1)
            print


def dump(table_name, verbose=False):
    make_csv(
        '%s.csv' % table_name,
        'SELECT * FROM "%s"' % table_name,
        verbose=verbose,
    )


def dump_all(pattern='^[cl]\_.*$', verbose=False):
    for table in table_list():
        if re.search(pattern, table):
            dump(table, verbose=verbose)
