import csv
import os.path
import glob
import re

import config
import import_fns
from sa_util import (
    swap_tables,
    run_sql,
    table_list,
    get_result_fields,
    create_table,
    insert_rows,
    build_indexes,
)

# FIXME add logging of imports


def process_header(row):
    fields = []
    pk = None
    for col in row:
        # null ignored fields
        if col == '':
            col = {'name': '', 'type': None}
            fields.append(col)
            continue
        # defaults
        pk = False
        index = False
        index_key = None
        missing = False
        fn = None
        fn_field = None
        # field datatype
        if ':' in col:
            field, type_ = col.split(':')
        else:
            field = col
            type_ = 'text'
        # ignored fields
        if col[0] == '-':
            field = field[1:]
            type_ = None
        # primary key
        if field[0] == '*':
            field = field[1:]
            pk = True
        # index
        if field[0] == '+':
            field = field[1:]
            index = True
            reg_ex = '\{(\d+)\}'
            m = re.match(reg_ex, field)
            if m:
                index_key = m.group(1)
                field = re.sub(reg_ex, '', field)
        # field not supplied in data
        if field[0] == '@':
            field = field[1:]
            missing = True
        # conversion function
        if type_ and '~' in type_:
            type_, fn = type_.split('~')
            if '|' in fn:
                fn, fn_field = fn.split('|')

        col = {
            'name': field,
            'type': type_,
            'pk': pk,
            'index': index,
            'index_key': index_key,
            'fn': fn,
            'fn_field': fn_field,
            'missing': missing,
        }
        fields.append(col)
    return fields


def unicode_csv_reader(filename, **kw):
    encoding = kw.pop('encoding', 'utf-8')
    dialect = kw.pop('dialect', csv.excel)
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
            fns[field['name']] = (
                getattr(import_fns, import_fns.AUTO_FNS[field['type']]),
                None
            )
    return fns


def import_csv(reader, table_name, fields=None, skip_first=False,
               verbose=False, limit=None):
    temp_table = config.TEMP_TABLE_STR + table_name
    count = 0
    t_fields = []
    data = []
    has_header_row = (fields is None) or skip_first
    first = True
    for row in reader:
        skip = False
        if first:
            if fields is None:
                fields = row
            t_fields = process_header(fields)
            t_fns = get_fns(t_fields)
            create_table(temp_table, t_fields, verbose=verbose)
            f = [
                field['name'] for field in t_fields
                if not field.get('missing')
            ]
            insert_sql = insert_rows(temp_table, t_fields)
        if not (has_header_row and first):
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
                    print(str(e))
                    print(row)
                    print(row_data)
                    skip = True
            if not skip:
                data.append(row_data)
            if count % config.BATCH_SIZE == 0 and count:
                run_sql(insert_sql, data)
                data = []
                if verbose:
                    print(count)
            if not skip:
                count += 1
            if limit and count == limit:
                break
        first = False
    if data:
        run_sql(insert_sql, data)

    if verbose:
        print('%s rows imported' % (count))
    # Add indexes
    build_indexes(temp_table, t_fields, verbose=verbose)


def import_single(filename, table_name, verbose=False, **kw):
    if verbose:
        print('importing %s' % table_name)
    reader = unicode_csv_reader(filename, **kw)
    import_csv(reader, table_name, verbose=verbose)


def import_all(verbose=False):
    files = glob.glob(os.path.join(config.DATA_PATH, 'import', '*.csv'))
    for f in files:
        table_name = os.path.splitext(os.path.basename(f))[0]
        import_single(f, table_name, verbose=verbose)
    swap_tables(verbose=verbose)


def import_drop_code_tables(verbose=False):
    files = glob.glob(os.path.join(config.DATA_PATH, 'import', 'c_*.csv'))
    tables = [t for t in table_list() if t.startswith('c_')]
    files = [os.path.splitext(os.path.basename(f))[0] for f in files]
    for table in tables:
        if table not in files:
            if verbose:
                print('Drop table %s' % table)
            sql = 'DROP TABLE "{table}";'.format(table=table)
            run_sql(sql)


def import_drop_lookup_tables(verbose=False):
    files = glob.glob(os.path.join(config.DATA_PATH, 'import', 'l_*.csv'))
    tables = [t for t in table_list() if t.startswith('l_')]
    files = [os.path.splitext(os.path.basename(f))[0] for f in files]
    for table in tables:
        if table not in files:
            if verbose:
                print('Drop table %s' % table)
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
        print('Processing %s' % table)
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
                    print('\nFields:')
                    for h in headers:

                        print('\t%s  \t%s' % tuple(h.split(':')))
                    print
                wrote_headers = True
            a.writerows([row])
            count += 1
        if verbose:
            print('%s rows written' % (count - 1))
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
