import csv
import os.path
import glob
import re
import collections

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
from common import process_header


# FIXME add logging of imports


def unicode_csv_reader(filename, **kw):
    encoding = kw.pop('encoding', 'utf-8')
    dialect = kw.pop('dialect', csv.excel)
    with open(filename, 'rb') as f:
        reader = csv.reader(f, dialect=dialect, **kw)
        for row in reader:
            yield [unicode(cell, encoding) for cell in row]


def get_fns(fields):
    fns = collections.OrderedDict()
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
               verbose=0, limit=None):
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
                    print(fn)
                    print(row_data)
                    skip = True
            if not skip:
                data.append(row_data)
            if count % config.BATCH_SIZE == 0 and count:
                run_sql(insert_sql, data)
                data = []
                if verbose:
                    print('{table}: {count:,}'.format(
                        table=table_name, count=count
                    ))
            if not skip:
                count += 1
            if limit and count == limit:
                break
        first = False
    if data:
        run_sql(insert_sql, data)

    if verbose:
        print('{table}: {count:,} rows imported'.format(
            table=table_name, count=count
        ))
    # Add indexes
    build_indexes(temp_table, t_fields, verbose=verbose)


def import_single(filename, table_name, verbose=0, **kw):
    if verbose:
        print('importing %s' % table_name)
    reader = unicode_csv_reader(filename, **kw)
    import_csv(reader, table_name, verbose=verbose)


def get_csv_files(directory):
    return glob.glob(os.path.join(config.DATA_PATH, directory, '*.csv'))


def table_name_from_path(p):
    return os.path.splitext(os.path.basename(p))[0]


def csv_table_list(directory):
    return [table_name_from_path(f) for f in get_csv_files(directory)]


def import_all(directory, verbose=0):
    for f in get_csv_files(directory):
        table_name = table_name_from_path(f)
        import_single(f, table_name, verbose=verbose)


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
