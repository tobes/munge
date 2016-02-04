import time

import sqlalchemy as sa

import sa_common
import config
from common import process_header
from summeries import update_summary_table as _update_summary_table


engine = sa.create_engine(config.CONNECTION_STRING, echo=False)
conn = engine.connect()

fields_match = sa_common.fields_match


def run_sql(*args, **kw):
    return sa_common.run_sql(engine, *args, **kw)


def get_indexes(*args, **kw):
    return sa_common.get_indexes(engine, *args, **kw)


def get_primary_keys(*args, **kw):
    return sa_common.get_primary_keys(engine, *args, **kw)


def get_pk_constraint(*args, **kw):
    return sa_common.get_pk_constraint(engine, *args, **kw)


def table_list():
    return sa_common.table_list(engine)


def view_list():
    return sa_common.view_list(engine)


def table_view_list():
    return sa_common.table_view_list(engine)


def table_columns(table_name):
    return sa_common.table_columns(engine, table_name)


def dependent_objects():
    return sa_common.dependent_objects(engine)


def get_sequence_names():
    return sa_common.get_sequence_names(engine)


def table_list():
    return sa_common.table_list(engine)


def get_result_fields(*args, **kw):
    return sa_common.get_result_fields(engine, *args, **kw)


def update_summary_table(data, created=True):
    name = data['name']
    tables = data['tables']
    importer = data.get('importer')
    description = data.get('summary')
    is_view = data.get('is_view', False)
    _update_summary_table(name,
                          description=description,
                          dependencies=tables,
                          importer=importer,
                          is_view=is_view,
                          created=created)


def clear_temp_objects(verbose=0):
    dependents = dependent_objects()
    # tables
    tables = [
        t for t in table_view_list()
        if t.startswith(config.TEMP_TABLE_STR)
    ]
    for table in tables:
        if table not in dependents:
            drop_table_or_view(table, verbose=verbose)
    # sequences
    sequence_names = [
        s for s in get_sequence_names()
        if s.startswith(config.TEMP_TABLE_STR)
    ]
    for name in sequence_names:
        if verbose:
            print('Dropping sequence %s' % name)
        sql = 'DROP SEQUENCE {name};'.format(name=quote(name))


def drop_sql(table, force=False):
    sql = ''
    if table in table_list():
        sql = 'DROP TABLE IF EXISTS {table}'.format(table=quote(table))
    elif table in view_list():
        sql = 'DROP VIEW IF EXISTS {table}'.format(table=quote(table))
    if sql:
        if force:
            sql = sql + ' CASCADE'
        sql = sql + ';'
    return sql


def drop_table_or_view(table, verbose=0):
    sql = None
    if verbose:
        print('Dropping %s' % table)
    sql = drop_sql(table)
    if verbose < 1:
        print(sql)
    run_sql(sql)


def truncate_table(table, verbose=0):
    sql = '''
        BEGIN;
        TRUNCATE TABLE {table};
        COMMIT;
    '''.format(table=quote(table))
    if verbose:
        print('Truncating %s' % table)
    if verbose < 1:
        print(sql)
    run_sql(sql)


def quoted_temp_table_name(name):
    return quote(config.TEMP_TABLE_STR + name)


def swap_tables(verbose=0, force=False):
    ''' SWAPS our temp tables, including renaming indexes and sequences
    '''
    temp_table_str = config.TEMP_TABLE_STR
    tmp_label_len = len(temp_table_str)
    tables = [t for t in table_list() if t.startswith(temp_table_str)]
    sql_list = []
    sql_list.append('BEGIN;')
    # views
    view_names = [
        s[tmp_label_len:]
        for s in view_list()
        if s.startswith(temp_table_str)
    ]
    for name in view_names:
        if verbose:
            print('Swap view %s' % name)
        sql = drop_sql(name, force=force)
        sql += '''
            ALTER VIEW {old_name} RENAME TO {name};
        '''
        sql_list.append(sql.format(
            name=quote(name), old_name=quoted_temp_table_name(name)
        ))

    for table in tables:
        if verbose:
            print('Swap table %s' % table)
        indexes = [i['name'][tmp_label_len:] for i in get_indexes(table)]
        pk = get_pk_constraint(table)
        if pk['name']:
            indexes.append(pk['name'][tmp_label_len:])

        table = table[tmp_label_len:]
        sql = drop_sql(table, force=force)
        sql += '''
        ALTER TABLE {old_table} RENAME TO {table};
        '''
        sql_list.append(sql.format(
            table=quote(table),
            old_table=quoted_temp_table_name(table),
        ))
        for index in indexes:
            if verbose:
                print('\tSwap index %s' % index)
            sql = 'ALTER INDEX {old_index} RENAME TO {index};'
            sql = sql.format(
                index=quote(index),
                old_index=quoted_temp_table_name(index),
            )
            sql_list.append(sql)

    # sequences
    sequence_names = [
        s[tmp_label_len:]
        for s in get_sequence_names()
        if s.startswith(temp_table_str)
    ]
    for name in sequence_names:
        if verbose:
            print('\tSwap sequence %s' % name)
        sql = 'ALTER SEQUENCE {old_name} RENAME TO {name};'
        sql_list.append(sql.format(
            name=quote(name),
            old_name=quoted_temp_table_name(name),
        ))

    sql_list.append('COMMIT;')
    if verbose > 1:
        print('\n'.join(sql_list))
    conn.execute('\n'.join(sql_list))


def quote(arg):
    ''' Double quote the arg '''
    return '"%s"' % arg


def make_tables_dict(tables):
    all_tables = table_view_list()
    output = {}
    for i, table in enumerate(tables):
        if config.TEMP_TABLE_STR + table in all_tables:
            name = config.TEMP_TABLE_STR + table
        else:
            name = table
        output['t%s' % (i + 1)] = quote(name)
    return output


def create_table(table, fields, primary_key=None, verbose=0, keep=False):
    if not keep:
        sql = 'DROP TABLE IF EXISTS %s' % quote(table)
        run_sql(sql)
    sql = ['CREATE TABLE IF NOT EXISTS %s (' % quote(table)]
    sql_fields = []
    for field in fields:
        # Skipped field
        if field['type'] is None:
            continue
        col = '\t"%s"\t%s' % (
            field['name'],
            field['type']
        )
        sql_fields.append(col)
    # Primary Key
    pk = []
    if primary_key:
        if isinstance(primary_key, basestring):
            primary_key = [primary_key]
        pk = primary_key
    for field in fields:
        if field['pk']:
            pk.append('"%s"' % field['name'])
    if pk:
        sql_fields.append('\tPRIMARY KEY (%s)' % ', '.join(pk))
    sql.append(',\n'.join(sql_fields))
    sql.append(')')
    sql = '\n'.join(sql)
    if verbose:
        print('Creating table %s' % table)
        if verbose > 1:
            print(sql)
    run_sql(sql)


def build_indexes(table_name, t_fields, verbose=0):
    # get indexed fields
    index_fields = [
        (f['index_key'], f['name'])
        for f in t_fields if f.get('indexed')
    ]
    # group them by index_key
    index_dict = {}
    for key, name in index_fields:
        index_dict.setdefault(key, []).append(name)
    # unpack into individual indexes
    indexes = []
    for k, v in index_dict.iteritems():
        if k is None:
            for ind in v:
                indexes.append([ind])
        else:
            indexes.append(v)
    sql_list = []
    for index in indexes:
        quoted_index_fields = ['"%s"' % i for i in index]
        sql = 'CREATE INDEX "{idx_name}" ON "{table}" ({index});'
        sql = sql.format(
            idx_name='%s_idx_%s' % (table_name, '_'.join(index)),
            table=table_name,
            index=', '.join(quoted_index_fields),
        )
        sql_list.append(sql)
        if verbose:
            print('creating index of %s' % index)
            print sql
    if sql_list:
        run_sql('\n'.join(sql_list))


def insert_rows(table, fields):
    sql = ['INSERT INTO "%s" (' % table]
    sql_fields = []
    data_fields = []
    for field in fields:
        if field['type'] is None:
            continue
        if field.get('missing') is True and field.get('fn') is None:
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


def results_dict(sql):
    first = True
    count = 0
    results = run_sql(sql)
    for row in results:
        if first:
            fields = get_result_fields(results)
            f = [field['name'] for field in fields]
            first = False
        row_data = dict(zip(f, row))
        yield row_data


def _build_summary(data, verbose=0, limit=None):
    table_name = data['name']
    sql = data['sql']
    tables = data['tables']

    if verbose:
        print('creating summary table %s' % table_name)
    tables_dict = make_tables_dict(tables)
    if verbose > 1:
        print(sql.format(**tables_dict))
    results = run_sql(sql.format(**tables_dict))

    row_function = data.get('row_function')
    table_function = data.get('table_function')
    if row_function:
        _results_to_row_function(row_function, data, results, verbose=verbose, limit=limit)
    if table_function:
        _results_to_table_function(table_function, data, results, verbose=verbose, limit=limit)
    else:
        _results_to_table(data, results, verbose=verbose, limit=limit)

def _results_to_table_function(function, data, results, verbose=0, limit=None):
    table_name = data['name']
    fields_data = data['fields']
    primary_key = data.get('primary_key')
    table_name_temp = config.TEMP_TABLE_STR + table_name
    first = True
    count = 0
    out = []
    for row in function(results, data, verbose=verbose):
        if first:
            fields = process_header(fields_data)
            f = [field['name'] for field in fields]
            create_table(table_name_temp,
                         fields,
                         primary_key=primary_key,
                         verbose=verbose)
            insert_sql = insert_rows(table_name_temp, fields)
            first = False
        else:
            row_data = dict(zip(f, row))
            out.append(row_data)
            count += 1
            if count % config.BATCH_SIZE == 0:
                run_sql(insert_sql, out)
                out = []
                if verbose:
                    print('{table}: {count:,}'.format(
                        table=table_name, count=count
                    ))
        if limit and count == limit:
            break
    if out:
        run_sql(insert_sql, out)

    if verbose:
        print('{table}: {count:,} rows imported'.format(
            table=table_name, count=count
        ))
    if count:
        # Add indexes
        build_indexes(table_name_temp, fields, verbose=verbose)
    update_summary_table(data)



def _results_to_row_function(function, data, results, verbose=0, limit=None):
    table_name = data['name']
    fields_data = data['fields']
    primary_key = data.get('primary_key')
    table_name_temp = config.TEMP_TABLE_STR + table_name
    first = True
    count = 0
    output = []
    for row in results:
        if first:
            fields = get_result_fields(results)
            f = [field['name'] for field in fields]
        row_data = dict(zip(f, row))
        row_data = function(row_data, verbose=verbose)
        if first:
            fields = process_header(fields_data)
            create_table(table_name_temp,
                         fields,
                         primary_key=primary_key,
                         verbose=verbose)
            insert_sql = insert_rows(table_name_temp, fields)
            first = False
        output.append(row_data)
        count += 1
        if count % config.BATCH_SIZE == 0:
            run_sql(insert_sql, output)
            output = []
            if verbose:
                print('{table}: {count:,}'.format(
                    table=table_name, count=count
                ))
        if limit and count == limit:
            break
    if output:
        run_sql(insert_sql, output)

    if verbose:
        print('{table}: {count:,} rows imported'.format(
            table=table_name, count=count
        ))
    if count:
        # Add indexes
        build_indexes(table_name_temp, fields, verbose=verbose)
    update_summary_table(data)


def _results_to_table(data, results, verbose=0, limit=None):
    table_name = data['name']
    primary_key = data.get('primary_key')
    table_name_temp = config.TEMP_TABLE_STR + table_name
    first = True
    count = 0
    output = []
    for row in results:
        if first:
            fields = get_result_fields(results)
            create_table(table_name_temp,
                         fields,
                         primary_key=primary_key,
                         verbose=verbose)
            f = [field['name'] for field in fields]
            insert_sql = insert_rows(table_name_temp, fields)
            first = False
        output.append(dict(zip(f, row)))
        count += 1
        if count % config.BATCH_SIZE == 0:
            run_sql(insert_sql, output)
            output = []
            if verbose:
                print('{table}: {count:,}'.format(
                    table=table_name, count=count
                ))
        if limit and count == limit:
            break
    if output:
        run_sql(insert_sql, output)

    if verbose:
        print('{table}: {count:,} rows imported'.format(
            table=table_name, count=count
        ))
    if count:
        # Add indexes
        build_indexes(table_name_temp, fields, verbose=verbose)
    update_summary_table(data)


def _build_view(data, verbose=0, force=False):
    view_name = data['name']
    temp_view_name = config.TEMP_TABLE_STR + view_name
    sql = 'CREATE VIEW {name} AS\n'.format(name=quote(temp_view_name))
    sql += data['sql']
    drop_sql = 'DROP VIEW IF EXISTS "%s"' % temp_view_name
    if force:
        drop_sql += ' CASCADE'
    run_sql(drop_sql)
    created = True
    if verbose:
        print('creating view %s' % temp_view_name)
    tables = data.get('tables')
    tables_dict = make_tables_dict(tables)
    if verbose > 1:
        print(sql.format(**tables_dict))
    run_sql(sql.format(**tables_dict))
    update_summary_table(data)


def time_fn(fn, args=None, kw=None, verbose=0):
    if not args:
        args = []
    if not kw:
        kw = {}
    kw['verbose'] = verbose
    start = time.time()
    fn(*args, **kw)
    elapsed = int(time.time() - start)
    if verbose and elapsed > 1:
        m, s = divmod(elapsed, 60)
        h, m = divmod(m, 60)
        print "%d:%02d:%02d" % (h, m, s)


def build_views_and_summaries(data, verbose=0, just_views=False, importer=None,
                              test_only=False, force=False, stage=0):
    for info in data:
        info['importer'] = importer
        if info.get('disabled'):
            continue
        if stage != 0 and info.get('stage', 0) != stage:
            continue
        if test_only and not info.get('test'):
            continue
        if info.get('as_view'):
            time_fn(_build_view, args=[info], verbose=verbose, kw={'force': force})
        else:
            if not just_views:
                time_fn(_build_summary, args=[info], verbose=verbose)


def swap_table(old_name, new_name):
    sql = '''
        BEGIN;
        DROP TABLE IF EXISTS {new_name};
        ALTER TABLE {old_name} RENAME TO {new_name};
        COMMIT;
    '''
    sql = sql.format(old_name=quote(old_name), new_name=quote(new_name))
    conn.execute(sql)
