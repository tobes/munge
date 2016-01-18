import time

import sqlalchemy as sa

import sa_common
import config


engine = sa.create_engine(config.CONNECTION_STRING, echo=False)
conn = engine.connect()


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


def dependent_objects():
    return sa_common.dependent_objects(engine)


def get_sequence_names():
    return sa_common.get_sequence_names(engine)


def table_list():
    return sa_common.table_list(engine)


def get_result_fields(*args, **kw):
    return sa_common.get_result_fields(engine, *args, **kw)


def clear_temp_objects(verbose=False):
    dependents = dependent_objects()
    # tables
    tables = [t for t in table_view_list() if t.startswith(config.TEMP_TABLE_STR)]
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
        sql = 'DROP TABLE {table}'.format(table=quote(table))
    elif table in view_list():
        sql = 'DROP VIEW {table}'.format(table=quote(table))
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


def create_table(table, fields, primary_key=None, verbose=0):
    sql = 'DROP TABLE IF EXISTS %s' % quote(table)
    run_sql(sql)
    sql = ['CREATE TABLE %s (' % quote(table)]
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


def build_indexes(table_name, t_fields, verbose=False):
    # get indexed fields
    index_fields = [
        (f['index_key'], f['name'])
        for f in t_fields if f.get('index')
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


def _build_summary(data, verbose=False, limit=None):
    table_name = data['name']
    sql = data['sql']
    tables = data['tables']
    primary_key = data.get('primary_key')

    table_name_temp = config.TEMP_TABLE_STR + table_name
    if verbose:
        print('creating summary table %s' % table_name)
    tables_dict = make_tables_dict(tables)
    if verbose > 1:
        print(sql.format(**tables_dict))
    result = run_sql(sql.format(**tables_dict))
    first = True
    count = 0
    data = []
    for row in result:
        if first:
            fields = get_result_fields(result)
            create_table(table_name_temp, fields, primary_key=primary_key, verbose=verbose)
            f = [field['name'] for field in fields if not field.get('missing')]
            insert_sql = insert_rows(table_name_temp, fields)
            first = False
        data.append(dict(zip(f, row)))
        count += 1
        if count % config.BATCH_SIZE == 0:
            run_sql(insert_sql, data)
            data = []
            if verbose:
                print('{table}: {count:,}'.format(
                    table=table_name, count=count
                ))
        if limit and count == limit:
            break
    if data:
        run_sql(insert_sql, data)

    if verbose:
        print('{table}: {count:,} rows imported'.format(
            table=table_name, count=count
        ))
    if count:
        # Add indexes
        build_indexes(table_name_temp, fields, verbose=verbose)


def build_view(data, verbose=0):
    view_name = data['name']
    view_name = config.TEMP_TABLE_STR + view_name
    sql = 'CREATE VIEW {name} AS\n'.format(name=quote(view_name))
    sql += data['sql']
    tables = data['tables']
    drop_sql = 'DROP VIEW IF EXISTS "%s"' % view_name
    run_sql(drop_sql)
    if verbose:
        print('creating view %s' % view_name)
    tables_dict = make_tables_dict(tables)
    if verbose > 1:
        print(sql.format(**tables_dict))
    run_sql(sql.format(**tables_dict))


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


def build_views_and_summaries(data, verbose=0, just_views=False, test_only=False):
    for info in data:
        if info.get('disabled'):
            continue
        if info.get('as_view'):
            time_fn(build_view, args=[info], verbose=verbose)
        else:
            if test_only and not info.get('test'):
                continue
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
