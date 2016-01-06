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
    # tables
    tables = [t for t in table_list() if t.startswith(config.TEMP_TABLE_STR)]
    for table in tables:
        drop_table(table, verbose=verbose)
    # sequences
    sequence_names = [
        s for s in get_sequence_names()
        if s.startswith(config.TEMP_TABLE_STR)
    ]
    for name in sequence_names:
        if verbose:
            print('Dropping sequence %s' % name)
        sql = 'DROP SEQUENCE "{name}";'.format(name=name)


def drop_table(table, verbose=0):
    if verbose:
        print('Dropping table %s' % table)
    sql = 'DROP TABLE "{table}";'.format(table=table)
    if verbose < 1:
        print(sql)
    run_sql(sql)


def swap_tables(verbose=0):
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
        sql = '''
            DROP VIEW IF EXISTS "{name}";
            ALTER VIEW "{TEMP_TABLE_STR}{name}" RENAME TO "{name}";
        '''
        sql_list.append(sql.format(name=name, TEMP_TABLE_STR=temp_table_str))

    for table in tables:
        if verbose:
            print('Swap table %s' % table)
        indexes = [i['name'][tmp_label_len:] for i in get_indexes(table)]
        pk = get_pk_constraint(table)
        if pk['name']:
            indexes.append(pk['name'][tmp_label_len:])

        table = table[tmp_label_len:]
        sql = '''
        DROP TABLE IF EXISTS "{table}";
        ALTER TABLE "{TEMP_TABLE_STR}{table}" RENAME TO "{table}";
        '''
        sql_list.append(sql.format(table=table, TEMP_TABLE_STR=temp_table_str))
        for index in indexes:
            if verbose:
                print('\tSwap index %s' % index)
            sql = 'ALTER INDEX "{TEMP_TABLE_STR}{index}" RENAME TO "{index}";'
            sql = sql.format(index=index, TEMP_TABLE_STR=temp_table_str)
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
        sql = 'ALTER SEQUENCE "{TEMP_TABLE_STR}{name}" RENAME TO "{name}";'
        sql_list.append(sql.format(name=name, TEMP_TABLE_STR=temp_table_str))

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


def create_table(table, fields, verbose=0):
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
        sql_fields.append(col)
    # Primary Key
    pk = []
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


def build_summary(data, verbose=False, limit=None):
    if data.get('disabled'):
        return
    table_name = data['name']
    sql = data['sql']
    tables = data['tables']

    table_name = config.TEMP_TABLE_STR + table_name
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
            create_table(table_name, fields, verbose=verbose)
            f = [field['name'] for field in fields if not field.get('missing')]
            insert_sql = insert_rows(table_name, fields)
            first = False
        data.append(dict(zip(f, row)))
        count += 1
        if count % config.BATCH_SIZE == 0:
            run_sql(insert_sql, data)
            data = []
            if verbose:
                print(count)
        if limit and count == limit:
            break
    if data:
        run_sql(insert_sql, data)

    if verbose:
        print('%s rows imported' % (count))
    if count:
        # Add indexes
        build_indexes(table_name, fields, verbose=verbose)


def build_summaries(data, verbose=False):
    for info in data:
        build_summary(info, verbose=verbose)


def build_view(data, verbose=0):
    view_name = data['name']
    sql = data['sql']
    tables = data['tables']
    view_name = config.TEMP_TABLE_STR + view_name
    drop_sql = 'DROP VIEW IF EXISTS "%s"' % view_name
    run_sql(drop_sql)
    if verbose:
        print('creating view %s' % view_name)
    tables_dict = make_tables_dict(tables)
    tables_dict['name'] = quote(view_name)
    if verbose > 1:
        print(sql.format(**tables_dict))
    run_sql(sql.format(**tables_dict))


def build_views(data, verbose=0):
    for info in data:
        build_view(info, verbose=verbose)


def swap_table(old_name, new_name):
    sql = '''
        BEGIN;
        DROP TABLE IF EXISTS "{new_name}";
        ALTER TABLE "{old_name}" RENAME TO "{new_name}";
        COMMIT;
    '''
    sql = sql.format(old_name=old_name, new_name=new_name)
    conn.execute(sql)
