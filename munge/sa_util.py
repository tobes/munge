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


def table_list(*args, **kw):
    return sa_common.table_list(engine, *args, **kw)


def get_result_fields(*args, **kw):
    return sa_common.get_result_fields(engine, *args, **kw)


def get_sequence_names():
    sql = 'SELECT c.relname FROM pg_class c WHERE c.relkind = \'S\';'
    results = conn.execute(sql)
    return [row[0] for row in results]


def clear_temp_objects(verbose=False):
    # tables
    tables = [t for t in table_list() if t.startswith(config.TEMP_TABLE_STR)]
    for table in tables:
        if verbose:
            print('Dropping table %s' % table)
        sql = 'DROP TABLE "{table}";'.format(table=table)
        conn.execute(sql)
    # sequences
    sequence_names = [
        s for s in get_sequence_names()
        if s.startswith(config.TEMP_TABLE_STR)
    ]
    for name in sequence_names:
        if verbose:
            print('Dropping sequence %s' % name)
        sql = 'DROP SEQUENCE "{name}";'.format(name=name)


def swap_tables(verbose=False):
    ''' SWAPS our temp tables, including renaming indexes and sequences
    '''
    temp_table_str = config.TEMP_TABLE_STR
    tmp_label_len = len(temp_table_str)
    tables = [t for t in table_list() if t.startswith(temp_table_str)]
    sql_list = []
    sql_list.append('BEGIN;')
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
    conn.execute('\n'.join(sql_list))


def make_tables_dict(tables):
    all_tables = table_list()
    output = {}
    for i, table in enumerate(tables):
        if config.TEMP_TABLE_STR + table in all_tables:
            name = config.TEMP_TABLE_STR + table
        else:
            name = table
        output['t%s' % (i + 1)] = name
    return output


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


def build_indexes(table_name, t_fields, verbose=False):
    index_fields = [f['name'] for f in t_fields if f.get('index')]
    sql_list = []
    for field in index_fields:
        if verbose:
            print('creating index of %s' % field)
        sql = 'CREATE INDEX "{idx_name}" ON "{table}" ("{field}");'
        sql = sql.format(
            idx_name='%s_idx_%s' % (table_name, field),
            table=table_name,
            field=field,
        )
        sql_list.append(sql)
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


def summary(table_name, sql, tables, verbose=False, limit=None):
    if verbose:
        print('creating summary table %s' % table_name)
    tables_dict = make_tables_dict(tables)
    result = run_sql(sql.format(**tables_dict))
    first = True
    count = 0
    data = []
    for row in result:
        if first:
            fields = get_result_fields(result)
            create_table(table_name, fields)
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
    # Add indexes
    build_indexes(table_name, fields, verbose=verbose)


def swap_table(old_name, new_name):
    sql = '''
        BEGIN;
        DROP TABLE IF EXISTS "{new_name}";
        ALTER TABLE "{old_name}" RENAME TO "{new_name}";
        COMMIT;
    '''
    sql = sql.format(old_name=old_name, new_name=new_name)
    conn.execute(sql)
