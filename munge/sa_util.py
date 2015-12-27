import sqlalchemy as sa
from sqlalchemy.engine import reflection

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


def swap_table(old_name, new_name):
    sql = '''
        BEGIN;
        DROP TABLE IF EXISTS "{new_name}";
        ALTER TABLE "{old_name}" RENAME TO "{new_name}";
        COMMIT;
    '''
    sql = sql.format(old_name=old_name, new_name=new_name)
    conn.execute(sql)
