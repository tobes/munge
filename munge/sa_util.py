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


def clear_temp_objects():
    tables = [t for t in table_list() if t.startswith('#')]
    for table in tables:
        sql = 'DROP TABLE "{table}";'.format(table=table)
        conn.execute(sql)


def swap_tables(verbose=False):
    ''' SWAPS our temp tables, including renaming indexes and sequences
    '''
    tables = [t for t in table_list() if t.startswith('#')]
    sql_list = []
    sql_list.append('BEGIN;')
    for table in tables:
        if verbose:
            print 'Swap table %s' % table
        indexes = [i['name'][1:] for i in get_indexes(table)]
        pk = get_pk_constraint(table)
        if pk['name']:
            indexes.append(pk['name'][1:])

        table = table[1:]
        sql = '''
        DROP TABLE IF EXISTS "{table}";
        ALTER TABLE "#{table}" RENAME TO "{table}";
        '''
        sql_list.append(sql.format(table=table))
        for index in indexes:
            if verbose:
                print '\tSwap index %s' % index
            sql = 'ALTER INDEX "#{index}" RENAME TO "{index}";'
            sql_list.append(sql.format(index=index))

    # sequences
    for name in [s[1:] for s in get_sequence_names() if s.startswith('#')]:
        if verbose:
            print '\tSwap sequence %s' % name
        sql = 'ALTER SEQUENCE "#{name}" RENAME TO "{name}";'
        sql_list.append(sql.format(name=name))

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