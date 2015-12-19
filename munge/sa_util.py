import sqlalchemy as sa
from sqlalchemy.engine import reflection

import config


engine = sa.create_engine(config.CONNECTION_STRING, echo=False)
conn = engine.connect()


OID_TYPE = {
    20: 'bigint',
    21: 'smallint',
    25: 'text',
    16: 'boolean',
    701: 'double precision',
}


def run_sql(sql, *args, **kw):
    sql = sa.sql.text(sql)
    return conn.execute(sql, *args, **kw)


def get_primary_keys(table_name):
    insp = reflection.Inspector.from_engine(engine)
    return insp.get_primary_keys(table_name)


def get_indexes(table_name):
    insp = reflection.Inspector.from_engine(engine)
    return insp.get_indexes(table_name)


def table_list():
    insp = reflection.Inspector.from_engine(engine)
    return insp.get_table_names()


def swap_table(old_name, new_name):
    sql = '''
        BEGIN;
        DROP TABLE IF EXISTS "{new_name}";
        ALTER TABLE "{old_name}" RENAME TO "{new_name}";
        COMMIT;
    '''
    sql = sql.format(old_name=old_name, new_name=new_name)
    conn.execute(sql)


def get_result_fields(result, table):
    types = [OID_TYPE.get(col[1], col[1]) for col in result.cursor.description]
    pk = get_primary_keys(table)
    index = get_indexes(table)
    fields = []
    for i, v in enumerate(result.keys()):
        col = {'name': v, 'type': types[i], 'pk': v in pk}
        fields.append(col)
    return fields
