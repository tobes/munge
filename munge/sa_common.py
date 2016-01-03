import sqlalchemy as sa
from sqlalchemy.engine import reflection

OID_TYPE = {
    20: 'bigint',
    21: 'smallint',
    23: 'integer',
    25: 'text',
    16: 'boolean',
    701: 'double precision',
    1082: 'date',
    1700: 'numeric',
}


def run_sql(engine, sql, *args, **kw):
    sql = sa.sql.text(sql)
    return engine.execute(sql, *args, **kw)


def get_sequence_names(engine):
    sql = "SELECT c.relname FROM pg_class c WHERE c.relkind = 'S';"
    result = engine.execute(sql)
    return [row[0] for row in result]


def table_list(engine):
    sql = '''
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
        AND table_type='BASE TABLE';
    '''
    result = engine.execute(sql)
    return [row[0] for row in result]


def view_list(engine):
    sql = "SELECT c.relname FROM pg_class c WHERE c.relkind = 'v';"
    result = engine.execute(sql)
    return [row[0] for row in result]


def table_view_list(engine):
    sql = '''
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
        AND table_type='BASE TABLE'
        UNION
        SELECT c.relname
        FROM pg_class c
        WHERE c.relkind = 'v' AND relowner != 10;
    '''
    result = engine.execute(sql)
    return [row[0] for row in result]


def get_pk_constraint(engine, table_name):
    insp = reflection.Inspector.from_engine(engine)
    return insp.get_pk_constraint(table_name)


def get_primary_keys(engine, table_name):
    insp = reflection.Inspector.from_engine(engine)
    return insp.get_primary_keys(table_name)


def get_indexes(engine, table_name):
    insp = reflection.Inspector.from_engine(engine)
    return insp.get_indexes(table_name)


def get_result_fields(engine, result, table=None):
    types = [OID_TYPE.get(col[1], col[1]) for col in result.cursor.description]
    if table:
        pks = get_primary_keys(engine, table)
        indexes = get_indexes(engine, table)
    else:
        pks = []
        indexes = []
    fields = []
    for i, v in enumerate(result.keys()):
        col = {
            'name': v,
            'type': types[i],
            'pk': v in pks,
            'indexed': v in indexes,
        }
        fields.append(col)
    return fields
