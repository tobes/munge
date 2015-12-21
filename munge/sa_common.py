import sqlalchemy as sa
from sqlalchemy.engine import reflection

OID_TYPE = {
    20: 'bigint',
    21: 'smallint',
    25: 'text',
    16: 'boolean',
    701: 'double precision',
}


def run_sql(engine, sql, *args, **kw):
    sql = sa.sql.text(sql)
    return engine.execute(sql, *args, **kw)


def table_list(engine):
    insp = reflection.Inspector.from_engine(engine)
    return insp.get_table_names()


def get_pk_constraint(engine, table_name):
    insp = reflection.Inspector.from_engine(engine)
    return insp.get_pk_constraint(table_name)


def get_primary_keys(engine, table_name):
    insp = reflection.Inspector.from_engine(engine)
    return insp.get_primary_keys(table_name)


def get_indexes(engine, table_name):
    insp = reflection.Inspector.from_engine(engine)
    return insp.get_indexes(table_name)


def get_result_fields(result, table):
    types = [OID_TYPE.get(col[1], col[1]) for col in result.cursor.description]
    pks = get_primary_keys(table)
    indexes = get_indexes(table)
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
