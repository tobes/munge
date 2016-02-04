import sqlalchemy as sa
from sqlalchemy.engine import reflection

OID_TYPE = {
    20: 'bigint',
    21: 'smallint',
    23: 'integer',
    25: 'text',
    16: 'boolean',
   # 701: 'double precision',
    701: 'float',
    1082: 'date',
    1700: 'numeric',
    1114: 'timestamp',
    1009: 'text[]',
}

NUMERIC_TYPES = [
    'bigint',
    'smallint',
    'integer',
    'double precision',
    'float',
    'numeric',
]

FLOAT_TYPES = [
    'double precision',
    'numeric',
    'float',
]

TRANSLATIONS = {
    'double precision': 'float'
}



def field_type(type):
    return TRANSLATIONS.get(type, type)


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


def dependent_objects(engine):
    ''' Returns list of objects that are depended on in the database '''
    sql = '''
        SELECT distinct dependent.relname
        FROM pg_depend
        JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
        JOIN pg_class as dependee ON pg_rewrite.ev_class = dependee.oid
        JOIN pg_class as dependent ON pg_depend.refobjid = dependent.oid
        JOIN pg_attribute ON pg_depend.refobjid = pg_attribute.attrelid
            AND pg_depend.refobjsubid = pg_attribute.attnum
        WHERE dependent.relowner != 10
        AND pg_attribute.attnum > 0
    '''
    result = engine.execute(sql)
    return [row[0] for row in result]


def table_columns(engine, table_name):
    sql = '''
        SELECT column_name,data_type
        FROM information_schema.columns
        WHERE table_name = :table_name
    '''
    result = run_sql(engine, sql, table_name=table_name)
    pks = get_primary_keys(engine, table_name)
    indexes = get_indexes(engine, table_name)
    fields = []
    for row in result:
        name = row[0]
        fields.append({
            'name': name,
            'type': field_type(row[1]),
            'pk': name in pks,
            'indexed': name in indexes,
        })
    return fields


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


def fields_match(f1, f2):
    set1 = set([
        '%s:%s:%s:%s' % (f['name'], field_type(f['type']), f['pk'], f['indexed'])
        for f in f1
    ])
    set2 = set([
        '%s:%s:%s:%s' % (f['name'], field_type(f['type']), f['pk'], f['indexed'])
        for f in f2
    ])
    return set1 == set2
