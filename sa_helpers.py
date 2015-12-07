import csv

import sqlalchemy as sa
from sqlalchemy.orm import Session
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

engine = sa.create_engine(
    'postgres://munge:pass@localhost/munge', echo=False)
conn = engine.connect()
sess = Session(engine)

def make_bool(value):
    if not value:
        return False
    return str(value).lower() in ['yes', 'true', 'y', 'f', '1', '0']


def make_int(value):
    try:
        return int(value)
    except ValueError:
        return None


FIELD_TYPE = {
    int: sa.BigInteger,
    float: sa.Float,
    make_bool: sa.Boolean,
    make_int: sa.Integer,
}

def auto_table(name, fields, translate=None, primary_key=None):

    def make_column(field):
        if translate and field in translate:
            field_type = FIELD_TYPE[translate[field]]
        else:
            field_type = sa.Text
        pk = bool(field == primary_key)
        return field, field_type, pk

    cols = []
    if not primary_key:
        primary_key = fields[0]
    if primary_key not in fields:
        cols.append((primary_key, sa.BigInteger, True))
    for field in fields:
        cols.append(make_column(field))

    class Cls(Base):

        __table__ = sa.Table(
            name, Base.metadata,
            *[sa.Column(f, t(), primary_key=p) for (f,t,p) in cols]
        )
    #Base.metadata.create_all(engine)
    try:
        Base.metadata.tables[name].create(bind=engine)
    except sa.exc.ProgrammingError:
        pass
    return Cls


def auto_csv_table(table_name, file_name, transform=None, primary_key=None, delimiter=','):
    with open(file_name, 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=delimiter)
        header_fields = map(str.strip, reader.next())
    return auto_table(
        table_name,
        header_fields,
        transform,
        primary_key
    )
