import sqlalchemy as sa
from sqlalchemy.orm import Session
from sqlalchemy.ext.declarative import declarative_base

from csv_tools import csv_2_table, csv_2_list
Base = declarative_base()

class Postcodes(Base):
    __table__ = sa.Table(
        'pc', Base.metadata,
        sa.Column('pcds', sa.Text, primary_key=True),
        sa.Column('lat', sa.Float()),
        sa.Column('long', sa.Float()),
        sa.Column('nuts', sa.Text()),
        sa.Column('gor', sa.Text()),
        sa.Column('oslaua', sa.Text()),
    )

engine = sa.create_engine(
    'postgres://munge:pass@localhost/munge', echo=False)
conn = engine.connect()
sess = Session(engine)

Base.metadata.create_all(engine)

directory = '/home/toby/Downloads/pc/Data'


data_file_pc = '/home/toby/Downloads/pc/Data/ONSPD_MAY_2015_UK.csv'
data_file_nuts = '/home/toby/Downloads/pc/Documents/LAU215_LAU115_NUTS315_NUTS215_NUTS115_UK_LU.txt'


def make_lookup(file_name, key, value, delimiter=None):
    data = csv_2_list(
        file_name,
        fields=[key, value],
        delimiter=delimiter
    )
    out = {}
    for row in data:
        k = row[key]
        v = row[value]
        out[k] = v
    return out


nuts_lookup = make_lookup(
    data_file_nuts,
    delimiter='\t',
    key='LAU215CD',
    value='NUTS315CD'
)


csv_2_table(
        data_file_pc,
        sa_obj=Postcodes,
        fields=['pcds', 'long', 'lat', 'nuts', 'oslaua', 'gor'],
        translate={'nuts': nuts_lookup},
        transform={'long': float, 'lat': float}
)
