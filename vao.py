import csv

import sqlalchemy as sa
from sqlalchemy.orm import Session
from sqlalchemy.ext.declarative import declarative_base

from csv_tools import csv_2_table, csv_2_list, csv_2_print
from sa_helpers import Base, engine, auto_table, make_bool, make_int, conn

##class VOA_base(Base):
##    __table__ = sa.Table(
##        'voa_base', Base.metadata,
##        sa.Column('Unique_Address_Reference_Number', sa.BigInteger),
##        sa.Column('Billing_Authority_Code', sa.Text()),
##        sa.Column('Firms_Name', sa.Text()),
##        sa.Column('Number_Name', sa.Text()),
##        sa.Column('Street', sa.Text()),
##        sa.Column('Town', sa.Text()),
##        sa.Column('Postcode', sa.Text()),
##        sa.Column('Total_Area', sa.Float()),
##        sa.Column('Total_Value', sa.Float()),
##        sa.Column('SCat_Code', sa.Integer()),
##        sa.Column('Unadjusted_Price', sa.Float()),
##    )

class VOA_regions(Base):
    __table__ = sa.Table(
        'voa_regions', Base.metadata,
        sa.Column('voa_ba', sa.Text(), primary_key=True),
        sa.Column('name', sa.Text()),
        sa.Column('nuts3', sa.Text()),
    )


header_fields['01'] = [
    "rec_type",
    "ass_ref",
    "uarn",
    "ba_code",
    "firm_name",
    "add_no",
    "add_3",
    "add_2",
    "add_1",
    "street",
    "post_dist",
    "town",
    "county",
    "pc",
    "scheme_ref",
    "desc",
    "total_area",
    "subtotal",
    "total_value",
    "adopted_rv",
    "list_year",
    "ba",
    "ba_ref",
    "vo_ref",
    "from_date",
    "to_date",
    "scat_code",
    "measure_unit",
    "unadjusted_price",
]

fields = header_fields

translations['01'] = {
    "unadjusted_price": float,
    "total_area": float,
    "total_value": float,
    "uarn": int,
    "scat_code": int,
    'list_year': int,
}

VOA_base = auto_table('vao_base', fields['01'], translations['01'], primary_key='id')

def create_row(fields, header_fields, transform, row, count):
    out = {}
    if len(header_fields) != len(row):
        print row
        print len(row)
        print len(header_fields)
        print count
        return None
    for field in fields:
        value = row[header_fields.index(field)]
        #if translate and field in translate:
        #    value = translate[field].get(value)
        if transform and field in transform:
            try:
                value = transform[field](value)
            except:
                print row
                print len(row)
                print len(header_fields)
                print count
                raise
        out[field.replace(' ', '_')] = value
    return out


def csv_2_gen(file_name):
    with open(file_name, 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter='*')

        out = {}
        count = 0
        for row in reader:
            count += 1
            record_type = row[0]
            if record_type == '01':
                out = create_row(fields['01'],
                                 header_fields['01'],
                                 translations['01'],
                                 row, count)
                yield out

def voa_2_print(data_file):
    count = 0
    for row in csv_2_gen(data_file):
        print row
        count += 1
        if count == 100:
            break
        if count % 10000 == 0:
            print count

def csv_2_ba(data_file):
    ba = {}
    count = 0
    for row in csv_2_gen(data_file):
        if row is None:
            continue
        count += 1
        ba[row['Billing Authority Code']] = row['Billing Authority']
        if count % 10000 == 0:
            print count
    for b in ba:
        print "%s, '%s'" % (b, ba[b])

def voa_2_table(data_file, sa_obj):
    count = 0
    table = sa_obj.__table__
    rows = []
    conn.execute('TRUNCATE TABLE %s' % table)
    for row in csv_2_gen(data_file):
        if row is None:
            continue
        count += 1
        rows.append(row)
        if count % 10000 == 0:
            conn.execute(table.insert(), rows)
            rows = []
            print count
    if rows:
        conn.execute(table.insert(), rows)


voa_2_table(data_file, VOA_base)
#voa_2_print(data_file)

if False:
    csv_2_table(
            data_file_lookup,
            sa_obj=VOA_regions,
            conn=conn,
            fields = ['nuts3', 'voa_ba', 'name'],
            trim = ['nuts3', 'voa_ba', 'name']
    )



