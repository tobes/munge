import csv
import os.path

import config
from csv_util import import_csv
from sa_util import swap_tables


filename = 'pc/ONSPD_MAY_2015_UK.csv'


def import_postcodes(verbose=False):
    global filename
    filename = os.path.join(config.DATA_PATH, filename)

    table_name = 'postcode'

    fields = [
        '*pcd',
        '+@pcc:text~compact_pc|pcd',
        '-pcd2',
        '-pcds',
        '-dointr',
        '-doterm',
        '-oscty',
        'la_code',
        '-osward',
        '-usertype',
        '-oseast1m',
        '-osnrth1m',
        '-osgrdind',
        '-oshlthau',
        '-hro',
        '-ctry',
        '-gor',
        '-streg',
        '-pcon',
        '-eer',
        '-teclec',
        '-ttwa',
        '-pct',
        'la_sub_code',
        '-psed',
        '-cened',
        '-edind',
        '-oshaprev',
        '-lea',
        '-oldha',
        '-wardc91',
        '-wardo91',
        '-ward98',
        '-statsward',
        '-oa01',
        '-casward',
        '-park',
        '-lsoa01',
        '-msoa01',
        '-ur01ind',
        '-oac01',
        '-oldpct',
        '-oa11',
        '-lsoa11',
        '-msoa11',
        '-parish',
        '-wz11',
        '-ccg',
        '-bua11',
        '-buasd11',
        '-ru11ind',
        '-oac11',
        'lat:double precision',
        'long:double precision',
        '@nuts_sub_code:text~la_sub_2_la|la_sub_code',
    ]

    with open(filename, 'rb') as f:
        reader = csv.reader(f, dialect=csv.excel)

        import_csv(reader, table_name, fields=fields,
                   skip_first=True, verbose=verbose)
        swap_tables()
