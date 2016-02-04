import csv
import os.path

from munge import config
from munge.csv_util import import_csv


DIRECTORY = 'pc'
IMPORTER = 'postcode'
FILENAME = 'ONSPD_MAY_2015_UK.csv'
TABLE_NAME = 'postcode'


def tables():
    return [TABLE_NAME]


def importer(verbose=0):
    filename = os.path.join(config.DATA_PATH, DIRECTORY, FILENAME)

    fields = [
        '-pcd',
        '-pcd2',
        '*pc',
        '+@pcc:text~compact_pc|pc',
        '+@outcode:text~outcode|pc',
        '+@areacode:text~areacode|pc',
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
        '-nuts',
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
        '+lsoa_code',
        '+msoa_code',
        '-parish',
        '-wz11',
        '-ccg',
        '-bua11',
        '-buasd11',
        '-ru11ind',
        '-oac11',
        '@nuts_sub_code:text~la_sub_2_la|nuts',
        '@nuts1_code:text~la_sub_2_nuts1|nuts_sub_code',
        '@nuts2_code:text~la_sub_2_nuts2|nuts_sub_code',
        '@nuts3_code:text~la_sub_2_nuts3|nuts_sub_code',
        'lat:double precision',
        'long:double precision',
    ]

    if verbose:
        print('Importing postcodes')
    with open(filename, 'rb') as f:
        reader = csv.reader(f, dialect=csv.excel)
        import_csv(reader,
                   TABLE_NAME,
                   fields=fields,
                   importer=IMPORTER,
                   description='Postcode data',
                   skip_first=True,
                   verbose=verbose)
