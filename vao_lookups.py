import os.path

from csv_tools import csv_2_table
from sa_helpers import auto_csv_table, make_bool, make_int, conn

DATA_PATH = '/home/toby/whythawk/munge/data/'

def auto_csv(info):
    data_file = os.path.join(DATA_PATH, info['data_file'])
    csv_2_table(
        data_file,
        transform=info.get('transform'),
        sa_obj=auto_csv_table(
            info['table'],
            data_file,
            transform=info.get('transform'),
            primary_key=info.get('primary_key')
        ),
        conn=conn,
    )


if True:
    auto_csv({
        'table': 'scat_codes',
        'data_file': 'vao/voa_scat_codes.csv',
        'transform': {
            'scat_code': int,
            'scat_group': int,
            'local_market': make_bool,
        },
    })


if True:
    auto_csv({
        'table': 'scat_groups',
        'data_file': 'vao/voa_scat_groups.csv',
        'transform': {
            'sg_code': int,
            'employee_m2': make_int,
        },
    })


if True:
    auto_csv({
        'table': 'ct_mapping',
        'data_file': 'vao/voa_consumer_trends.csv',
        'transform': {
            'ct_group': int,
        },
        'primary_key': 'ct_code',
    })


if True:
    auto_csv({
        'table': 'cgroups',
        'data_file': 'vao/voa_consumer_groups.csv',
        'transform': {
            'cg_code': int,
        },
    })


