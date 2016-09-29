import zipfile
import os

import sqlalchemy as sa

import config
import sa_common
from csv_util import make_csv

engine = sa.create_engine(config.CONNECTION_USER, echo=False)
conn = engine.connect()


def run_sql(*args, **kw):
    return sa_common.run_sql(engine, *args, **kw)


def make_zip(file_path, files):
    zf = zipfile.ZipFile(file_path, mode='w')
    for f in files:
        arcname = os.path.basename(f)
        zf.write(f, compress_type=zipfile.ZIP_DEFLATED, arcname=arcname)
        os.remove(f)
    zf.close()


TEMP_PATH = '/tmp'

data = [

    {
        # c_ct
        'file_name': 'consumer_trend_codes.csv',
        'sql': 'select * from c_ct',
    },
    {
        # scat_group_codes
        'file_name': 'scat_group_codes.csv',
        'sql': 'select code, "desc" from c_scat_group',
    },
    {
        # premises
        'file_name': 'premises.csv',
        'sql': 'select * from v_download_premises_data where la_code IN :la_codes',
    },
#    {
#        # market size
#        'file_name': 'market_size.csv',
#        'sql': 'select * from s_la_spending_by_ct where la_code IN :la_codes',
#    },
]


def zoopla_downloads():
    sql = '''
        SELECT id, la_codes FROM "group"
        WHERE 'download' = ANY(permissions)
    '''

    results = run_sql(sql)
    for result in results:
        vao_downloads(dict(result))


def vao_downloads(result):
    directory = os.path.join(config.DOWNLOAD_DIR, str(result['id']))
    try:
        os.makedirs(directory)
    except OSError:
        pass

    files = []
    for item in data:
        file_name = item['file_name']
        sql = item['sql']

        path = os.path.join(TEMP_PATH, file_name)

        make_csv(
            path,
            sql,
            data={'la_codes': tuple(result['la_codes'])},
            simple=True,
            verbose=0
        )

        files.append(path)

    make_zip(os.path.join(TEMP_PATH, 'premises.zip'), files)
    shutil.move(
        os.path.join(TEMP_PATH, 'premises.zip'),
        os.path.join(directory, 'premises.zip')
    )

zoopla_downloads()
