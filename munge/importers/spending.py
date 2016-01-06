import os.path

from munge import config
from munge.csv_util import import_csv, unicode_csv_reader
from munge.sa_util import build_summaries, build_views


DIRECTORY = 'spending'

HEI1_FILE = 'spending_by_nuts1.csv'

TABLE_NAME = 'nuts1_ct_spending_factor'

TABLE_FIELDS = [
    'ct_code',
    'nuts1_code',
    'factor:double precision',
]

SUMMARIES_DATA = [
    {
        'name': 's_population_by_nuts1',
        'sql': '''
             SELECT
             l.nuts1_code, p.population,
              p.population::float / t.population::float AS percent
             FROM "{t1}" p
             RIGHT JOIN "{t2}" l ON l.nuts1_ons_code = la_code
             LEFT outer JOIN "{t1}" t ON t.la_code = 'K02000001';
        ''',
        'tables': ['population_by_la', 'l_nuts1_ons'],
    },

    {
        'name': 's_consumer_spend_national',
        'sql': '''
        SELECT
        ct.ct_code,
        ct.amount nation_spend,
        (ct.amount / t.population)::numeric(11,2)
            AS spend_per_capita
        FROM "{t1}" ct
        LEFT outer JOIN "{t2}" t ON t.la_code = 'K02000001'
        WHERE ct.amount is not null
        ''',
        'tables': [
            'v_consumer_trend_latest',
            'population_by_la',
        ],
        'disabled': False,
    },

    {
        'name': 's_consumer_spend_by_nuts1',
        'sql': '''
        SELECT
        f.nuts1_code,
        ct.ct_code,
        ct.amount nation_spend,
        ct.amount * p.percent area_spend,
        (ct.amount * p.percent * f.factor)::numeric(15,2)
            AS adj_area_spend,
        (ct.amount * p.percent * f.factor / p.population)::numeric(11,2)
            AS adj_spend_per_capita,
        (100 * (((ct.amount * p.percent * f.factor / p.population)
        / n.spend_per_capita) - 1))::numeric(11,2)
            AS percent_from_national
        FROM "{t1}" ct
        LEFT OUTER JOIN "{t2}" f ON f.ct_code = ct.ct_code
        LEFT OUTER JOIN "{t3}" p ON p.nuts1_code = f.nuts1_code
        LEFT OUTER JOIN "{t4}" n ON n.ct_code = ct.ct_code
        WHERE ct.amount is not null
        ''',
        'tables': [
            'v_consumer_trend_latest',
            'nuts1_ct_spending_factor',
            's_population_by_nuts1',
            's_consumer_spend_national',
        ],
        'disabled': False,
    },
]

VIEWS_DATA = [
    {
        'name': 'v_consumer_trend_latest',
        'sql': '''
        CREATE VIEW "{name}" AS
        SELECT ct_code, amount * 1000000 as amount
        FROM "{t1}"
        WHERE date = '2014';
        ''',
        'tables': ['consumer_trend_yearly'],
    },
]


def hei1_reader():
    f = os.path.join(config.DATA_PATH, DIRECTORY, HEI1_FILE)
    reader = unicode_csv_reader(f)
    first = True
    for row in reader:
        if first:
            nuts = row[1:]
            first = False
            continue
        ct_code = row[0]
        for i, nut in enumerate(nuts):
            out = [ct_code, nut, row[i + 1]]
            yield out


def importer(verbose=0):
    if verbose:
        print('importing spending')
    reader = hei1_reader()
    import_csv(
        reader,
        TABLE_NAME,
        fields=TABLE_FIELDS,
        verbose=verbose
    )
