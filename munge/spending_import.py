import os.path

import config
from csv_util import unicode_csv_reader, import_csv
from sa_util import swap_tables, summary, build_view


f = 'spending/spending_by_nuts1.csv'

fields = [
    'ct_code',
    'nuts1_code',
    'factor:double precision',
]

summary_data = [
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
    },
]

views_data = [
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


def build_summaries(verbose=False):
    for info in summary_data:
        summary(
            config.TEMP_TABLE_STR + info['name'],
            info['sql'],
            info['tables'],
            verbose=verbose
        )


def build_views(verbose=False):
    for info in views_data:
        build_view(
            info['name'],
            info['sql'],
            info['tables'],
            verbose=verbose
        )


def hei1_reader(filename):
    f = os.path.join(config.DATA_PATH, filename)
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


def import_spending(verbose=False):
    if verbose:
        print('importing spending')
    reader = hei1_reader(f)
    import_csv(
        reader,
        'nuts1_ct_spending_factor',
        fields=fields,
        verbose=verbose
    )
    build_views(verbose=verbose)
    build_summaries(verbose=verbose)
    swap_tables(verbose=verbose)
