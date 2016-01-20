import os.path
from decimal import Decimal

from munge import config
from munge.csv_util import import_csv, unicode_csv_reader
from munge.sa_util import results_dict, get_result_fields

DIRECTORY = 'vao'

VAO_LIST_TABLE = 'vao_list_raw'

VAO_LIST_FILE = 'LIST_2010_MERGED.dta.30Sep2015'
VAO_FILE = 'SMV_2010_MERGED.dta.30Sep2015'

vao_list_fields = [
    '*@id:bigserial',
    'inc_entry_no:bigint',
    '+ba_code',
    '+@la_code:text~ba_2_la|ba_code',
    'comm_code',
    'ba_ref',
    'prm_desc_code',
    'prm_desc',
    '+uarn:bigint',
    'fp_id',
    'firm_name',
    'add_no',
    'street',
    'town',
    'post_district',
    'county',
    'pc',
    '+@pcc:text~compact_pc|pc',
    '+@outcode:text~outcode|pc',
    '+@areacode:text~areacode|pc',
    'effective_date:date~make_date_DD_MON_YYYY',
    'composite',
    'rateable_value:bigint',
    'settlement_type',
    'ass_ref',
    'alter_date:date~make_date_DD_MON_YYYY',
    '+scat_code:smallint~make_scat',
    'sub_street_level_3',
    'sub_street_level_2',
    'sub_street_level_1',
    '-extra_column_in_data',
]

vao_base_fields = [
    '*@id:bigserial',
    '-record_type',
    'ass_ref',
    '+{1}uarn:bigint',
    '+{1}version:smallint',
    '+ba_code',
    '+@la_code:text~ba_2_la|ba_code',
    'firm_name',
    'add_no',
    'add_3',
    'add_2',
    'add_1',
    'street',
    'post_dist',
    'town',
    'county',
    'pc',
    '+@pcc:text~compact_pc|pc',
    '+@outcode:text~outcode|pc',
    '+@areacode:text~areacode|pc',
    'scheme_ref:bigint',
    'desc',
    'total_area:numeric',
    'subtotal:bigint',
    'total_value:bigint',
    'adopted_rv:bigint',
    'list_year:int',
    '-ba',
    'ba_ref',
    'vo_ref',
    'from_date:date~make_date_DD_MON_YYYY',
    'to_date:date~make_date_DD_MON_YYYY',
    '+scat_code:smallint',
    'measure_unit',
    'unadjusted_price:numeric',
]


vao_base_02_fields = [
    '*@id:bigserial',
    '+{1}uarn:bigint',
    '+{1}version:smallint',
    '-record_type',
    'line:smallint',
    'floor',
    'description',
    'area:numeric~make_numeric_na',
    'price:numeric',
    'value:bigint',
]

vao_base_03_fields = [
    '*@id:bigserial',
    '+{1}uarn:bigint',
    '+{1}version:smallint',
    '-record_type',
    'description',
    'oa_size:numeric',
    'oa_price:numeric',
    'oa_value:bigint',
]

vao_base_04_fields = [
    '*@id:bigserial',
    '+{1}uarn:bigint',
    '+{1}version:smallint',
    '-record_type',
    'plant_value:bigint',
]

vao_base_05_fields = [
    '*@id:bigserial',
    '+{1}uarn:bigint',
    '+{1}version:smallint',
    '-record_type',
    'spaces:bigint',
    'spaces_value:bigint',
    'area:bigint',
    'area_value:bigint',
    'total:bigint',
]

vao_base_06_fields = [
    '*@id:bigserial',
    '+{1}uarn:bigint',
    '+{1}version:smallint',
    '-record_type',
    'adj_desc',
    'adj_percent',

]

vao_base_07_fields = [
    '*@id:bigserial',
    '+{1}uarn:bigint',
    '+{1}version:smallint',
    '-record_type',
    'total_before_adj:bigint',
    'total_adj:bigint',
]

vao_types = [
    ('01', 'vao_base_raw', vao_base_fields),
    ('02', 'vao_line_raw', vao_base_02_fields),
    ('03', 'vao_additions_raw', vao_base_03_fields),
    ('04', 'vao_plant_raw', vao_base_04_fields),
    ('05', 'vao_parking_raw', vao_base_05_fields),
    ('06', 'vao_adj_raw', vao_base_06_fields),
    ('07', 'vao_adj_totals_raw', vao_base_07_fields),
]


cache = {}

def make_key(keys, data, key_delimiter='#'):
    out = []
    for key in keys:
        out.append(str(data[key]))
    return key_delimiter.join(out)


AREA_ESTIMATERS = [
    {
        'name': 'areacode',
        'cutoff': 10,
        'value': 'median_price_per_m2',
        'sql': '''
            SELECT areacode, scat_code, median_price_per_m2, count
            FROM s_vao_area_areacode_by_scat
        ''',
        'keys': ['scat_code', 'areacode'],
        'code': 1
    },
    {
        'name': 'outcode',
        'cutoff': 10,
        'value': 'median_price_per_m2',
        'sql': '''
            SELECT outcode, scat_code, median_price_per_m2, count
            FROM s_vao_area_outcode_by_scat
        ''',
        'keys': ['scat_code', 'outcode'],
        'code': 2
    },
    {
        'name': 'la',
        'cutoff': 10,
        'value': 'median_price_per_m2',
        'sql': '''
            SELECT la_code, scat_code, median_price_per_m2, count
            FROM s_vao_area_la_by_scat
        ''',
        'keys': ['scat_code', 'la_code'],
        'code': 3
    },
    {
        'name': 'nuts3',
        'cutoff': 10,
        'value': 'median_price_per_m2',
        'sql': '''
            SELECT nuts3_code, scat_code, median_price_per_m2, count
            FROM s_vao_area_nuts3_by_scat
        ''',
        'keys': ['scat_code', 'nuts3_code'],
        'code': 4
    },
    {
        'name': 'nuts2',
        'cutoff': 10,
        'value': 'median_price_per_m2',
        'sql': '''
            SELECT nuts2_code, scat_code, median_price_per_m2, count
            FROM s_vao_area_nuts2_by_scat
        ''',
        'keys': ['scat_code', 'nuts2_code'],
        'code': 5
    },
    {
        'name': 'nuts1',
        'cutoff': 10,
        'value': 'median_price_per_m2',
        'sql': '''
            SELECT nuts1_code, scat_code, median_price_per_m2, count
            FROM s_vao_area_nuts1_by_scat
        ''',
        'keys': ['scat_code', 'nuts1_code'],
        'code': 6
    },
    {
        'name': 'national',
        'cutoff': 0,
        'value': 'median_price_per_m2',
        'sql': '''
            SELECT scat_code, median_price_per_m2, no_with_area as count
            FROM s_vao_area_national_by_scat
        ''',
        'keys': ['scat_code'],
        'code': 7
    },
    {
        'name': 'unpriced',
        'cutoff': 0,
        'value': 'median_m2',
        'sql': '''
            SELECT scat_code, median_m2, no_with_area as count
            FROM s_vao_area_national_by_scat
        ''',
        'keys': ['scat_code'],
        'no_calc': True,
        'code': 8
    },
]


def build_cache(name, sql, keys, value, cutoff=0, verbose=0):
    if verbose:
        print 'creating cache', name
    data = {}
    for row in results_dict(sql):
        if cutoff and row['count'] < cutoff:
            continue
        data[make_key(keys, row)] = row[value]
    cache[name] = data
    if verbose:
        print 'completed'


def s_vao_premises_area(data, verbose=0):
    if 'nuts1' not in cache:
        for item in AREA_ESTIMATERS:
            build_cache(
                item['name'],
                item['sql'],
                item['keys'],
                item['value'],
                item['cutoff'],
                verbose,
            )
    code = None
    out = {
        'uarn': data['uarn'],
        'scat_code': data['scat_code'],
    }
    area = data['total_area']
    if area:
        code = 0
    if not area:
        for item in AREA_ESTIMATERS:
            value = cache[item['name']].get(make_key(item['keys'], data))
            if value:
                if not item.get('no_calc'):
                    value = round(data['rateable_value'] / value, 2)
                if value:
                    code = item['code']
                    area = Decimal(str(value))
                    break
    out['area_source_code'] = code
    out['area'] = area
    return out


def s_vao_area(results, data, verbose=0):
    first = True
    count = 0
    store = {}
    for row in results:
        if first:
            f = [field['name'] for field in get_result_fields(results)]
        row_data = dict(zip(f, row))
        key = row_data['scat_code']
        value = row_data['total_value']
        area = row_data['total_area']
        if key not in store:
            store[key] = ([], [], 0, 0)
        if area:
            store[key][1].append(area)
        if area and value:
            store[key][0].append(value/area)
        count += 1
        store[key][3] += 1
        if area == 1:
            store[key][2] += 1
        if verbose and count % config.BATCH_SIZE == 0:
                print('processing {count:,}'.format(
                    count=count
                ))

    if verbose:
        print('calculating...')
    out = [data['fields']]
    for key in sorted(store.keys()):
        if verbose:
            print('{key}  {c1:,}  {c2:,}'.format(
                key=key, c1=len(store[key][0]), c2=len(store[key][1])
            ))
        out.append(
            [
                key,
                median(store[key][0]),
                median(store[key][1]),
                len(store[key][0]),
                len(store[key][1]),
                store[key][2],
                store[key][3],
            ]
        )

    return out


def median(lst):
    lst = sorted(lst)
    if len(lst) < 1:
            return None
    if len(lst) %2 == 1:
            return lst[((len(lst)+1)/2)-1]
    else:
            return float(sum(lst[(len(lst)/2)-1:(len(lst)/2)+1]))/2.0


AUTO_SQL = [
    # Only rows with a rateable value are valid
    {
        'name': 'vao_list',
        'sql': '''
        SELECT *
        FROM {t1}
        WHERE rateable_value is not null
        ''',
        'tables': ['vao_list_raw'],
        'as_view': True,
    },
    # create index of uran, version to be able to select active rows from
    # summary data
    {
        'name': 'vao_index',
        'sql': '''
             SELECT DISTINCT ON (b.uarn)
                 first_value(b.uarn) OVER wnd AS uarn,
                 first_value(b.version) OVER wnd AS version
             FROM {t1} b
             LEFT JOIN {t2} l ON l.uarn = b.uarn
             WINDOW wnd AS (
                 PARTITION BY b.uarn, b.version
                 ORDER BY b.from_date DESC NULLS LAST
                 ROWS BETWEEN UNBOUNDED PRECEDING
                 AND UNBOUNDED FOLLOWING
             )
        ''',
        'tables': ['vao_base_raw', 'vao_list'],
        'primary_key': ['uarn', 'version'],
        'disabled': False,
        'summary': '',
    },

    # Create the views of the active rows
    {
        'name': 'vao_base',
        'sql': '''
        SELECT b.*
        FROM {t1} b
        RIGHT JOIN {t2} i ON i.uarn = b.uarn AND i.version = b.version
        ''',
        'tables': ['vao_base_raw', 'vao_index'],
        'as_view': True,
    },
    {
        'name': 'vao_line',
        'sql': '''
        SELECT t.*
        FROM {t1} t
        RIGHT JOIN {t2} i ON i.uarn = t.uarn AND i.version = t.version
        ''',
        'tables': ['vao_line_raw', 'vao_index'],
        'as_view': True,
    },
    {
        'name': 'vao_parking',
        'sql': '''
        SELECT t.*
        FROM {t1} t
        RIGHT JOIN {t2} i ON i.uarn = t.uarn AND i.version = t.version
        ''',
        'tables': ['vao_parking_raw', 'vao_index'],
        'as_view': True,
    },
    {
        'name': 'vao_plant',
        'sql': '''
        SELECT t.*
        FROM {t1} t
        RIGHT JOIN {t2} i ON i.uarn = t.uarn AND i.version = t.version
        ''',
        'tables': ['vao_plant_raw', 'vao_index'],
        'as_view': True,
    },
    {
        'name': 'vao_additions',
        'sql': '''
        SELECT t.*
        FROM {t1} t
        RIGHT JOIN {t2} i ON i.uarn = t.uarn AND i.version = t.version
        ''',
        'tables': ['vao_additions_raw', 'vao_index'],
        'as_view': True,
    },
    {
        'name': 'vao_adj',
        'sql': '''
        SELECT t.*
        FROM {t1} t
        RIGHT JOIN {t2} i ON i.uarn = t.uarn AND i.version = t.version
        ''',
        'tables': ['vao_adj_raw', 'vao_index'],
        'as_view': True,
    },
    {
        'name': 'vao_adj_totals',
        'sql': '''
        SELECT t.*
        FROM {t1} t
        RIGHT JOIN {t2} i ON i.uarn = t.uarn AND i.version = t.version
        ''',
        'tables': ['vao_adj_totals_raw', 'vao_index'],
        'as_view': True,
    },
    # End of the active row views

    # How many list premises also have base data
    {
        'name': 's_vao_list_base_summary',
        'sql': '''
             SELECT
             count(t1.uarn) as list_entries,
             count(t2.uarn) as base_entries,
             count(t1.uarn) - count(t2.uarn) as difference,
             t1.scat_code as scat_code
             FROM {t1} t1
             LEFT OUTER JOIN {t2} t2 ON t1.uarn = t2.uarn
             LEFT OUTER JOIN {t3} t3 ON t3.code = t1.scat_code
             GROUP BY t3.desc, t1.scat_code
             ORDER BY t3.desc
        ''',
        'tables': ['vao_list', 'vao_base', 'c_scat'],
        'disabled': False,
        'summary': '',
    },

    {
        'name': 'vao_area_info',
        'sql': '''
        SELECT t.*, c.code, c.cg_code
        FROM {t1} t
        LEFT JOIN {t2} c ON c.code = t.scat_code
        WHERE c.cg_code != ''
        ''',
        'tables': ['s_vao_list_base_summary', 'c_scat'],
        'as_view': True,
    },


    # Median nationwide price per meter for scat code
    {
        'name': 's_vao_area_national_by_scat',
        'sql': '''
        SELECT code as scat_code,
        (
            WITH y AS (
               SELECT total_area, row_number() OVER (ORDER BY total_area) AS rn
               FROM   {t1}
               WHERE  total_area IS NOT NULL
               AND total_area != 0
               AND scat_code = code
               )
            , c AS (SELECT count(*) AS ct FROM y)
            SELECT CASE WHEN c.ct%2 = 0 THEN
                      round((SELECT avg(total_area) FROM y WHERE y.rn IN (c.ct/2, c.ct/2+1)), 3)
                   ELSE
                            (SELECT     total_area  FROM y WHERE y.rn = (c.ct+1)/2)
                   END AS median_m2
            FROM c
        ),
        (
            WITH y AS (
               SELECT rateable_value/total_area median_price_per_m2,
               row_number() OVER (ORDER BY rateable_value/total_area) AS rn
               FROM {t1} b
               LEFT JOIN {t2} l ON l.uarn = b.uarn
               WHERE  total_area IS NOT NULL
               AND total_area != 0
               AND b.scat_code = code
               )
            , c AS (SELECT count(*) AS ct FROM y)
            SELECT CASE WHEN c.ct%2 = 0 THEN
                      round((SELECT avg(median_price_per_m2) FROM y WHERE y.rn IN (c.ct/2, c.ct/2+1)), 3)
                   ELSE
                            (SELECT     median_price_per_m2  FROM y WHERE y.rn = (c.ct+1)/2)
                   END AS median_price_per_m2
            FROM c
        ),
        (
               SELECT count(*)
               FROM   {t1}
               WHERE  total_area IS NOT NULL
               AND total_area != 0
               AND scat_code = code
        ) no_with_area,
        (
               SELECT count(*)
               FROM   {t2} l
               LEFT OUTER JOIN {t1} b ON l.uarn = b.uarn
               WHERE  b.uarn is Null
               AND l.scat_code = code
        ) no_without_area
        FROM {t3}
        ''',
        'tables': ['vao_base', 'vao_list', 'c_scat'],
        'disabled': False,
        'summary': '',
    },

    {
        'name': 's_vao_area_nuts1_by_scat',
        'sql': '''
            SELECT nuts1_code, t1.scat_code, count(*),
            usr_median(total_value/total_area) as median_price_per_m2
            FROM {t1} t1
            LEFT JOIN {t2} p ON p.pc = t1.pc
            WHERE total_area > 0
            AND nuts1_code IS NOT NULL
            GROUP BY nuts1_code, t1.scat_code;
        ''',
        'tables': ['vao_base', 'postcode'],
        'disabled': False,
        'summary': '',
    },

    {
        'name': 's_vao_area_nuts2_by_scat',
        'sql': '''
            SELECT nuts2_code, t1.scat_code, count(*),
            usr_median(total_value/total_area) as median_price_per_m2
            FROM {t1} t1
            LEFT JOIN {t2} p ON p.pc = t1.pc
            WHERE total_area > 0
            AND nuts2_code IS NOT NULL
            GROUP BY nuts2_code, t1.scat_code;
        ''',
        'tables': ['vao_base', 'postcode'],
        'disabled': False,
        'summary': '',
    },

    {
        'name': 's_vao_area_nuts3_by_scat',
        'sql': '''
            SELECT nuts3_code, t1.scat_code, count(*),
            usr_median(total_value/total_area) as median_price_per_m2
            FROM {t1} t1
            LEFT JOIN {t2} p ON p.pc = t1.pc
            WHERE total_area > 0
            AND nuts3_code IS NOT NULL
            GROUP BY nuts3_code, t1.scat_code;
        ''',
        'tables': ['vao_base', 'postcode'],
        'disabled': False,
        'summary': '',
    },

    {
        'name': 's_vao_area_outcode_by_scat',
        'sql': '''
            SELECT t1.outcode, t1.scat_code, count(*),
            sum(total_area) as total_m2,
            sum(total_value) as total_value,
            usr_median(total_area) as median_m2,
            usr_median(total_value/total_area) as median_price_per_m2,
            t2.median_price_per_m2 as national_price_per_m2
            FROM {t1} t1
            LEFT OUTER JOIN {t2} t2 on t1.scat_code = t2.scat_code
            WHERE total_area > 0
            GROUP BY t1.outcode, t1.scat_code, national_price_per_m2
        ''',
        'tables': ['vao_base', 's_vao_area_national_by_scat'],
        'disabled': False,
        'summary': '',
    },

    {
        'name': 's_vao_area_areacode_by_scat',
        'sql': '''
            SELECT t1.areacode, t1.scat_code, count(*),
            sum(total_area) as total_m2,
            sum(total_value) as total_value,
            usr_median(total_area) as median_m2,
            usr_median(total_value/total_area) as median_price_per_m2,
            t2.median_price_per_m2 as national_price_per_m2
            FROM {t1} t1
            LEFT OUTER JOIN {t2} t2 on t1.scat_code = t2.scat_code
            WHERE total_area > 0
            GROUP BY t1.areacode, t1.scat_code, national_price_per_m2

        ''',
        'tables': ['vao_base', 's_vao_area_national_by_scat'],
        'disabled': False,
        'summary': '',
    },

    {
        'name': 's_vao_area_la_by_scat',
        'sql': '''
            SELECT la_code, t1.scat_code, count(*),
            sum(total_area) as total_m2,
            sum(total_value) as total_value,
            usr_median(total_area) as median_m2,
            usr_median(total_value/total_area) as median_price_per_m2,
            t2.median_price_per_m2 as national_price_per_m2
            FROM {t1} t1
            LEFT OUTER JOIN {t2} t2 on t1.scat_code = t2.scat_code
            WHERE total_area > 0
            GROUP BY la_code, t1.scat_code, national_price_per_m2

        ''',
        'tables': ['vao_base', 's_vao_area_national_by_scat'],
        'disabled': False,
        'summary': '',
    },

    {
        'name': 's_vao_premises_area_summary',
        'sql': '''
            SELECT t1.uarn, t1.scat_code,
            p.areacode, p.outcode,
            nuts1_code, nuts2_code, nuts3_code, t1.la_code,
            t1.rateable_value, t2.total_area, t2.total_value
            FROM {t1} t1
            LEFT JOIN {t2} t2 ON t1.uarn = t2.uarn
            LEFT JOIN {t3} p ON p.pc = t1.pc
        ''',
        'tables': ['vao_list', 'vao_base', 'postcode'],
        'disabled': False,
        'summary': '',
    },
    {
        'name': 's_vao_area2_national',
        'sql': '''
            SELECT  scat_code, total_area, total_value FROM {t1}
        ''',
        'tables': ['vao_base'],
        'fields': [
            'scat_code:smallint',
            'median_m2:numeric',
            'median_price_per_m2:numeric',
            'count_m2:integer',
            'count_ppm2:integer',
            'singles:integer',
            'total:integer',
         ],
        'table_function': s_vao_area,
        'disabled': False,
        'test': True,
        'summary': '',
    },
    {
        'name': 's_vao_premises_area',
        'sql': '''
            SELECT  * FROM {t1}
        ''',
        'tables': ['s_vao_premises_area_summary'],
        'fields': [
            '*uarn:bigint',
            'area:numeric',
            'scat_code:smallint',
            'area_source_code:smallint',
         ],
        'row_function': s_vao_premises_area,
        'disabled': False,
        'summary': '',
    },
    {
        'name': 'v_vao_scat_group_area_summary',
        'sql': '''
            select scat_group_code, a.*
            from {t1} c
            LEFT JOIN {t2} a ON a.scat_code = c.code
            ORDER BY scat_group_code;
        ''',
        'tables': ['c_scat', 's_vao_area_national_by_scat'],
        'as_view': True,
    },
    {
        'name': 's_vao_area_estimates',
        'sql': '''
            SELECT
            l.uarn,

            CASE WHEN total_area IS NOT NULL THEN
                    0
                 WHEN a.count >= 10 AND a.median_price_per_m2 > 0 THEN
                    1
                 WHEN o.count >= 10 AND o.median_price_per_m2 > 0 THEN
                    2
                 WHEN la.count >= 10 AND la.median_price_per_m2 > 0 THEN
                    3
                 WHEN n3.count >= 10 AND n3.median_price_per_m2 > 0 THEN
                    4
                 WHEN n2.count >= 10 AND n2.median_price_per_m2 > 0 THEN
                    5
                 WHEN n1.count >= 10 AND n1.median_price_per_m2 > 0 THEN
                    6
                 WHEN sc.no_with_area > 0 AND sc.median_price_per_m2 > 0 THEN
                    7
                 WHEN sc.no_with_area > 0 AND sc.median_m2 IS NOT NULL THEN
                    8
                 ELSE null
            END area_source_code,

            CASE WHEN total_area IS NOT NULL THEN
                    total_area
                 WHEN a.count >= 10 AND a.median_price_per_m2 > 0 THEN
                    l.rateable_value/a.median_price_per_m2
                 WHEN o.count >= 10 AND o.median_price_per_m2 > 0 THEN
                    l.rateable_value/o.median_price_per_m2
                 WHEN la.count >= 10 AND la.median_price_per_m2 > 0 THEN
                    l.rateable_value/la.median_price_per_m2
                 when n3.count >= 10 and n3.median_price_per_m2 > 0 then
                    l.rateable_value/n3.median_price_per_m2
                 WHEN n2.count >= 10 AND n2.median_price_per_m2 > 0 THEN
                    l.rateable_value/n2.median_price_per_m2
                 WHEN n1.count >= 10 AND n1.median_price_per_m2 > 0 THEN
                    l.rateable_value/n1.median_price_per_m2
                 WHEN sc.no_with_area > 0 AND sc.median_price_per_m2 > 0 THEN
                    l.rateable_value/sc.median_price_per_m2
                 WHEN sc.no_with_area > 0 AND sc.median_m2 IS NOT NULL THEN
                    sc.median_m2
                 ELSE null
            END area_estimate

            FROM {t1} l

            LEFT JOIN {t2} a
            On a.areacode = l.areacode AND a.scat_code = l.scat_code

            LEFT JOIN {t3} o
            On o.outcode = l.outcode AND o.scat_code = l.scat_code

            LEFT JOIN {t4} la
            On la.la_code = l.la_code AND la.scat_code = l.scat_code

            LEFT JOIN {t5} sc On sc.scat_code = l.scat_code

            LEFT JOIN {t6} n3 ON n3.nuts3_code = l.nuts3_code

            LEFT JOIN {t7} n2 ON n2.nuts2_code = l.nuts2_code

            LEFT JOIN {t8} n1 ON n1.nuts1_code = l.nuts1_code

            WHERE l.rateable_value != 0
        ''',
        'tables': [
            's_vao_premises_area_summary', 's_vao_area_areacode_by_scat',
            's_vao_area_outcode_by_scat', 's_vao_area_la_by_scat',
            's_vao_area_national_by_scat', 's_vao_area_nuts3_by_scat',
            's_vao_area_nuts2_by_scat', 's_vao_area_nuts1_by_scat',
        ],
        'disabled': True,
        'summary': '',
    },





# ==================================



    {
        'name': 's_vao_base_areas_scat_group',
        'sql': '''
            SELECT v.la_code, s.scat_group_code, count(v.*),
            sum(v.total_area) as total_m2,
            sum(v.total_value) as total_value,
            avg(v.total_area) as mean_m2,
            usr_median(v.total_area) as median_m2,
            usr_mode(v.total_area) as mode_m2,
            sum(v.total_area * v.unadjusted_price) as total_area_price,
            (sum(v.total_area * v.unadjusted_price) - sum(v.total_value)) as diff
            FROM {t1} v
            LEFT JOIN {t2} s ON s.code = v.scat_code
            GROUP BY v.la_code, s.scat_group_code
        ''',
        'tables': ['vao_base', 'c_scat'],
        'disabled': True,
        'summary': '',
    },
    {
        'name': 's_vao_scat_group_median_areas',
        'sql': '''
        SELECT sg.code as scat_group_code,
        (
            WITH y AS (
               SELECT total_area, row_number() OVER (ORDER BY total_area) AS rn
               FROM   {t1} v
               LEFT JOIN {t2} s ON s.code = v.scat_code
               WHERE  total_area IS NOT NULL
               AND s.scat_group_code = sg.code
               )
            , c AS (SELECT count(*) AS ct FROM y)
            SELECT CASE WHEN c.ct%2 = 0 THEN
                      round((SELECT avg(total_area) FROM y WHERE y.rn IN (c.ct/2, c.ct/2+1)), 3)
                   ELSE
                            (SELECT     total_area  FROM y WHERE y.rn = (c.ct+1)/2)
                   END AS median_m2
            FROM c
        ), (
               SELECT count(*)
               FROM   {t1} v
               LEFT JOIN {t2} s ON s.code = v.scat_code
               WHERE  total_area IS NOT NULL
               AND s.scat_group_code = sg.code

        ) count
        FROM {t3} sg
        ''',
        'tables': ['vao_base', 'c_scat', 'c_scat_group'],
        'disabled': True,
        'summary': '',
    },

    {
        'name': 's_vao_base_areas_min_max',
        'sql': '''
            SELECT scat_code,
            max(median_m2) as max_med_m2,
            min(median_m2) as min_med_m2
            FROM {t1}
            GROUP BY scat_code
        ''',
        'tables': ['s_vao_base_areas'],
        'disabled': True,
        'summary': '',
    },
    {
        'name': 's_vao_base_areas_national',
        'sql': '''
            SELECT v.scat_code, count(v.*),
            sum(v.total_area) as total_m2,
            sum(v.total_value) as total_value,
            avg(v.total_area) as mean_m2,
            m.median_m2,
            la.min_med_m2,
            la.max_med_m2
            FROM {t1} v
            LEFT OUTER JOIN {t2} m On v.scat_code = m.scat_code
            LEFT OUTER JOIN {t3} la On v.scat_code = la.scat_code
            GROUP BY v.scat_code, m.median_m2, la.min_med_m2, la.max_med_m2
        ''',
        'tables': ['vao_base', 's_vao_scat_median_areas', 's_vao_base_areas_min_max'],
        'disabled': True,
        'summary': '',
    },
    {
        'name': 's_vao_base_missing_list',
        'sql': '''
            SELECT
            t2.uarn, t2.scat_code, t2.ba_code
            FROM {t1} t1
            RIGHT OUTER JOIN {t2} t2 ON t1.uarn = t2.uarn
            WHERE t1.uarn is null
        ''',
        'tables': ['vao_list', 'vao_base'],
        'disabled': True,
        'summary': '',
    },
]


def tables():
    t = [t[1] for t in vao_types]
    t.append(VAO_LIST_TABLE)
    return t


def vao_reader(data_type):
    f = os.path.join(config.DATA_PATH, DIRECTORY, VAO_FILE)
    reader = unicode_csv_reader(f, encoding='latin-1', delimiter='*')
    uarn = None
    versions = {}
    for row in reader:
        r_type = row[0]
        if r_type == '01':
            uarn = row[2]
            versions[uarn] = versions.get(uarn, 0) + 1
            base_version = versions[uarn]
        if r_type == data_type:
            if r_type == '01':
                row = row[:3] + [base_version] + row[3:]
            else:
                row = [uarn, base_version] + row
            yield row


def import_vao_summary(verbose=False):
    for rec_type, table, fields in vao_types:
        if verbose:
            print('importing %s' % table)
        reader = vao_reader(rec_type)
        import_csv(reader, table, fields=fields, verbose=verbose)


def import_vao_list(verbose=False):
    if verbose:
        print('importing vao_list')
    f = os.path.join(config.DATA_PATH, DIRECTORY, VAO_LIST_FILE)
    reader = unicode_csv_reader(f, encoding='latin-1', delimiter='*')
    import_csv(reader, VAO_LIST_TABLE, fields=vao_list_fields, verbose=verbose)


def importer(verbose=False):
    import_vao_list(verbose=verbose)
    import_vao_summary(verbose=verbose)
