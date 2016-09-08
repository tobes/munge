import os.path
from decimal import Decimal

from munge import config
from munge.csv_util import import_csv, unicode_csv_reader
from munge.sa_util import results_dict, get_result_fields

DIRECTORY = 'vao'
IMPORTER = 'vao'

VAO_LIST_TABLE = 'vao_list_raw'

VAO_LIST_FILE = 'LIST_2010_MERGED.dta.30Sep2015'
VAO_FILE = 'SMV_2010_MERGED.dta.30Sep2015'

vao_list_fields = [
    '*@id:bigserial',
    'inc_entry_no:bigint',
    '+ba_code',
    '+@la_code:text~ba_2_la|ba_code',
    'comm_code',
    '+ba_ref',
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
            SELECT scat_code, median_price_per_m2, count
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
            SELECT scat_code, median_m2, count
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
            store[key] = [[], [], 0, 0]
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
        'summary': 'Premises with rateable values',
    },
    # create index of uran, version to be able to select active rows from
    # summary data
    {
        'name': 'vao_index',
        'sql': '''
             SELECT DISTINCT ON (b.uarn)
                 first_value(b.uarn) OVER wnd AS uarn,
                 first_value(b.from_date) OVER wnd AS date,
                 last_value(b.from_date) OVER wnd AS date2,
                 first_value(b.version) OVER wnd AS version
             FROM {t1} b
             INNER JOIN {t2} l ON l.uarn = b.uarn
             WINDOW wnd AS (
                 PARTITION BY b.uarn, b.version
                 ORDER BY b.version  NULLS LAST
                 ROWS BETWEEN UNBOUNDED PRECEDING
                 AND UNBOUNDED FOLLOWING
             )
        ''',
        'tables': ['vao_base_raw', 'vao_list'],
        'primary_key': ['uarn', 'version'],
        'disabled': True,
        'summary': 'Used to select which summay data to use for a premesis',
    },

    # Create the views of the active rows
    {
        'name': 'vao_base',
        'sql': '''
        SELECT b.*
        FROM {t1} b
        WHERE version = 1
        ''',
        'tables': ['vao_base_raw'],
        'as_view': True,
    },
    {
        'name': 'vao_line',
        'sql': '''
        SELECT t.*
        FROM {t1} t
        WHERE version = 1
        ''',
        'tables': ['vao_line_raw'],
        'as_view': True,
    },
    {
        'name': 'vao_parking',
        'sql': '''
        SELECT t.*
        FROM {t1} t
        WHERE version = 1
        ''',
        'tables': ['vao_parking_raw'],
        'as_view': True,
    },
    {
        'name': 'vao_plant',
        'sql': '''
        SELECT t.*
        FROM {t1} t
        WHERE version = 1
        ''',
        'tables': ['vao_plant_raw'],
        'as_view': True,
    },
    {
        'name': 'vao_additions',
        'sql': '''
        SELECT t.*
        FROM {t1} t
        WHERE version = 1
        ''',
        'tables': ['vao_additions_raw'],
        'as_view': True,
    },
    {
        'name': 'vao_adj',
        'sql': '''
        SELECT t.*
        FROM {t1} t
        WHERE version = 1
        ''',
        'tables': ['vao_adj_raw'],
        'as_view': True,
    },
    {
        'name': 'vao_adj_totals',
        'sql': '''
        SELECT t.*
        FROM {t1} t
        WHERE version = 1
        ''',
        'tables': ['vao_adj_totals_raw'],
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
        'stage': 2,
        'summary': '',
    },

    {
        'name': 'vao_area_info',
        'sql': '''
        SELECT t.*, c.code, c.cg_code
        FROM {t1} t
        LEFT JOIN {t2} c ON c.code = t.scat_code
        WHERE c.cg_code is not null
        ''',
        'tables': ['s_vao_list_base_summary', 'c_scat'],
        'as_view': True,
        'stage': 2,
    },


    # Median nationwide price per meter for scat code
    {
        'name': 's_vao_area_national_by_scat',
        'sql': '''
            SELECT t1.scat_code,
            (SELECT count(*) FROM {t1} c WHERE c.scat_code = t1.scat_code) total,
            count(*),
            (SELECT count(*) FROM {t1} c WHERE c.scat_code = t1.scat_code
            AND total_area <= 1) count_small,
            quantile(total_value/total_area, 0.5) as median_price_per_m2,
            min(total_value/total_area) as min_price_per_m2,
            max(total_value/total_area) as max_price_per_m2,
            quantile(total_area, 0.5) as median_m2,
            min(total_area) as min_m2,
            max(total_area) as max_m2
            FROM {t1} t1
            WHERE total_area > 0
            GROUP BY t1.scat_code
            ORDER BY scat_code
        ''',
        'tables': ['vao_base'],
        'disabled': False,
        'summary': '',
        'stage': 2,
    },

    {
        'name': 's_vao_area_nuts1_by_scat',
        'sql': '''
            SELECT nuts1_code, t1.scat_code, count(*),
            sum(total_area) as total_m2,
            sum(total_value) as total_value,
            quantile(total_value/total_area, 0.5) as median_price_per_m2,
            quantile(total_area, 0.5) as median_m2
            FROM {t1} t1
            LEFT JOIN {t2} p ON p.pc = t1.pc
            WHERE total_area > 0
            AND nuts1_code IS NOT NULL
            GROUP BY nuts1_code, t1.scat_code
            ORDER BY scat_code, nuts1_code
        ''',
        'tables': ['vao_base', 'postcode'],
        'disabled': False,
        'summary': '',
        'stage': 2,
    },

    {
        'name': 's_vao_area_nuts2_by_scat',
        'sql': '''
            SELECT nuts2_code, t1.scat_code, count(*),
            sum(total_area) as total_m2,
            sum(total_value) as total_value,
            quantile(total_value/total_area, 0.5) as median_price_per_m2,
            quantile(total_area, 0.5) as median_m2
            FROM {t1} t1
            LEFT JOIN {t2} p ON p.pc = t1.pc
            WHERE total_area > 0
            AND nuts2_code IS NOT NULL
            GROUP BY nuts2_code, t1.scat_code
            ORDER BY scat_code, nuts2_code
        ''',
        'tables': ['vao_base', 'postcode'],
        'disabled': False,
        'summary': '',
        'stage': 2,
    },

    {
        'name': 's_vao_area_nuts3_by_scat',
        'sql': '''
            SELECT nuts3_code, t1.scat_code, count(*),
            sum(total_area) as total_m2,
            sum(total_value) as total_value,
            quantile(total_value/total_area, 0.5) as median_price_per_m2,
            quantile(total_area, 0.5) as median_m2
            FROM {t1} t1
            LEFT JOIN {t2} p ON p.pc = t1.pc
            WHERE total_area > 0
            AND nuts3_code IS NOT NULL
            GROUP BY nuts3_code, t1.scat_code
            ORDER BY scat_code, nuts3_code
        ''',
        'tables': ['vao_base', 'postcode'],
        'disabled': False,
        'summary': '',
        'stage': 2,
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
        'stage': 2,
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
        'stage': 2,
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
        'stage': 2,
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
        'stage': 2,
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
        'disabled': True,
        'summary': '',
        'stage': 2,
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
        'dependencies': [
            's_vao_area_areacode_by_scat',
            's_vao_area_outcode_by_scat',
            's_vao_area_la_by_scat',
            's_vao_area_nuts3_by_scat',
            's_vao_area_nuts2_by_scat',
            's_vao_area_nuts1_by_scat',
            's_vao_area_national_by_scat',
            's_vao_area_national_by_scat',
        ],
        'disabled': False,
        'summary': '',
        'stage': 2,
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
        'summary': '',
        'stage': 2,
    },

    {
        'name': 'v_premises_summary',
        'sql': '''
            SELECT
            l.uarn,
            initcap(l.add_no) address_number,
            initcap(l.street) street,
            initcap(l.town) town,
            initcap(l.post_district) postal_district,
            initcap(l.county) county,
            l.pc postcode,
            l.scat_code,
            l.la_code,
            initcap(l.prm_desc) description,
            a.area,
            a.area_source_code,
            s.local_market,
            s.scat_group_code,
            safe_divide(a.area, employee_m2) employees,
            lsoa_code,
            msoa_code
            FROM {t1} l
            LEFT JOIN {t2} a ON a.uarn=l.uarn
            LEFT JOIN {t3} s ON s.code=l.scat_code
            LEFT JOIN {t5} sg ON s.scat_group_code=sg.code
            LEFT OUTER JOIN {t4} p ON p.pc = l.pc
        ''',
        'tables': ['vao_list', 's_vao_premises_area', 'c_scat', 'postcode', 'c_scat_group'],
        'as_view': True,
        'summary': '',
        'stage': 3,
    },

    {
        'name': 'v_premises_summary2',
        'sql': '''
            SELECT
            l.uarn,
            l.rateable_value,
            l.pc postcode,
            l.street,
            l.scat_code,
            l.la_code,
            l.fp_id,
            p.long,
            p.lat,
            a.area,
            a.area_source_code,
            s.local_market,
            s.scat_group_code,
            s.cg_code as ct_group_code,
            safe_divide(a.area, employee_m2) employees,
            CASE
                WHEN area > 0 AND employee_m2 > 0
                THEN round(a.area/employee_m2) * w.wage
                ELSE null
            END AS    employee_cost,
            CASE
                WHEN area > 0 AND employee_m2 > 0
                THEN (l.rateable_value + (round(a.area/employee_m2) * w.wage)) / 0.4
                ELSE l.rateable_value / 0.4
            END AS break_even,
            w.wage wage_employee,
            lsoa_code,
            msoa_code,
            vac.tenant,
            vac.prop_empty empty
            FROM {t1} l
            LEFT JOIN {t2} a ON a.uarn=l.uarn
            LEFT JOIN {t3} s ON s.code=l.scat_code
            LEFT JOIN {t5} sg ON s.scat_group_code=sg.code
            LEFT JOIN {t6} w ON l.la_code = w.la_code
            LEFT JOIN {t7} vac ON vac.uarn = l.uarn
            LEFT OUTER JOIN {t4} p ON p.pc = l.pc
        ''',
        'tables': ['vao_list', 's_vao_premises_area', 'c_scat', 'postcode', 'c_scat_group', 'v_wages', 'vacancy_info'],
        'as_view': True,
        'summary': '',
        'stage': 3,
    },

    {
        'name': 's_vao_no_lsoa',
        'sql': '''
            SELECT scat_code, count(*)
            FROM {t1}
            WHERE lsoa_code is null
            GROUP BY scat_code
            ORDER BY count(*) desc;
        ''',
        'tables': ['v_premises_summary'],
        'summary': '',
        'stage': 3,
    },

    {
        'name': 's_vao_no_area',
        'sql': '''
            SELECT scat_code, count(*)
            FROM {t1}
            WHERE lsoa_code is null
            GROUP BY scat_code
            ORDER BY count(*) desc;
        ''',
        'tables': ['v_premises_summary'],
        'summary': '',
        'stage': 3,
    },



    {
        'name': 's_la_scat_code_areas',
        'sql': '''
            SELECT scat_code, la_code, sum(area) as area
            FROM {t1}
            GROUP BY scat_code, la_code
        ''',
        'tables': ['v_premises_summary2'],
        'summary': '',
        'stage': 4,
    },



    {
        'name': 's_la_ct_group_code_areas',
        'sql': '''
            SELECT ct_group_code, la_code, sum(area) as area
            FROM {t1}
            GROUP BY ct_group_code, la_code
            ORDER BY la_code, ct_group_code
        ''',
        'tables': ['v_premises_summary2'],
        'summary': '',
        'stage': 4,
    },



    {
        'name': 's_la_spending_by_ct',
        'sql': '''
            SELECT la.nuts1_code, la.la_code, p.population,
            adj_spend_per_capita, s.ct_code, ct.ct_group as ct_group_code,
            p.population * adj_spend_per_capita total_adj_spend,
            p.population * adj_spend_per_capita / a.area spend_per_m2,
a.area
            FROM {t1} la
            LEFT JOIN {t2} p ON p.la_code = la.la_code
            LEFT JOIN {t3} s ON s.nuts1_code = la.nuts1_code
            LEFT JOIN {t4} ct ON ct.ct_code = s.ct_code
            LEFT JOIN {t5}  a On a.ct_group_code = ct.ct_group
AND a.la_code = la.la_code
            WHERE a.area > 0
        ''',
        'tables': ['l_la_nuts', 'population_by_la',
                   's_consumer_spend_by_nuts1', 'ct_mapping',
                   's_la_ct_group_code_areas'],
        'summary': '',
        'stage': 5,
    },


    {
        'name': 's_estimated_income',
        'sql': '''
            SELECT uarn,
            s.ct_code, spend_per_m2, p.area,
            break_even, employee_cost, employees,
            spend_per_m2 * p.area as est_revenue,
            (spend_per_m2 * p.area) - break_even as est_profit,
            (spend_per_m2 * p.area) / break_even as rating
            FROM {t1} p
            LEFT JOIN {t2} s ON s.ct_group_code = p.ct_group_code
            AND s.la_code = p.la_code
            LEFT JOIN {t3} ct ON ct.code = s.ct_code
            WHERE ct_code IS NOT NULL
            AND break_even > 0
            AND p.area > 0
            and ct.ct_level < 2
            ORDER BY uarn
        ''',
        'tables': ['v_premises_summary2', 's_la_spending_by_ct', 'c_ct'],
        'summary': '',
        'stage': 5,
    },


    {
        'name': 's_premesis_rating',
        'sql': '''
             SELECT uarn, max(rating)
             FROM {t1} GROUP BY uarn
        ''',
        'tables': ['s_estimated_income'],
        'summary': '',
        'stage': 5,
    },

    {
        'name': 's_la_general_summary',
        'sql': '''
            SELECT p.la_code, scat_code,
            count(p.uarn) count,
            count(nullif(v.prop_empty = false, true)) vacant_count,
            wage_employee average_wage,
            sum(area) total_area,
            usr_median(area) median_area,
            min(area) min_area,
            max(area) max_area,
            usr_median(rateable_value) median_rateable_value,
            min(rateable_value) min_rateable_value,
            max(rateable_value) max_rateable_value,
            usr_median(safe_divide(rateable_value, area)) median_rate_per_area,
            min(safe_divide(rateable_value, area)) min_rate_per_area,
            max(safe_divide(rateable_value, area)) max_rate_per_area,
            sum(break_even) total_break_even,
            sum(employees) estimated_employees,
             sum(employees) * wage_employee as estimated_employee_earnings,
             sum(rateable_value) as total_rateable_value
            FROM {t1} p
            JOIN {t2} v ON p.uarn = v.uarn
            GROUP BY p.la_code, scat_code, wage_employee

        ''',
        'tables': ['v_premises_summary2', 'vacancy_info'],
        'summary': '',
        'test': True,
        'stage': 5,
    },

    {
        'name': 's_la_general_summary_total',
        'sql': '''
            SELECT p.la_code, scat_group_code,
            count(p.uarn) count,
            count(nullif(v.prop_empty = false, true)) vacant_count,
            wage_employee average_wage,
            sum(area) total_area,
            usr_median(area) median_area,
            min(area) min_area,
            max(area) max_area,
            usr_median(rateable_value) median_rateable_value,
            min(rateable_value) min_rateable_value,
            max(rateable_value) max_rateable_value,
            usr_median(safe_divide(rateable_value, area)) median_rate_per_area,
            min(safe_divide(rateable_value, area)) min_rate_per_area,
            max(safe_divide(rateable_value, area)) max_rate_per_area,
            sum(break_even) total_break_even,
            sum(employees) estimated_employees,
             sum(employees) * wage_employee as estimated_employee_earnings,
             sum(rateable_value) as total_rateable_value
            FROM {t1} p
            JOIN {t2} v ON p.uarn = v.uarn
            GROUP BY p.la_code, scat_group_code, wage_employee

        ''',
        'tables': ['v_premises_summary2', 'vacancy_info'],
        'summary': '',
        'test': True,
        'stage': 5,
    },

    {
        'name': 's_lsoa_general_summary',
        'sql': '''
            SELECT p.lsoa_code, scat_code,
            count(p.uarn) count,
            count(nullif(v.prop_empty = false, true)) vacant_count,
            wage_employee average_wage,
            sum(area) total_area,
            usr_median(area) median_area,
            min(area) min_area,
            max(area) max_area,
            usr_median(rateable_value) median_rateable_value,
            min(rateable_value) min_rateable_value,
            max(rateable_value) max_rateable_value,
            usr_median(safe_divide(rateable_value, area)) median_rate_per_area,
            min(safe_divide(rateable_value, area)) min_rate_per_area,
            max(safe_divide(rateable_value, area)) max_rate_per_area,
            sum(break_even) total_break_even,
            sum(employees) estimated_employees,
             sum(employees) * wage_employee as estimated_employee_earnings,
             sum(rateable_value) as total_rateable_value
            FROM {t1} p
            JOIN {t2} v ON p.uarn = v.uarn
            GROUP BY p.lsoa_code, scat_code, wage_employee

        ''',
        'tables': ['v_premises_summary2', 'vacancy_info'],
        'summary': '',
        'test': True,
        'stage': 5,
    },

    {
        'name': 's_msoa_general_summary',
        'sql': '''
            SELECT p.msoa_code, scat_code,
            count(p.uarn) count,
            count(nullif(v.prop_empty = false, true)) vacant_count,
            wage_employee average_wage,
            sum(area) total_area,
            usr_median(area) median_area,
            min(area) min_area,
            max(area) max_area,
            usr_median(rateable_value) median_rateable_value,
            min(rateable_value) min_rateable_value,
            max(rateable_value) max_rateable_value,
            usr_median(safe_divide(rateable_value, area)) median_rate_per_area,
            min(safe_divide(rateable_value, area)) min_rate_per_area,
            max(safe_divide(rateable_value, area)) max_rate_per_area,
            sum(break_even) total_break_even,
            sum(employees) estimated_employees,
             sum(employees) * wage_employee as estimated_employee_earnings,
             sum(rateable_value) as total_rateable_value
            FROM {t1} p
            JOIN {t2} v ON p.uarn = v.uarn
            GROUP BY p.msoa_code, scat_code, wage_employee

        ''',
        'tables': ['v_premises_summary2', 'vacancy_info'],
        'summary': '',
        'test': True,
        'stage': 5,
    },



    {
        'name': 's_la_median_scat_ratable_breakeven',
        'sql': '''
             SELECT scat_code,
             usr_median(total_rateable_value) median_total_rateable_value,
             usr_median(total_break_even) median_total_break_even
             FROM {t1} GROUP BY scat_code
        ''',
        'tables': ['s_la_general_summary'],
        'summary': '',
        'stage': 5,
    },

    {
        'name': 's_msoa_median_scat_ratable_breakeven',
        'sql': '''
             SELECT scat_code,
             usr_median(total_rateable_value) median_total_rateable_value,
             usr_median(total_break_even) median_total_break_even
             FROM {t1} GROUP BY scat_code
        ''',
        'tables': ['s_msoa_general_summary'],
        'summary': '',
        'stage': 5,
    },


    {
        'name': 's_lsoa_median_scat_ratable_breakeven',
        'sql': '''
             SELECT scat_code,
             usr_median(total_rateable_value) median_total_rateable_value,
             usr_median(total_break_even) median_total_break_even
             FROM {t1} GROUP BY scat_code
        ''',
        'tables': ['s_lsoa_general_summary'],
        'summary': '',
        'stage': 5,
    },


    {
        'name': 'v_la_general_summary',
        'sql': '''
    SELECT la_code, s.scat_code, count, total_area,
    percent_diff(vacant_count, count) empty_percent,
    estimated_employees, estimated_employee_earnings,
    total_rateable_value, m.median_total_rateable_value,
    percent_diff( m.median_total_rateable_value, total_rateable_value)
        ratable_variance,
    total_break_even,
    percent_diff( m.median_total_break_even, total_break_even)
        break_even_variance,
    m.median_total_rateable_value/total_area median_rate_m2,
    min_area,
    max_area,
    median_rate_per_area,
    min_rate_per_area,
    max_rate_per_area

    FROM {t1} s
    LEFT JOIN {t2} m on m.scat_code = s.scat_code
        ''',
        'tables': ['s_la_general_summary', 's_la_median_scat_ratable_breakeven'],
        'summary': '',
        'as_view': True,
        'stage': 5,
    },

    {
        'name': 'v_msoa_general_summary',
        'sql': '''
    SELECT msoa_code, s.scat_code, count, total_area,
    percent_diff(vacant_count, count) empty_percent,
    estimated_employees, estimated_employee_earnings,
    total_rateable_value, m.median_total_rateable_value,
    percent_diff( m.median_total_rateable_value, total_rateable_value)
        ratable_variance,
    total_break_even,
    percent_diff( m.median_total_break_even, total_break_even)
        break_even_variance,
    m.median_total_rateable_value/total_area median_rate_m2,
    min_area,
    max_area,
    median_rate_per_area,
    min_rate_per_area,
    max_rate_per_area

    FROM {t1} s
    LEFT JOIN {t2} m on m.scat_code = s.scat_code
        ''',
        'tables': ['s_msoa_general_summary', 's_msoa_median_scat_ratable_breakeven'],
        'summary': '',
        'as_view': True,
        'stage': 5,
    },

    {
        'name': 'v_lsoa_general_summary',
        'sql': '''
    SELECT lsoa_code, s.scat_code, count, total_area,
    percent_diff(vacant_count, count) empty_percent,
    estimated_employees, estimated_employee_earnings,
    total_rateable_value, m.median_total_rateable_value,
    percent_diff( m.median_total_rateable_value, total_rateable_value)
        ratable_variance,
    total_break_even,
    percent_diff( m.median_total_break_even, total_break_even)
        break_even_variance,
    m.median_total_rateable_value/total_area median_rate_m2,
    min_area,
    max_area,
    median_rate_per_area,
    min_rate_per_area,
    max_rate_per_area

    FROM {t1} s
    LEFT JOIN {t2} m on m.scat_code = s.scat_code
        ''',
        'tables': ['s_lsoa_general_summary', 's_lsoa_median_scat_ratable_breakeven'],
        'summary': '',
        'as_view': True,
        'stage': 5,
    },

    {
        'name': 's_nuts1_spending_by_ct_group',
        'sql': '''
            SELECT nuts1_code, ct_group_code,
            sum(adj_spend_per_capita) spend_per_capita
            FROM {t1} s
            JOIN {t2} c ON c.code = s.ct_code
            JOIN {t3} cg ON cg.ct_code = c.code
            WHERE base = true
            AND nuts1_code IS NOT NULL
            GROUP BY nuts1_code, ct_group_code;
        ''',
        'tables': ['s_consumer_spend_by_nuts1', 'c_ct', 'l_ct_cg'],
        'summary': '',
        'stage': 6,
    },

    {
        'name': 's_la_area_rental_per_ct_group',
        'sql': '''
            SELECT la_code, ct_group_code,
            sum(area) total_area,
            sum(rateable_value) total_rateable_value,
            sum(break_even) total_breakeven
            FROM {t1}
            GROUP BY la_code, ct_group_code;
        ''',
        'tables': ['v_premises_summary2'],
        'summary': '',
        'stage': 6,
    },


    {
        'name': 'v_la_ct_sum_projections',
        'sql': '''
    SELECT c.ct_code,
    sp.ct_group_code,
    s.la_code,
    sp.area * adj_spend_per_capita/spend_per_capita area_m2,
    sp.spend_per_m2,
    (s.total_rateable_value /sp.area) * (adj_spend_per_capita/spend_per_capita) rateable_value_per_m2,
    (s.total_breakeven /sp.area) * (adj_spend_per_capita/spend_per_capita) projected_spend_per_m2,
(s.total_rateable_value /sp.area) * 0.15 as rent_per_m2,
   sp.spend_per_m2 * 0.15 as projected_rent_per_m2,
    safe_divide(((sp.area * adj_spend_per_capita/spend_per_capita ) * (s.total_rateable_value /sp.area) * (adj_spend_per_capita/spend_per_capita)), sp.spend_per_m2 * 0.15) projected_area_m2
    FROM {t1} c
    LEFT JOIN {t2} s
    ON c.ct_group = s.ct_group_code
    LEFT JOIN {t3} sp
    ON sp.ct_code = c.ct_code AND sp.la_code = s.la_code
    JOIN {t4} t ON t.nuts1_code = sp.nuts1_code AND sp.ct_group_code = t.ct_group_code
        ''',
        'tables': ['ct_mapping', 's_la_area_rental_per_ct_group',
            's_la_spending_by_ct', 's_nuts1_spending_by_ct_group'],
        'summary': '',
        'as_view': True,
        'stage': 6,
    },




    {
        'name': 's_lsoa_median_scat_ratable_breakeven',
        'sql': '''
             SELECT scat_code,
             usr_median(total_rateable_value) median_total_rateable_value,
             usr_median(total_break_even) median_total_break_even
             FROM {t1} GROUP BY scat_code
        ''',
        'tables': ['s_lsoa_general_summary'],
        'summary': '',
        'stage': 7,
    },

    {
        'name': 's_msoa_median_scat_ratable_breakeven',
        'sql': '''
             SELECT scat_code,
             usr_median(total_rateable_value) median_total_rateable_value,
             usr_median(total_break_even) median_total_break_even
             FROM {t1} GROUP BY scat_code
        ''',
        'tables': ['s_msoa_general_summary'],
        'summary': '',
        'stage': 7,
    },

    {
        'name': 's_la_scat_summary',
        'sql': '''
            SELECT s.scat_code, la_code, count, total_area,
            estimated_employees, estimated_employee_earnings,
            total_rateable_value, m.median_total_rateable_value,
            percent_diff(m.median_total_rateable_value, total_rateable_value)
                ratable_variance,
            total_break_even,
            percent_diff(m.median_total_break_even, total_break_even)
                break_even_variance
            FROM {t1} s
            LEFT JOIN {t2} m on m.scat_code = s.scat_code
        ''',
        'tables': ['s_la_general_summary', 's_la_median_scat_ratable_breakeven'],
        'stage': 8,
    },


    {
        'name': 'v_download_premises_data',
        'sql': '''
        SELECT
            v1.uarn,
            v1.address_number,
            v1.street,
            v1.town,
            v1.postal_district,
            v1.county,
            v1.postcode,
          --  scat_code (description),
            sc.desc scat_code_desc,
          --  la_code (description),
            la.desc la_desc,
            v1.description,
            v1.area,
            v1.area_source_code,
            v1.local_market,
         --   scat_group_code (description),
            sg.desc scat_group_desc,
            v1.la_code,
            v1.lsoa_code,
            v1.msoa_code,
            v2.ct_group_code,
            v2.rateable_value,
            v2.employees,
            v2.employee_cost,
            v2.break_even
        FROM {t1} v1
        JOIN {t2} v2 ON v1.uarn = v2.uarn
        JOIN {t3} sc ON v1.scat_code = sc.code
        JOIN {t4} sg ON v1.scat_group_code = sg.code
        JOIN {t5} la ON v1.la_code = la.code
        ''',
        'tables': ['v_premises_summary', 'v_premises_summary2',
            'c_scat', 'c_scat_group', 'c_la'],
        'as_view': True,
        'stage': 8,
    },

    {
        'name': 's_map_premises_names',
        'sql': '''
            SELECT v.uarn, v.lat, v.long
            FROM
            (SELECT count(uarn) count,
            lat, long
            from {t1} GROUP BY lat, long) t
            JOIN {t1} v
            ON v.lat = t.lat AND v.long = t.long
            AND t.count = 1
        ''',
        'tables': ['v_premises_summary2'],
        'as_view': False,
    },

    {
        'name': 's_map_premises_names_sg',
        'sql': '''
            SELECT v.uarn, v.lat, v.long, v.scat_group_code
            FROM
            (SELECT count(uarn) count,
            lat, long, scat_group_code
            from {t1} GROUP BY lat, long, scat_group_code) t
            JOIN {t1} v
            ON v.lat = t.lat AND v.long = t.long
            AND v.scat_group_code = t.scat_group_code
            AND t.count = 1
        ''',
        'tables': ['v_premises_summary2'],
        'as_view': False,
    },

    {
        'name': 's_map_premises_names_sc',
        'sql': '''
            SELECT v.uarn, v.lat, v.long, v.scat_code
            FROM
            (SELECT count(uarn) count,
            lat, long, scat_code
            from {t1} GROUP BY lat, long, scat_code) t
            JOIN {t1} v
            ON v.lat = t.lat AND v.long = t.long
            AND v.scat_code = t.scat_code
            AND t.count = 1
        ''',
        'tables': ['v_premises_summary2'],
        'as_view': False,
    },


    {
        'name': 's_map_premises',
        'sql': '''
         SELECT
            count(v.uarn) count,
            POINT(v.lat,v.long) as location,
            sum(rateable_value) rateable_value,
            sum(area) area,
            sum(employees) employees,
            sum(employee_cost) employee_cost,
            sum(break_even) break_even,
            coalesce(
                vac.tenant,
                concat(INITCAP(v.street), ' ', v.postcode,
                       ' [', count(v.uarn)::text, ']')
            ) as desc,
            vac.type,
            max(r.max) as max,
            quantile(r.max, 0.5) as median,
            min(r.max) as min
         FROM {t1} v
         LEFT OUTER JOIN {t2} vn
             ON vn.lat = v.lat AND vn.long=v.long
         LEFT OUTER JOIN {t3} r on r.uarn = vn.uarn
         LEFT OUTER JOIN {t4} vac on vac.uarn = vn.uarn
         GROUP BY v.lat, v.long, vac.type,
            vn.uarn, v.fp_id, vac.tenant, v.street, v.postcode
        ''',
        'tables': ['v_premises_summary2', 's_map_premises_names', 's_premesis_rating', 'v_vacancy_info'],
     #   'primary_key': 'location',
        'summary': 'Premises map location',
    },

    {
        'name': 's_map_premises_sc',
        'sql': '''
         SELECT
            count(v.uarn) count,
            POINT(v.lat,v.long) as location,
            sum(rateable_value) rateable_value,
            sum(area) area,
            sum(employees) employees,
            sum(employee_cost) employee_cost,
            sum(break_even) break_even,
            CASE
                WHEN vn.uarn is not null and vac.tenant is not null
                    THEN vac.tenant
                ELSE
                    concat(INITCAP(v.street), ' ', v.postcode,
                    ' [', count(v.uarn)::text, ']')
            END as desc,
            vac.type,
            v.scat_code,
            max(r.max) as max,
            quantile(r.max, 0.5) as median,
            min(r.max) as min
         FROM {t1} v
         LEFT OUTER JOIN {t2} vn
             ON vn.lat = v.lat AND vn.long=v.long
             AND vn.scat_code = v.scat_code
         LEFT OUTER JOIN {t3} r on r.uarn = v.uarn
         JOIN {t4} vac on vac.uarn = v.uarn
         GROUP BY v.scat_code, v.lat, v.long, vac.type,
            vn.uarn, v.fp_id, vac.tenant, v.street, v.postcode
        ''',
        'tables': ['v_premises_summary2', 's_map_premises_names_sc', 's_premesis_rating', 'v_vacancy_info'],
     #   'primary_key': 'location',
        'summary': 'Premises map location',
    },

    {
        'name': 's_map_premises_sg',
        'sql': '''
         SELECT
            count(v.uarn) count,
            POINT(v.lat,v.long) as location,
            sum(rateable_value) rateable_value,
            sum(area) area,
            sum(employees) employees,
            sum(employee_cost) employee_cost,
            sum(break_even) break_even,
            CASE
                WHEN vn.uarn is not null and vac.tenant is not null
                    THEN vac.tenant
                ELSE
                    concat(INITCAP(v.street), ' ', v.postcode,
                    ' [', count(v.uarn)::text, ']')
            END as desc,
            vac.type,
            v.scat_group_code,
            max(r.max) as max,
            quantile(r.max, 0.5) as median,
            min(r.max) as min
         FROM {t1} v
         LEFT OUTER JOIN {t2} vn
             ON vn.lat = v.lat AND vn.long = v.long
             AND vn.scat_group_code = v.scat_group_code
         LEFT OUTER JOIN {t3} r on r.uarn = v.uarn
         JOIN {t4} vac on vac.uarn = v.uarn
         GROUP BY v.scat_group_code, v.lat, v.long, vac.type,
            vn.uarn, v.fp_id, vac.tenant, v.street, v.postcode
        ''',
        'tables': ['v_premises_summary2', 's_map_premises_names_sg', 's_premesis_rating', 'v_vacancy_info'],
     #   'primary_key': 'location',
        'summary': 'Premises map location',
    },

    {
        'name': 'v_vacancy_info',
        'sql': '''
         SELECT
            uarn,
            tenant,
            bs.type
         FROM {t1} v
         CROSS JOIN {t2} bs
         WHERE
            CASE
                WHEN v.prop_empty = true THEN 1
                WHEN v.prop_empty = false THEN 0
                ELSE 2
            END = ANY(bs.values)
        ''',
        'tables': ['vacancy_info', 'bool_split'],
        'as_view': True,
        'summary': 'vacancy type for uarn',
    },
    {
        'name': 's_map_la',
        'sql': '''
         SELECT
            count(v.uarn) count, v.la_code,
            POINT(c.lat,c.long) as location,
            sum(rateable_value) rateable_value,
            sum(area) area,
            sum(employees) employees,
            sum(employee_cost) employee_cost,
            sum(break_even) break_even,
            vac.type,
            d.desc, max(r.max) as max,
            quantile(r.max, 0.5) as median,
            min(r.max) as min
         FROM {t1} v
         JOIN {t2} c on c.la_code = v.la_code
         JOIN {t3} d on c.la_code = d.code
         LEFT OUTER JOIN {t4} r on r.uarn = v.uarn
         JOIN {t5} vac on vac.uarn = v.uarn
         GROUP BY v.la_code, c.lat, c.long, d.desc, vac.type
        ''',
        'tables': ['v_premises_summary2', 'la_centroids', 'c_la',
            's_premesis_rating', 'v_vacancy_info'],
        'summary': 'Premises map all by la',
    },
    {
        'name': 's_map_la_sg',
        'sql': '''
         SELECT
            count(v.uarn) count, v.la_code,
            POINT(c.lat,c.long) as location,
            sum(rateable_value) rateable_value,
            sum(area) area,
            sum(employees) employees,
            sum(employee_cost) employee_cost,
            sum(break_even) break_even,
            vac.type,
            v.scat_group_code,
            d.desc, max(r.max) as max,
            quantile(r.max, 0.5) as median,
            min(r.max) as min
         FROM {t1} v
         JOIN {t2} c on c.la_code = v.la_code
         JOIN {t3} d on c.la_code = d.code
         LEFT OUTER JOIN {t4} r on r.uarn = v.uarn
         JOIN {t5} vac on vac.uarn = v.uarn
         GROUP BY v.scat_group_code, v.la_code, c.lat, c.long, d.desc, vac.type
        ''',
        'tables': ['v_premises_summary2', 'la_centroids', 'c_la', 's_premesis_rating', 'v_vacancy_info'],
        'summary': 'Premises map scat group by la',
    },
    {
        'name': 's_map_la_sc',
        'sql': '''
         SELECT
            count(v.uarn) count, v.la_code,
            POINT(c.lat,c.long) as location,
            sum(rateable_value) rateable_value,
            sum(area) area,
            sum(employees) employees,
            sum(employee_cost) employee_cost,
            sum(break_even) break_even,
            vac.type,
            v.scat_code,
            d.desc, max(r.max) as max,
            quantile(r.max, 0.5) as median,
            min(r.max) as min
         FROM {t1} v
         JOIN {t2} c on c.la_code = v.la_code
         JOIN {t3} d on c.la_code = d.code
         LEFT OUTER JOIN {t4} r on r.uarn = v.uarn
         JOIN {t5} vac on vac.uarn = v.uarn
         GROUP BY v.scat_code, v.la_code, c.lat, c.long, d.desc, vac.type
        ''',
        'tables': ['v_premises_summary2', 'la_centroids', 'c_la', 's_premesis_rating', 'v_vacancy_info'],
        'summary': 'Premises map location scat code by la',
    },
    {
        'name': 's_map_lsoa',
        'sql': '''
         SELECT
            count(v.uarn) count, v.lsoa_code,
            POINT(c.lat,c.long) as location,
            sum(rateable_value) rateable_value,
            sum(area) area,
            sum(employees) employees,
            sum(employee_cost) employee_cost,
            sum(break_even) break_even,
            vac.type,
            c.desc, max(r.max) as max,
            quantile(r.max, 0.5) as median,
            min(r.max) as min
         FROM {t1} v
         JOIN {t2} c on c.code = v.lsoa_code
         LEFT OUTER JOIN {t3} r on r.uarn = v.uarn
         JOIN {t4} vac on vac.uarn = v.uarn
         GROUP BY v.lsoa_code, c.lat, c.long, c.desc, vac.type
        ''',
        'tables': ['v_premises_summary2', 'c_lsoa', 's_premesis_rating', 'v_vacancy_info'],
        'summary': 'Premises map all by lsoa',
    },
    {
        'name': 's_map_lsoa_sg',
        'sql': '''
         SELECT
            count(v.uarn) count, v.lsoa_code,
            POINT(c.lat,c.long) as location,
            sum(rateable_value) rateable_value,
            sum(area) area,
            sum(employees) employees,
            sum(employee_cost) employee_cost,
            sum(break_even) break_even,
            vac.type,
            v.scat_group_code,
            c.desc, max(r.max) as max,
            quantile(r.max, 0.5) as median,
            min(r.max) as min
         FROM {t1} v
         JOIN {t2} c on c.code = v.lsoa_code
         LEFT OUTER JOIN {t3} r on r.uarn = v.uarn
         JOIN {t4} vac on vac.uarn = v.uarn
         GROUP BY v.scat_group_code, v.lsoa_code, c.lat, c.long, c.desc, vac.type
        ''',
        'tables': ['v_premises_summary2', 'c_lsoa', 's_premesis_rating', 'v_vacancy_info'],
        'summary': 'Premises map scat group by lsoa',
    },
    {
        'name': 's_map_lsoa_sc',
        'sql': '''
         SELECT
            count(v.uarn) count, v.lsoa_code,
            POINT(c.lat,c.long) as location,
            sum(rateable_value) rateable_value,
            sum(area) area,
            sum(employees) employees,
            sum(employee_cost) employee_cost,
            sum(break_even) break_even,
            vac.type,
            v.scat_code,
            c.desc, max(r.max) as max,
            quantile(r.max, 0.5) as median,
            min(r.max) as min
         FROM {t1} v
         JOIN {t2} c on c.code = v.lsoa_code
         LEFT OUTER JOIN {t3} r on r.uarn = v.uarn
         JOIN {t4} vac on vac.uarn = v.uarn
         GROUP BY v.scat_code, v.lsoa_code, c.lat, c.long, c.desc, vac.type
        ''',
        'tables': ['v_premises_summary2', 'c_lsoa', 's_premesis_rating', 'v_vacancy_info'],
        'summary': 'Premises map location scat code by lsoa',
    },
    {
        'name': 's_map_msoa',
        'sql': '''
         SELECT
            count(v.uarn) count, v.msoa_code,
            POINT(c.lat,c.long) as location,
            sum(rateable_value) rateable_value,
            sum(area) area,
            sum(employees) employees,
            sum(employee_cost) employee_cost,
            sum(break_even) break_even,
            vac.type,
            c.desc, max(r.max) as max,
            quantile(r.max, 0.5) as median,
            min(r.max) as min
         FROM {t1} v
         JOIN {t2} c on c.code = v.msoa_code
         LEFT OUTER JOIN {t3} r on r.uarn = v.uarn
         JOIN {t4} vac on vac.uarn = v.uarn
         GROUP BY v.msoa_code, c.lat, c.long, c.desc, vac.type
        ''',
        'tables': ['v_premises_summary2', 'c_msoa', 's_premesis_rating', 'v_vacancy_info'],
        'summary': 'Premises map all by msoa',
    },
    {
        'name': 's_map_msoa_sg',
        'sql': '''
         SELECT
            count(v.uarn) count, v.msoa_code,
            POINT(c.lat,c.long) as location,
            sum(rateable_value) rateable_value,
            sum(area) area,
            sum(employees) employees,
            sum(employee_cost) employee_cost,
            sum(break_even) break_even,
            vac.type,
            v.scat_group_code,
            c.desc, max(r.max) as max,
            quantile(r.max, 0.5) as median,
            min(r.max) as min
         FROM {t1} v
         JOIN {t2} c on c.code = v.msoa_code
         LEFT OUTER JOIN {t3} r on r.uarn = v.uarn
         JOIN {t4} vac on vac.uarn = v.uarn
         GROUP BY v.scat_group_code, v.msoa_code, c.lat, c.long, c.desc, vac.type
        ''',
        'tables': ['v_premises_summary2', 'c_msoa', 's_premesis_rating', 'v_vacancy_info'],
        'summary': 'Premises map scat group by msoa',
    },
    {
        'name': 's_map_msoa_sc',
        'sql': '''
         SELECT
            count(v.uarn) count, v.msoa_code,
            POINT(c.lat,c.long) as location,
            sum(rateable_value) rateable_value,
            sum(area) area,
            sum(employees) employees,
            sum(employee_cost) employee_cost,
            sum(break_even) break_even,
            vac.type,
            v.scat_code,
            c.desc, max(r.max) as max,
            quantile(r.max, 0.5) as median,
            min(r.max) as min
         FROM {t1} v
         JOIN {t2} c on c.code = v.msoa_code
         LEFT OUTER JOIN {t3} r on r.uarn = v.uarn
         JOIN {t4} vac on vac.uarn = v.uarn
         GROUP BY v.scat_code, v.msoa_code, c.lat, c.long, c.desc, vac.type
        ''',
        'tables': ['v_premises_summary2', 'c_msoa', 's_premesis_rating', 'v_vacancy_info'],
        'summary': 'Premises map location scat code by msoa',
    },


    {
        'name': 'v_la_scat_rating',
        'sql': '''
            SELECT
            max(r.max) rating,
            scat_code,
            la_code,
            scat_group_code
            FROM
            {t1} r
            RIGHT OUTER JOIN {t2} p on p.uarn = r.uarn
            GROUP BY la_code, scat_group_code, scat_code
        ''',
        'tables': ['s_premesis_rating', 'v_premises_summary2'],
        'summary': 'Top rating for la by scat code',
        'as_view': True,
    },
    {
        'name': 'v_la_general_summary_desc',
        'sql': '''


            SELECT
            t.la_code,
            la.desc as la_code_desc,
            s.code as scat_code,
            sg.code as scat_group_code,
            s.desc scat_code_desc,
            sg.desc scat_group_code_desc,
            t.count,
            round(t.total_area) total_area,
            round(t.total_break_even) total_break_even,
            t.break_even_variance,
            round(t.estimated_employee_earnings) estimated_employee_earnings,
            round(t.estimated_employees) estimated_employees,
            round(t.total_rateable_value) total_rateable_value,
            median_rate_per_area,
            min_rate_per_area,
            max_rate_per_area,
            ratable_variance,
            r.rating

            FROM {t1} t
            JOIN {t2} s ON s.code = t.scat_code
            JOIN {t3} sg ON sg.code = s.scat_group_code
            JOIN {t4} la ON la.code = t.la_code
            JOIN {t5} r ON r.la_code = t.la_code AND r.scat_code=t.scat_code
        ''',
        'tables': ['v_la_general_summary', 'c_scat', 'c_scat_group', 'c_la', 'v_la_scat_rating'],
        'summary': 'General LA summary used by client site',
        'as_view': True,
    },


    {
        'name': 's_area_parent_info',
        'sql': '''
            select distinct msoa_code code, la_code parent, null grandparent,
            c1.desc title, c2.desc title_parent, null title_grandparent
            from {t1} v
            join {t3} c1 on c1.code=v.msoa_code
            join {t4} c2 on c2.code=v.la_code
            where split_part(c2.desc, ',', 1) = regexp_replace(c1.desc, ' \w+$', '')

            union

            select distinct lsoa_code code, msoa_code parent, la_code grandparent,
            c1.desc title, c2.desc title_parent, c3.desc title_grandparent
            from {t1} v
            join {t2} c1 on c1.code=v.lsoa_code
            join {t3} c2 on c2.desc=left(c1.desc, -1)
            join {t4} c3 on split_part(c3.desc, ',', 1) = regexp_replace(c2.desc, ' \w+$', '')

            union

             select distinct la_code code, null parent, null grandparent,
             c.desc title, null title_parent, null title_grandparent
            from {t1} v
            join {t4} c on c.code=v.la_code
            ;
        ''',
        'tables': ['v_premises_summary2', 'c_lsoa', 'c_msoa', 'c_la'],
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


def get_file(directory, prefix):
    for file in os.listdir(directory):
        if file.startswith(prefix):
            return os.path.join(directory, file)


def vao_reader(data_type):
    f = get_file(os.path.join(config.DATA_PATH, DIRECTORY),'SMV')
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
    f = get_file(os.path.join(config.DATA_PATH, DIRECTORY), 'LIST')
    reader = unicode_csv_reader(f, encoding='latin-1', delimiter='*')
    import_csv(reader, VAO_LIST_TABLE, fields=vao_list_fields, verbose=verbose)


def importer(verbose=False):
    import_vao_list(verbose=verbose)
    import_vao_summary(verbose=verbose)
