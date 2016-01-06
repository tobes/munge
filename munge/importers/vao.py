import os.path

from munge import config
from munge.csv_util import import_csv, unicode_csv_reader

DIRECTORY = 'spending'

VAO_LIST_TABLE = 'vao_list'

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


AUTO_SQL = [
    {
        'name': 'vao_index',
        'sql': '''
             SELECT DISTINCT ON (uarn)
                 first_value(uarn) OVER wnd AS uarn,
                 first_value(version) OVER wnd AS version
             FROM {t1}
             WINDOW wnd AS (
                 PARTITION BY uarn, version ORDER BY version DESC
                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
             )
        ''',
        'tables': ['vao_base_raw'],
        'primary_key': ['uarn', 'version'],
        'disabled': False,
        'early': True,
        'summary': '',
    },
    {
        'name': 'vao_base',
        'sql': '''
        SELECT b.*, l.scat_code AS scat_code_list, l.outcode
        FROM {t1} b
        LEFT JOIN {t2} i ON i.uarn = b.uarn AND i.version = b.uarn
        LEFT JOIN {t3} l ON l.uarn = b.uarn
        WHERE to_date IS NOT null;
        ''',
        'tables': ['vao_base', 'vao_index', 'vao_list'],
        'as_view': True,
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
        'tables': ['v_vao_base', 'c_scat', 'c_scat_group'],
        'disabled': True,
        'summary': '',
    },
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
        'tables': ['v_vao_base', 'c_scat'],
        'disabled': True,
        'summary': '',
    },
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
        'tables': ['vao_list', 'v_vao_base', 'c_scat'],
        'disabled': True,
        'summary': '',
    },
    {
        'name': 's_vao_base_areas',
        'sql': '''
            SELECT la_code, scat_code, count(*),
            sum(total_area) as total_m2,
            sum(total_value) as total_value,
            avg(total_area) as mean_m2,
            usr_median(total_area) as median_m2,
            usr_mode(total_area) as mode_m2,
            sum(total_area * unadjusted_price) as total_area_price,
            (sum(total_area * unadjusted_price) - sum(total_value)) as diff
            FROM {t1}
            GROUP BY la_code, scat_code
        ''',
        'tables': ['v_vao_base'],
        'disabled': True,
        'summary': '',
    },
    {
        'name': 's_vao_scat_median_areas',
        'sql': '''
        SELECT code as scat_code,
        (
            WITH y AS (
               SELECT total_area, row_number() OVER (ORDER BY total_area) AS rn
               FROM   {t1}
               WHERE  total_area IS NOT NULL
               AND scat_code = code
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
               FROM   {t1}
               WHERE  total_area IS NOT NULL
               AND scat_code = code
        ) count
        FROM {t2}
        ''',
        'tables': ['v_vao_base', 'c_scat'],
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
        'tables': ['v_vao_base', 's_vao_scat_median_areas', 's_vao_base_areas_min_max'],
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
        'tables': ['vao_list', 'v_vao_base'],
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
    for row in reader:
        r_type = row[0]
        if r_type == '01':
            uarn = row[2]
        if r_type == data_type:
            if r_type != '01':
                row = [uarn] + row
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
