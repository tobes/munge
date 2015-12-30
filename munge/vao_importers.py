import os.path

import config
from csv_util import unicode_csv_reader, import_csv
from sa_util import swap_tables, summary, build_view


vao_list_file = 'vao/LIST_2010_MERGED.dta.30Sep2015'

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

vao_file = 'vao/SMV_2010_MERGED.dta.30Sep2015'

vao_base_fields = [
    '*@id:bigserial',
    '-record_type',
    'ass_ref',
    '+uarn:bigint',
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
    '+uarn:bigint',
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
    '+uarn:bigint',
    '-record_type',
    'description',
    'oa_size:numeric',
    'oa_price:numeric',
    'oa_value:bigint',
]

vao_base_04_fields = [
    '*@id:bigserial',
    '+uarn:bigint',
    '-record_type',
    'plant_value:bigint',
]

vao_base_05_fields = [
    '*@id:bigserial',
    '+uarn:bigint',
    '-record_type',
    'spaces:bigint',
    'spaces_value:bigint',
    'area:bigint',
    'area_value:bigint',
    'total:bigint',
]

vao_base_06_fields = [
    '*@id:bigserial',
    '+uarn:bigint',
    '-record_type',
    'adj_desc',
    'adj_percent',

]

vao_base_07_fields = [
    '*@id:bigserial',
    '+uarn:bigint',
    '-record_type',
    'total_before_adj:bigint',
    'total_adj:bigint',
]

vao_types = [
    ('01', 'vao_base', vao_base_fields),
    ('02', 'vao_line', vao_base_02_fields),
    ('03', 'vao_additions', vao_base_03_fields),
    ('04', 'vao_plant', vao_base_04_fields),
    ('05', 'vao_parking', vao_base_05_fields),
    ('06', 'vao_adj', vao_base_06_fields),
    ('07', 'vao_adj_totals', vao_base_07_fields),
]


summary_data = [
    {
        'name': 's_vao_list_base_summary',
        'sql': '''
             SELECT
             count(t1.uarn) as list_entries,
             count(t2.uarn) as base_entries,
             count(t1.uarn) - count(t2.uarn) as difference,
             t1.scat_code as scat_code
             FROM "{t1}" t1
             LEFT OUTER JOIN "{t2}" t2 ON t1.uarn = t2.uarn
             LEFT OUTER JOIN "{t3}" t3 ON t3.code = t1.scat_code
             GROUP BY t3.desc, t1.scat_code
             ORDER BY t3.desc
        ''',
        'tables': ['vao_list', 'v_vao_base', 'c_scat'],
    },
    {
        'name': 's_vao_base_areas',
        'sql': '''
            SELECT la_code, scat_code, count(*),
            sum(total_area) as total_m2,
            sum(total_value) as total_value,
            sum(total_area * unadjusted_price) as total_area_price,
            (sum(total_area * unadjusted_price) - sum(total_value)) as diff
            FROM "{t1}"
            GROUP BY la_code, scat_code
        ''',
        'tables': ['v_vao_base'],

    },
    {
        'name': 's_vao_base_missing_list',
        'sql': '''
            SELECT
            t2.uarn, t2.scat_code, t2.ba_code
            FROM "{t1}" t1
            RIGHT OUTER JOIN "{t2}" t2 ON t1.uarn = t2.uarn
            WHERE t1.uarn is null
        ''',
        'tables': ['vao_list', 'v_vao_base'],
    },
]


views_data = [
    {
        'name': 'v_vao_base',
        'sql': '''
        CREATE VIEW "{name}" AS
        SELECT * FROM "{t1}" WHERE to_date is null;
        ''',
        'tables': ['vao_base'],
    },
]


def vao_reader(filename, data_type):
    f = os.path.join(config.DATA_PATH, filename)
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
        reader = vao_reader(vao_file, rec_type)
        import_csv(reader, table, fields=fields, verbose=verbose)


def import_vao_list(verbose=False):
    if verbose:
        print('importing vao_list')
    f = os.path.join(config.DATA_PATH, vao_list_file)
    reader = unicode_csv_reader(f, encoding='latin-1', delimiter='*')
    import_csv(reader, 'vao_list', fields=vao_list_fields, verbose=verbose)


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


def import_vao_full(verbose=False):
    import_vao_list(verbose=verbose)
    import_vao_summary(verbose=verbose)
    build_views(verbose=verbose)
    build_summaries(verbose=verbose)
    swap_tables(verbose=verbose)
