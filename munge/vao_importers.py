import os.path

import config
from csv_util import unicode_csv_reader, import_csv, create_table, insert_rows, build_indexes
from sa_util import swap_tables, run_sql, get_result_fields


vao_list_file = 'vao/LIST_2010_MERGED.dta.30Sep2015'

vao_list_fields = [
    '*@id:bigserial',
    'inc_entry_no:bigint',
    '+ba_code',
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
    'effective_date:date~make_date_DD_MON_YYYY',
    'composite',
    'rateable_value:double precision',
    'settlement_type',
    'ass_ref',
    'alter_date:date~make_date_DD_MON_YYYY',
    'scat_code:smallint~make_scat',
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
    'ba_code',
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
    'scheme_ref',
    'desc',
    'total_area:double precision',
    'subtotal',
    'total_value:double precision',
    'adopted_rv',
    'list_year:int',
    'ba',
    'ba_ref',
    'vo_ref',
    'from_date:date~make_date_DD_MON_YYYY',
    'to_date:date~make_date_DD_MON_YYYY',
    'scat_code:smallint',
    'measure_unit',
    'unadjusted_price:double precision',
]


vao_base_02_fields = [
    '*@id:bigserial',
    '+uarn:bigint',
    '-record_type',
    'line:smallint',
    'floor',
    'description',
    'area',
    'price:double precision',
    'value:double precision',
]

vao_base_03_fields = [
    '*@id:bigserial',
    '+uarn:bigint',
    '-record_type',
    'description',
    'oa_size:double precision',
    'oa_price:double precision',
    'oa_value',
]



vao_base_04_fields = [
    '*@id:bigserial',
    '+uarn:bigint',
    '-record_type',
    'plant_value',
]



vao_base_05_fields = [
    '*@id:bigserial',
    '+uarn:bigint',
    '-record_type',
    'spaces:double precision',
    'spaces_value:double precision',
    'area:double precision',
    'area_value:double precision',
    'total:double precision',
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
    'total_before_adj:double precision',
    'total_adj:double precision',
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


summary_data = {
    's_vao_list_base_summary': '''
         SELECT
         count(l.uarn) as list_entries,
         count(b.uarn) as base_entries,
         count(l.uarn) - count(b.uarn) as difference,
         c.desc as scat_desc,
         l.scat_code as scat_code
         FROM vao_list l
         LEFT OUTER JOIN vao_base b ON l.uarn = b.uarn
         LEFT OUTER JOIN c_scat c ON c.code = l.scat_code
         GROUP BY c.desc, l.scat_code
         ORDER BY c.desc;
    ''',

    's_vao_base_areas': '''
        SELECT ba_code, scat_code, count(*),
        sum(total_area) as total_m2,
        sum(total_value) as total_value,
        sum(total_area * unadjusted_price) as total_area_price,
        (sum(total_area * unadjusted_price) - sum(total_value)) as diff
        FROM vao_base
        GROUP BY ba_code, scat_code;
    ''',
}


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
            print 'importing', table
        reader = vao_reader(vao_file, rec_type)
        import_csv(reader, table, fields=fields, verbose=verbose)


def import_vao_list(verbose=False):
    if verbose:
        print 'importing vao_list'
    f = os.path.join(config.DATA_PATH, vao_list_file)
    reader = unicode_csv_reader(f, encoding='latin-1', delimiter='*')
    import_csv(reader, 'vao_list', fields=vao_list_fields, verbose=verbose)


def summary(table, sql, verbose=False):
    if verbose:
        print 'creating summary table %s' % table
    result = run_sql(sql)
    first = True
    count = 0
    data = []
    for row in result:
        if first:
            fields = get_result_fields(result)
            create_table(table, fields)
            f = [field['name'] for field in fields if not field.get('missing')]
            insert_sql = insert_rows(table, fields)
            first = False
        data.append(dict(zip(f, row)))
        count += 1
        if count % config.BATCH_SIZE == 0:
            run_sql(insert_sql, data)
            data = []
            if verbose:
                print(count)
    if data:
        run_sql(insert_sql, data)

    if verbose:
        print('%s rows imported' % (count))
    # Add indexes
    build_indexes(table, fields, verbose=verbose)


def build_summaries(verbose=False):
    for table in summary_data:
        summary('#' + table, summary_data[table], verbose=verbose)
    swap_tables(verbose=verbose)


def import_vao_full(verbose=False):
    import_vao_list(verbose=verbose)
    import_vao_summary(verbose=verbose)
    #build_summaries(verbose=verbose)
    swap_tables(verbose=verbose)
