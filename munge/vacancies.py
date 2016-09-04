import xlrd
from os import listdir
from os.path import isfile, join
from datetime import datetime
from decimal import Decimal

import unicodecsv as csv

from sa_util import run_sql

DIR = '/home/toby/whythawk/munge/data/vacancy/'

la_codes = [x[0] for x in run_sql('select code from c_la')]


def make_date(row, idx, date_mode):
    x = row[idx].ctype
    if x in [0, 5, 6]:
        return None
    v = row[idx].value
    if x in [3]:
        return datetime(*xlrd.xldate_as_tuple(v, date_mode))
    elif x in [1]:
        v = v.strip()
        if v == '':
            return None
        v = v.replace('.', '/').split(' ')[0]
        return datetime.strptime(v, '%d/%m/%Y')
    else:
        raise Exception('INVALID DATE')


def make_la_code(row, idx, la):
    v = row[idx].value.strip()
    if v != la:
        raise Exception('INVALID LA')
    elif v not in la_codes:
        raise Exception('INVALID LA (CODE)')
    else:
        return v


def make_bool(row, idx):
    x = row[idx].ctype
    if x in [0, 2, 3, 5, 6]:
        return None
    v = row[idx].value
    if x in [4]:
        return bool(v)
    elif x in [1]:
        v = v.strip()
        if v == '':
            return None
        elif v == 'Y':
            return True
        elif v == 'N':
            return False
        raise Exception('INVALID BOOL')
    else:
        raise Exception('INVALID BOOL')


def make_number(row, idx):
    x = row[idx].ctype
    if x in [0, 3, 4, 5, 6]:
        return None
    v = row[idx].value
    if x in [2]:
        return v
    elif x in [1]:
        v = v.strip()
        if v == '':
            return None
        raise Exception('INVALID INT')
    else:
        raise Exception('INVALID INT')


def check_ba_ref(la, ba_ref):
    ba_ref = '0*%s' % ba_ref
    result = run_sql(
        'select ba_ref from vao_list where la_code=:la and ba_ref SIMILAR TO  :ba',
        ba=ba_ref, la=la)
    for x in result:
        return x[0]


def make_ba_ref(row, idx, la):
    x = row[idx].ctype
    if x in [0, 3, 4, 5, 6]:
        raise Exception('INVALID BA REF')
    v = row[idx].value
    if x in [2]:
        v = str(int(v))
        return v#, check_ba_ref(la, v)
    elif x in [1]:
        v = v.strip()
        if v == '':
            raise Exception('INVALID BA REF')
        return v#, check_ba_ref(la, v)
    else:
        raise Exception('INVALID BA REF')


def process(f):
    out = []
    errors = []
    print f
    parts = f.split('.')[0].split('_')
    source = parts[0]
    la = parts[1]
    book = xlrd.open_workbook(join(DIR, f))
    date_mode = book.datemode
    sheet = book.sheet_by_index(0)
    for x in xrange(1, sheet.nrows):
        row = sheet.row(x)
        try:
            out.append(
                [
                    make_la_code(row, 0, la),
                    make_ba_ref(row, 1, la),
                    make_bool(row, 2),
                    make_date(row, 3, date_mode),
                    make_bool(row, 4),
                    make_date(row, 5, date_mode),
                    make_number(row, 6),
                    row[7].value,
                ]
            )

        except Exception as e:
            errors.append([f, x, e] + sheet.row_values(x))
    return out, errors


def import_raw_xlsx():
    fields = [
        'la_code',
        'ba_ref',
        'prop_empty:boolean',
        'prop_empty_date:date~make_date_YYYY_MM_DD',
        'prop_occupied:boolean',
        'prop_occupied_date:date',
        'prop_ba_rates:numeric',
        'tenant',
    ]

    files = [f for f in listdir(DIR) if isfile(join(DIR, f))]


    with open('vacancy_errors.csv', 'w') as fe:
        with open('vacancy.csv', 'w') as fp:
            a = csv.writer(fp, delimiter=',')
            e = csv.writer(fe, delimiter=',')
            a.writerows([fields])
            e.writerows([['file', 'line', 'error'] + fields])
            for f in files:
                out, errors = process(f)
                a.writerows(out)
                e.writerows(errors)

sql = '''
DROP TABLE vacancy_updates;
'''
#run_sql(sql)

sql = '''
DROP TABLE vacancy_info CASCADE;
'''
#run_sql(sql)

sql = '''
    CREATE TABLE IF NOT EXISTS vacancy_updates (
        la_code text NOT NULL,
        ba_ref text NOT NULL,
        uarn bigint,
        prop_empty boolean,
        prop_empty_date date,
        prop_occupied boolean,
        prop_occupied_date date,
        prop_ba_rates numeric,
        tenant text,
        postcode text,
        last_updated timestamp
    );

    CREATE TABLE IF NOT EXISTS vacancy_info (
        la_code text NOT NULL,
        uarn bigint,
        prop_empty boolean,
        real_data boolean,
        prop_occupied_date date,
        prop_empty_date date,
        tenant text,
        last_updated timestamp
    );

do
$$
begin
if not exists (
    select indexname
        from pg_indexes
    where
        tablename = 'vacancy_info'
        and indexname = 'vacancy_info_uarn'
)
then
    create index vacancy_info_uarn on vacancy_info (uarn);
end if;
end
$$;

do
$$
begin
if not exists (
    select indexname
        from pg_indexes
    where
        tablename = 'vacancy_updates'
        and indexname = 'vacancy_updates_ba_ref'
)
then
    create index vacancy_updates_ba_ref on vacancy_updates (ba_ref);
end if;
end
$$;

do
$$
begin
if not exists (
    select indexname
        from pg_indexes
    where
        tablename = 'vao_list_raw'
        and indexname = 'vao_base_ba_ref'
)
then
    create index vao_base_ba_ref on vao_list_raw (ba_ref);
end if;
end
$$;

do
$$
begin
if not exists (
    select indexname
        from pg_indexes
    where
        tablename = 'vacancy_updates'
        and indexname = 'vacancy_updates_uarn'
)
then
    create index vacancy_updates_uarn on vacancy_updates (uarn);
end if;
end
$$;
'''
run_sql(sql)

sql = '''
    CREATE OR REPLACE VIEW v_vacancy_stats AS
    SELECT
    la_code,
    count(uarn) premisis_count,
    count(nullif(real_data, false)) real_count,
    100 * count(nullif(prop_empty, false)) / count(uarn) percent_empty,
    count(nullif(prop_empty, false)) empty_count,
    100 * count(nullif(real_data, false)) / count(uarn) percent_real
    FROM vacancy_info
    GROUP BY la_code
    ORDER BY la_code
'''

run_sql(sql)


# import_raw_xlsx()


def make_date_c(v):
    return datetime.strptime(v.split(' ')[0], '%Y-%m-%d')


def make_bool_c(v):
    return v == 'True'


def make_number_c(v):
    return Decimal(v)


def make_text_c(v):
    return v


def get_uarn(la_code='', ba_ref=''):
    sql = '''
    SELECT uarn FROM vao_list
    WHERE trim(leading '0' from ba_ref)=:ba_ref
    '''
    if la_code:
        sql += '''
            AND la_code=:la_code
            '''
    if la_code and ba_ref[0] not in '0123456789' and len(ba_ref) > 9:
        count = 3
    else:
        count = 0
    for x in range(count + 1):
        result = run_sql(sql, ba_ref=ba_ref[x:].lstrip('0'), la_code=la_code)
        for r in result:
            return r[0]


def update_matches():
    sql = '''
    SELECT ba_ref, la_code FROM vacancy_updates
    WHERE uarn is null
    '''
    result = run_sql(sql)
    count = 0
    match = 0
    for row in result:
        uarn = get_uarn(row[1], row[0])
        if uarn:
            sql = '''
            UPDATE vacancy_updates SET uarn = :uarn
            WHERE ba_ref = :ba_ref AND la_code = :la_code
            '''
            run_sql(sql, ba_ref=row[0], la_code=row[1], uarn=uarn)
            match += 1
        count += 1
        if count % 100 == 0:
            print count, match
    print count, match


def update_matches_wrong_la():
    sql = '''
    SELECT ba_ref FROM vacancy_updates
    WHERE uarn is null
    '''
    result = run_sql(sql)
    count = 0
    match = 0
    for row in result:
        print count, match
        uarn = get_uarn(ba_ref=row[0])
        if uarn:
         #   sql = '''
         #   UPDATE vacancy_updates SET uarn = :uarn
         #   WHERE ba_ref = :ba_ref AND la_code = :la_code
         #   '''
         #   run_sql(sql, ba_ref=row[0], la_code=row[1], uarn=uarn)
            match += 1
        count += 1
        if count % 100 == 0:
            print count, match
    print count, match


def update_vacancies():
    data = {}
    fields = [
        ('prop_empty', make_bool_c),
        ('prop_empty_date', make_date_c),
        ('prop_occupied', make_bool_c),
        ('prop_occupied_date', make_date_c),
        ('prop_ba_rates', make_number_c),
        ('tenant', make_text_c),
    ]

    with open('vacancy.csv') as f:
        rows = csv.reader(f, delimiter=',')
        count = 0
        for row in rows:
            count += 1
            if count == 1:
                continue
        #    if count == 100:
        #        break
            sql = '''
            SELECT uarn FROM vacancy_updates
            WHERE ba_ref=:ba_ref AND la_code=:la_code
            '''
            result = run_sql(sql, ba_ref=row[1], la_code=row[0])
            exists = False
            uarn = None
            for r in result:
                exists = True
                uarn = r[0]

            if not exists:
                sql = '''
                INSERT INTO vacancy_updates (ba_ref, la_code)
                VALUES (:ba_ref, :la_code)
                '''
                result = run_sql(sql, ba_ref=row[1], la_code=row[0])

            updates = {}
          #  if not uarn:
          #      uarn = get_uarn(la_code=row[0], ba_ref=row[1])
          #      if uarn:
          #          updates['uarn'] = uarn

            for i in range(6):
                if row[i + 2].strip():
                    updates[fields[i][0]] = fields[i][1](row[i + 2])
            if updates:
                updates['last_updated'] = datetime.now()
            params = ' ,'.join(['%s = :%s' % (k, k) for k in updates.keys()])
            sql = '''
            UPDATE vacancy_updates SET %s
            WHERE ba_ref = :ba_ref AND la_code = :la_code
            ''' % (params)
            run_sql(sql, ba_ref=row[1], la_code=row[0], **updates)
            if count % 100 == 0:
                print count - 1, len(data)


def match_uarn():


    sql = """
UPDATE vacancy_updates
SET uarn=subquery.uarn
FROM (

    SELECT b.uarn, b.ba_ref, b.la_code
    FROM vao_list b
    JOIN vacancy_updates v
    ON trim(leading '0' from v.ba_ref) = trim(leading '0' from b.ba_ref)
    WHERE v.uarn is null

) AS subquery
WHERE vacancy_updates.ba_ref=subquery.ba_ref
AND vacancy_updates.la_code=subquery.la_code;
    """
    run_sql(sql)

    sql = """
UPDATE vacancy_updates
SET uarn=subquery.uarn
FROM (

    SELECT b.uarn, v.ba_ref, v.la_code
    FROM vao_list b
    JOIN vacancy_updates v
    ON ltrim(ltrim(v.ba_ref, 'N'''), '0') = ltrim(b.ba_ref, '0')
    AND b.la_code = v.la_code
    WHERE v.uarn is null
    ORDER BY uarn


) AS subquery
WHERE vacancy_updates.ba_ref=subquery.ba_ref
AND vacancy_updates.la_code=subquery.la_code;
    """
    run_sql(sql)



    sql = """
UPDATE vacancy_updates
SET uarn=subquery.uarn
FROM (

 SELECT b.uarn, v.ba_ref, v.la_code
FROM vacancy_updates v
JOIN vao_list b
ON v.ba_ref = right(b.ba_ref, length(v.ba_ref))
AND b.la_code = v.la_code
WHERE v.uarn is null
AND length(v.ba_ref) > 6
AND v.la_code in
('E07000195', 'W06000006', 'E07000085', 'E07000087',
'E06000056', 'E07000089', 'E06000043', 'E07000099',
'E06000034', 'E07000203', 'E07000102', 'E07000117',
'E07000135', 'W06000004', 'E07000193', 'W06000011',
'W06000013', 'W06000024', 'E07000200', 'E07000067',
'E07000032', 'E07000080', 'W06000003')

) AS subquery
WHERE vacancy_updates.ba_ref=subquery.ba_ref
AND vacancy_updates.la_code=subquery.la_code;
    """
    run_sql(sql)


def info_table():

    sql = '''
    INSERT INTO vacancy_info (la_code, uarn)
    SELECT la_code, uarn from vao_list;
    '''

    # run_sql(sql)

    sql = '''
    SELECT DISTINCT la_code FROM vacancy_updates
    '''

    results = run_sql(sql)

    for row in results:
        la_code = row[0]

        print(la_code)
        sql = '''
        DELETE FROM vacancy_info
        WHERE la_code=:la_code
        '''
        data = run_sql(sql, la_code=la_code)

        sql = '''
        SELECT DISTINCT prop_empty
        FROM vacancy_updates where la_code=:la_code
        '''

        data = run_sql(sql, la_code=la_code)
        default_empty = None
        for d in data:
            if default_empty:
                default_empty = None
                break
            else:
                default_empty = not d[0]

        sql = '''
        UPDATE vacancy_info
        SET
        prop_empty = :default_empty,
        real_data = false
        WHERE la_code = :la_code
        '''

        run_sql(sql, la_code=la_code, default_empty=default_empty)

        sql = '''
        UPDATE vacancy_info vi
        SET
        prop_empty=v.prop_empty,
        prop_occupied_date=v.prop_occupied_date,
        prop_empty_date=v.prop_empty_date,
        tenant=v.tenant,
        real_data = true
        FROM (
            SELECT
                uarn,
                prop_empty,
                prop_occupied_date,
                prop_empty_date,
                tenant
            FROM vacancy_updates
            WHERE la_code = :la_code
        ) AS v
        WHERE vi.uarn = v.uarn
        '''

        run_sql(sql, la_code=la_code)
        print('done')
#import_raw_xlsx()
#update_vacancies()
#match_uarn()
#update_matches()
#update_matches_wrong_la()


#info_table()
