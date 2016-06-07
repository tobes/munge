import datetime

from sa_util import run_sql


AUTO_FNS = {
    'double precision': 'make_float',
    'float': 'make_float',
    'bigint': 'make_int',
    'smallint': 'make_int',
    'integer': 'make_int',
    'boolean': 'make_bool',
    'numeric': 'make_numeric',
}


lookups = {}


def make_bool(value):
    if value == '':
        return None
    return str(value).lower() in ['yes', 'true', 'y', 't', '1']


def make_int(value):
    if value == '':
        return None
    try:
        return int(value)
    except ValueError:
        return int(float(value))


def int_commas(value):
    value = value.replace(',', '')
    if value == '':
        return None
    return int(value)


def make_scat(value):
    if not value:
        return None
    return int(value[:-1])


def make_float(value):
    if value == '':
        return None
    return float(value)


def make_date_DD_MON_YYYY(value):
    if value == '':
        return None
    date = datetime.datetime.strptime(value, "%d-%b-%Y")
    return date.date()


def make_date_YYYY_MM_DD(value):
    if value == '':
        return None
    date = datetime.datetime.strptime(value.split(' ')[0], "%Y-%m-%d")
    return date.date()


def make_str(value):
    return value


def remove_comma(value):
    if value == '':
        return None
    return int(value.replace(',', ''))


def make_numeric(value):
    if value == '':
        return None
    return value


def make_numeric_na(value):
    if value == '' or value.strip().lower() == 'n/a':
        return None
    return value


def compact_pc(value):
    if value == '':
        return None
    else:
        return value.replace(' ', '').upper()


def compact_space(value):
    if value == '':
        return None
    else:
        return ' '.join(value.split())


def outcode(value):
    if value == '':
        return None
    else:
        return value.split()[0]


def areacode(value):
    if value:
        parts = value.split()
        if len(parts) > 1:
            return parts[0] + ' ' + parts[1][0]
    return None


def copy(value):
    return value


def ct_level(value):
    return len(value.split('.'))


def ct_level_1(value):
    s = str(value).split('.')
    return int(s[0])


def ct_level_2(value):
    s = str(value).split('.')
    if len(s) > 1:
        return int(s[1])


def ct_level_3(value):
    s = str(value).split('.')
    if len(s) > 2:
        return int(s[2])


def _translation_lookup(name, sql):
    def fn(value):
        if name not in lookups:
            result = run_sql(sql)
            l = {}
            for row in result:
                l[row[0]] = row[1]
            lookups[name] = l
        return lookups[name].get(value)
    return fn

# FIX ME MOVE INTO importer
sql = 'SELECT la_sub_code, la_code FROM l_la_sub_la;'
la_sub_2_la = _translation_lookup('la_sub_2_la', sql)


sql = 'SELECT ba_code, la_code FROM l_ba_la;'
ba_2_la = _translation_lookup('ba_2_la', sql)

sql = 'SELECT la_code, nuts1_code FROM l_la_nuts;'
la_sub_2_nuts1 = _translation_lookup('la_sub_2_nuts1', sql)

sql = 'SELECT la_code, nuts2_code FROM l_la_nuts;'
la_sub_2_nuts2 = _translation_lookup('la_sub_2_nuts2', sql)

sql = 'SELECT la_code, nuts3_code FROM l_la_nuts;'
la_sub_2_nuts3 = _translation_lookup('la_sub_2_nuts3', sql)
