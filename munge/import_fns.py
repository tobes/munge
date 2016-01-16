import datetime

from sa_util import run_sql


AUTO_FNS = {
    'double precision': 'make_float',
    'bigint': 'make_int',
    'smallint': 'make_int',
    'integer': 'make_int',
    'boolean': 'make_bool',
    'numeric': 'make_numeric',
}


la_sub_2_la_dict = {}
ba_2_la_dict = {}


def make_bool(value):
    if value == '':
        return None
    return str(value).lower() in ['yes', 'true', 'y', 't', '1', '0']


def make_int(value):
    if value == '':
        return None
    return int(value)


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


def make_str(value):
    return value


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


def outcode(value):
    if value == '':
        return None
    else:
        return value.split(' ')[0]


def areacode(value):
    if value == '':
        return None
    else:
        parts = value.split(' ')
        return parts[0] + ' ' + parts[1][0]


def copy(value):
    return value


def la_sub_2_la(value):
    if not la_sub_2_la_dict:
        sql = 'SELECT la_sub_code, la_code FROM l_la_sub_la;'
        result = run_sql(sql)
        for row in result:
            la_sub_2_la_dict[row[0]] = row[1]
    return la_sub_2_la_dict.get(value)


def ba_2_la(value):
    if not ba_2_la_dict:
        sql = 'SELECT ba_code, la_code FROM l_ba_la;'
        result = run_sql(sql)
        for row in result:
            ba_2_la_dict[row[0]] = row[1]
    return ba_2_la_dict.get(value)


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
