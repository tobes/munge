import datetime



AUTO_FNS = {
    'double precision': 'make_float',
    'bigint': 'make_int',
    'smallint': 'make_int',
    'integer': 'make_int',
    'boolean': 'make_bool',
    'numeric': 'make_numeric',
}


def make_bool(value):
    if value == '':
        return None
    return str(value).lower() in ['yes', 'true', 'y', 't', '1', '0']


def make_int(value):
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
