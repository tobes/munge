import xlrd
import re
import numbers


f = '/home/toby/whythawk/munge/data/ons/consumertrends2015q1cpsa_tcm77-408694-1.xls'

f2 = '/home/toby/whythawk/munge/data/ons/MYE2_population_by_sex_and_age_for_local_authorities_UK.xls'

f3 = '/home/toby/whythawk/munge/data/ons/a35final2011_tcm77-282401.xls'
f4 = '/home/toby/whythawk/munge/data/ons/a11final2011_tcm77-281053.xls'

f5 = '/home/toby/whythawk/munge/data/ons/Work Geography Table 7.7a   Annual pay - Gross 2013.xls'


def ons_xls(file_path, regex=None, sheets=None):
    book = xlrd.open_workbook(file_path)

    sheet_names = []
    for name in book.sheet_names():
        if regex and re.match(regex, name):
            sheet_names.append(name)
        elif sheets and name in sheets:
            sheet_names.append(name)

    codes_info = {}
    codes_data = {}
    for sheet_name in sheet_names:
        sheet = book.sheet_by_name(sheet_name)

        codes_desc = sheet.row_values(8)[1:]
        codes_num = sheet.row_values(10)[1:]
        codes_alpha = sheet.row_values(12)[1:]

        for index in xrange(len(codes_alpha)):
            info = {
                'index': codes_num[index],
                'description': codes_desc[index]
            }
            codes_info[codes_alpha[index]] = info


        row_count = 13

        while True:
            row_count += 1
            try:
                row = sheet.row_values(row_count)
            except IndexError:
                break
            try:
                key = int(row[0])
            except ValueError:
                key = unicode(row[0])
            if not key:
                continue
            values = row[1:]
            if key not in codes_data:
                codes_data[key] = {}
            values = dict(zip(codes_alpha, values))
            codes_data[key].update(values)
        #    print key, values
        #    print dict(zip(codes_alpha, values))


    print codes_data
    print codes_info

def ons_simple(file_path, regex=None, sheets=None, key_name=None, fields=None):
    book = xlrd.open_workbook(file_path)

    sheet_names = []
    for name in book.sheet_names():
        if regex and re.match(regex, name):
            sheet_names.append(name)
        elif sheets and name in sheets:
            sheet_names.append(name)
    fields_row = 2
    data = {}
    for sheet_name in sheet_names:
        sheet = book.sheet_by_name(sheet_name)
        field_names = sheet.row_values(fields_row)

        print fields
        row_count = 3

        while True:
            row_count += 1
            try:
                row = sheet.row_values(row_count)
            except IndexError:
                break
            key = row[field_names.index(key_name)]
            if not key:
                continue
            print key
            values = {}
            for field in fields:
                value = row[field_names.index(field)]
                values[field] = value
            data[key] = values
    print data

def is_num(x):
    return isinstance(x, numbers.Number)


def ons_madness(file_path, inset=4, field_names=None):

    data = []
    code_count = []
    def fix(row):
        key_info = []
        values = []
        found = False
        for col in row:
            if col != '':
                if not found:
                    key_info.append(col)
                else:
                    values.append(col)
            else:
                if key_info:
                    found = True
        if len(key_info) < 2:
            code_count.append(True)
            key_info = ['0.%s' % len(code_count)] + key_info
        data.append({'key':key_info, 'values':values})

    book = xlrd.open_workbook(file_path)

    sheet = book.sheet_by_index(0)

    row_count = 0
    while True:
        row_count += 1
        try:
            row = sheet.row_values(row_count)
        except IndexError:
            break
        if row[len(row) - 1] and is_num(row[len(row) - 1]):

            fix(row)

    output_data = []
    codes_info = [dict(code=x['key'][0], description=x['key'][1]) for x in data]
    for row in data:
        values = row['values']
        key = row['key'][0]
        for i in xrange(len(values)):
            output_data.append([key, field_names[i], values[i]])
    return output_data, codes_info


def ons_pay(file_path, fields):
    book = xlrd.open_workbook(file_path)

    sheet = book.sheet_by_name('All')
    row_count = 0

    data = []
    while True:
        row_count += 1
        try:
            row = sheet.row_values(row_count)
        except IndexError:
            break
        if len(row[1]) != 9:
            continue
        row_dict = {}
        for i in xrange(len(fields)):
            if fields[i]:
                row_dict[fields[i]] = row[i]
        data.append(row_dict)

    print data


#ons_simple(f2, sheets=['UK persons'], key_name='CODE', fields=['ALL AGES'])

# HIE1
#ons_xls(f, regex=r'\d\dCS')

regions = [
    'E12000001',
    'E12000002',
    'E12000003',
    'E12000004',
    'E12000005',
    'E12000006',
    'E12000007',
    'E12000008',
    'E12000009',
    'E99999999', # England
    'W99999999',
    'S99999999',
    'N99999999',
    'UK9999999', # UK
]
# HIE1a
#print ons_madness(f3, field_names=regions)

# HIE1b
ages = [
        'AGE_0030',
        'AGE_3049',
        'AGE_5064',
        'AGE_6574',
        'AGE_75UP',
        'AGE_ALL',
        ]
#print ons_madness(f4, field_names=ages)

work_fields = [
    '',
    'key',
    'jobs',
    'median',
    'median_p',
    '',
    '',
    'p10',
    'p20',
    'p25',
    'p30',
    'p40',
    'p60',
    'p70',
    'p75',
    'p80',
    'p90',
]
# HIE3
ons_pay(f5, fields=work_fields)
