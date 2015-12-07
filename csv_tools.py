import os.path
import csv

def csv_2_gen(file_name, fields=None, delimiter=None, translate=None, transform=None, trim=None):
    if delimiter is None:
        delimiter = ','
    with open(file_name, 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=delimiter)
        if fields:
            header_fields = fields
        else:
            header_fields = map(str.strip, reader.next())
            fields = header_fields

        for row in reader:
            out = {}
            for field in fields:
                value = row[header_fields.index(field)]
                if translate and field in translate:
                    value = translate[field].get(value)
                if transform and field in transform:
                    value = transform[field](value)
                if trim and field in trim:
                    value = value.strip()
                out[field] = value
            yield out


def csv_2_list(*args, **kw):
    out = []
    for row in csv_2_gen(*args, **kw):
        out.append(row)
    return out

def csv_2_table(*args, **kw):
    count = 0
    table = kw.pop('sa_obj').__table__
    rows = []
    conn = kw.pop('conn')
    conn.execute('TRUNCATE TABLE %s' % table)
    for row in csv_2_gen(*args, **kw):
        count += 1
        rows.append(row)
        if count % 1000 == 0:
            conn.execute(table.insert(), rows)
            rows = []
            print count
    if rows:
        conn.execute(table.insert(), rows)


def csv_2_print(*args, **kw):
    count = 0
    for row in csv_2_gen(*args, **kw):
        print row
        count += 1
        if count % 1000 == 0:
            print count
            break

