import datetime

import sa_util

table_fields = [
    '*name',
    'description',
    'created:timestamp',
    'updated:timestamp',
]

_initiated = False


def _init():
    sa_util.create_table('tables',
                         sa_util.process_header(table_fields),
                         keep=True)
    global _initiated
    _initiated = True


def update_summary_table(table_name, description, created=False):
    if not _initiated:
        _init()
    # set the table name
    data = {}
    data['name'] = table_name
    data['description'] = description
    data['updated'] = datetime.datetime.now()
    if created:
        data['created'] = datetime.datetime.now()
    # does the entry exist?
    exists = False
    sql = 'SELECT * FROM tables WHERE name=:name'
    results = sa_util.run_sql(sql, {'name': table_name})
    for result in results:
        exists = True
    columns = ', '.join([sa_util.quote(x) for x in data.keys()])
    values = ', '.join([':%s' % x for x in data.keys()])
    if exists:
        sql = "UPDATE tables SET ({columns}) = ({values}) WHERE name=:name"
    else:
        sql = "INSERT INTO tables ({columns}) VALUES ({values})"

    sql = sql.format(columns=columns, values=values)
    sa_util.run_sql(sql, **data)
