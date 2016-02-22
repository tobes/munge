import datetime

import sa_util


table_fields = [
    '*name',
    'view:boolean',
    'description',
    'created:timestamp',
    'updated:timestamp',
    'dependencies:text[]',
    'importer',
    'time',
    'rows:bigint',
]

_initiated = False


def _init():
    sa_util.create_table('table_summaries',
                         sa_util.process_header(table_fields),
                         keep=True)
    global _initiated
    _initiated = True


def update_summary_table(table_name,
                         description,
                         dependencies=None,
                         is_view=False,
                         created=False,
                         time=None,
                         rows=None,
                         importer=None):
    if not _initiated:
        _init()
    data = {
        'name': table_name,
        'view': is_view,
        'importer': importer,
    }
    data['dependencies'] = dependencies
    if description:
        data['description'] = description
    data['updated'] = datetime.datetime.now()
    if created:
        data['created'] = datetime.datetime.now()
    if time:
        data['time'] = time
    if rows:
        data['rows'] = rows
    # does the entry exist?
    exists = False
    sql = 'SELECT * FROM table_summaries WHERE name=:name'
    results = sa_util.run_sql(sql, {'name': table_name})
    for result in results:
        exists = True
    columns = ', '.join([sa_util.quote(x) for x in data.keys()])
    values = ', '.join([':%s' % x for x in data.keys()])
    if exists:
        sql = "UPDATE table_summaries SET ({columns}) = ({values}) WHERE name=:name"
    else:
        sql = "INSERT INTO tables ({columns}) VALUES ({values})"

    sql = sql.format(columns=columns, values=values)
    sa_util.run_sql(sql, **data)
