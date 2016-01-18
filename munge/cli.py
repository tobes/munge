import argparse
import os.path

import config
import importers
import sa_util
import csv_util


def import_module(args):
    for module in args.module:
        m = getattr(importers, module)
        m.importer(verbose=args.verbose)
    view_summaries(args)
    sa_util.swap_tables(verbose=args.verbose)


def view_summaries(args, just_views=False):
    for module in args.module:
        m = getattr(importers, module)
        data = getattr(m, 'AUTO_SQL')
        if data:
            sa_util.build_views_and_summaries(
                data, verbose=args.verbose, just_views=just_views
            )


def defined_tables():
    tables = []
    for module in importers.__all__:
        m = getattr(importers, module)
        tables_fn = getattr(m, 'tables', None)
        if tables_fn:
            tables += tables_fn()
        info = []
        info += getattr(m, 'VIEWS_DATA', None) or []
        info += getattr(m, 'SUMMARIES_DATA', None) or []
        for item in info:
            tables.append(item['name'])
    return tables


def clean_db(args):
    sa_util.clear_temp_objects(verbose=args.verbose)
    tables = sorted(list(
        set(sa_util.table_view_list())
        - set(defined_tables())
        - set(sa_util.dependent_objects())
    ))
    print 'Unknown tables'
    for table in tables:
        print '\t%s' % table
    for table in tables:
        response = raw_input('Delete table `%s` [No/yes/quit]:' % table)
        if response and response.upper()[0] == 'Y':
            sa_util.drop_table_or_view(table, verbose=args.verbose)
        if response and response.upper()[0] == 'Q':
            return


def export_all(verbose=False):
    if verbose:
        print('Exporting all tables')
    csv_util.dump_all(verbose=verbose)


def export_custom(verbose=False):
    if verbose:
        print('Exporting custom tables')
    import custom_output


def db_functions(verbose=False):
    if verbose:
        print('Creating db functions')
    import postgres_functions


def import_csv(args):
    verbose = args.verbose
    filename = args.filename
    tablename = args.tablename
    delimiter = args.delimiter
    filename = os.path.join(config.DATA_PATH, 'import', filename)
    if delimiter == '\\t':
        delimiter = '\t'
    if not tablename:
        tablename = os.path.splitext(os.path.basename(filename))[0]
    if verbose:
        print('Importing %s' % args.filename)
    csv_util.import_single(filename, tablename, encoding=args.encoding,
                  delimiter=delimiter, verbose=verbose)
    sa_util.swap_tables(verbose=verbose)


def webserver(args):
    from munge.app import app
    app.run(debug=True)


def main():

    commands = [
        'export_all',
        'export_custom',
        'web',
        'clean_db',
        'db_functions',
    ]

    parser = argparse.ArgumentParser(
        description='Command line interface for munge'
    )
    parser.add_argument('-v', '--verbose', action='count', default=0)
    subparsers = parser.add_subparsers(help='commands', dest='command')

    for command in commands:
        subparsers.add_parser(command)

    import_csv_parser = subparsers.add_parser('import_csv')
    import_csv_parser.add_argument("--encoding", default='utf-8')
    import_csv_parser.add_argument("--delimiter", default=',')
    import_csv_parser.add_argument('--tablename', default=None)
    import_csv_parser.add_argument('filename')

    swap_temp_parser = subparsers.add_parser('swap_temp')
    swap_temp_parser.add_argument('-f', '--force', default=False)

    module_commands = [
        'import',
        'views',
        'summaries',
    ]

    for command in module_commands:
        module_parser = subparsers.add_parser(command)
        module_parser.add_argument('module', nargs='*')

    args = parser.parse_args()
    if args.command == 'export_all':
        export_all(verbose=args.verbose)
    elif args.command == 'import':
        import_module(args)
    elif args.command == 'views':
        view_summaries(args, just_views=True)
        sa_util.swap_tables(verbose=args.verbose)
    elif args.command == 'swap_temp':
        sa_util.swap_tables(verbose=args.verbose, force=args.force)
    elif args.command == 'summaries':
        view_summaries(args)
        sa_util.swap_tables(verbose=args.verbose)
    elif args.command == 'export_custom':
        export_custom(verbose=args.verbose)
    elif args.command == 'import_csv':
        import_csv(args)
    elif args.command == 'web':
        webserver(args)
    elif args.command == 'clean_db':
        clean_db(args)
    elif args.command == 'db_functions':
        db_functions(verbose=args.verbose)
