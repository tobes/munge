import argparse
import os.path

import config
import importers
from sa_util import swap_tables, build_views, build_summaries


def import_module(args):
    for module in args.module:
        m = getattr(importers, module)
        m.importer(verbose=args.verbose)
    views(args)
    summeries(args)
    swap_tables(verbose=args.verbose)


def views(args):
    for module in args.module:
        m = getattr(importers, module)
        data = getattr(m, 'VIEWS_DATA')
        if data:
            build_views(data, verbose=args.verbose)


def summeries(args):
    for module in args.module:
        m = getattr(importers, module)
        data = getattr(m, 'SUMMARIES_DATA')
        if data:
            build_summaries(data, verbose=args.verbose)
        m = getattr(importers, module)
        m.importer(verbose=verbose)
    swap_tables()


def export_all(verbose=False):
    if verbose:
        print('Exporting all tables')
    from csv_util import dump_all
    dump_all(verbose=verbose)


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
    from csv_util import import_single, swap_tables
    import_single(filename, tablename, encoding=args.encoding,
                  delimiter=delimiter, verbose=verbose)
    swap_tables(verbose=verbose)


def webserver(verbose=False):
    from munge.app import app
    app.run(debug=True)


def clean_db(verbose=False):
    from sa_util import clear_temp_objects
    from csv_util import import_drop_code_tables, import_drop_lookup_tables
    import_drop_code_tables(verbose=verbose)
    import_drop_lookup_tables(verbose=verbose)
    clear_temp_objects(verbose=verbose)


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

    module_commands = [
        'import',
        'views',
        'summeries',
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
        views(args)
        swap_tables(verbose=args.verbose)
    elif args.command == 'summeries':
        summeries(args)
        swap_tables(verbose=args.verbose)
    elif args.command == 'export_custom':
        export_custom(verbose=args.verbose)
    elif args.command == 'import_csv':
        import_csv(args)
    elif args.command == 'web':
        webserver(verbose=args.verbose)
    elif args.command == 'clean_db':
        clean_db(verbose=args.verbose)
    elif args.command == 'db_functions':
        db_functions(verbose=args.verbose)
