import argparse
import os.path

import config
import definitions
import sa_util
import csv_util


def import_module(args):
    from dependencies import dependencies_manager
    tables = []
    for module in args.module:
        if not args.updateonly:
            definitions.get_importer(module)(verbose=args.verbose)
        tables += definitions.get_tables(module)
    deps = dependencies_manager.updates_for(tables, include=False)
    if not args.noupdate:
        sa_util.build_views_and_summaries(
            items=deps,
            verbose=args.verbose,
        )


def build_views_summaries(args):
    sa_util.build_views_and_summaries(
        items=args.module,
        all=args.all,
        verbose=args.verbose,
        force=args.force,
        dependencies=not args.no_dependants,
    )


def sql(args):
    from definitions import get_definition
    for item in args.items:
        print '\n\nSQL for %s\n' % item
        info = get_definition(item)
        sql = info.get('sql')
        tables = info.get('tables')
        ts = {}
        for index, table in enumerate(tables):
            ts['t%s' % (index + 1)] = '"%s"' % table
        print sql.format(**ts)


def deps(args):
    from dependencies import dependencies_manager
    for item in args.items:
        print 'Dependencies for %s' % item
        print dependencies_manager.get_needed_updates(item)


def clear_views(args):
    for view in sa_util.view_list():
        sa_util.drop_table_or_view(view, force=True)


def recreate_views(args):
    from dependencies import dependencies_manager
    views = definitions.defined_views()
    existing_views = sa_util.view_list()
    updates = dependencies_manager.updates_for(views)
    needed = []
    for update in updates:
        if update in views:
            if update in existing_views:
                continue
            needed.append(update)
    needed.reverse()
    print needed
    sa_util.build_views_and_summaries(
        items=needed,
        dependencies=False,
        verbose=args.verbose,
    )
    sa_util.swap_tables(verbose=args.verbose)


def clean_db(args):
    sa_util.clear_temp_objects(verbose=args.verbose)
    tables = sorted(list(
        set(sa_util.table_view_list())
        - set(definitions.defined_tables())
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
    csv_util.import_single(
        filename,
        tablename,
        encoding=args.encoding,
        delimiter=delimiter,
        verbose=verbose
    )
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
        'clear_views',
        'recreate_views',
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
    swap_temp_parser.add_argument('-f', '--force', action="store_true")

    module_commands = [
        'import',
        'summaries',
    ]

    for command in module_commands:
        module_parser = subparsers.add_parser(command)
        module_parser.add_argument('-f', '--force', action="store_true")
        module_parser.add_argument('-d', '--no-dependants', action="store_true")
        module_parser.add_argument('-a', '--all', action="store_true")
        module_parser.add_argument('-t', '--test', action="store_true")
        module_parser.add_argument('-n', '--noupdate', action="store_true")
        module_parser.add_argument('-u', '--updateonly', action="store_true")
        module_parser.add_argument('-s', '--stage', default=0, type=int)
        module_parser.add_argument('module', nargs='*')

    dep_parser = subparsers.add_parser('deps')
    dep_parser.add_argument('items', nargs='*')

    dep_parser = subparsers.add_parser('sql')
    dep_parser.add_argument('items', nargs='*')

    args = parser.parse_args()
    if args.command == 'deps':
        deps(args)
    if args.command == 'sql':
        sql(args)
    if args.command == 'export_all':
        export_all(verbose=args.verbose)
    if args.command == 'export_all':
        export_all(verbose=args.verbose)
    elif args.command == 'import':
        import_module(args)
        sa_util.swap_tables(verbose=args.verbose)
    elif args.command == 'swap_temp':
        sa_util.swap_tables(verbose=args.verbose, force=args.force)
    elif args.command == 'summaries':
        build_views_summaries(args)
        if not args.noupdate:
            sa_util.swap_tables(verbose=args.verbose, force=args.force)
    elif args.command == 'export_custom':
        export_custom(verbose=args.verbose)
    elif args.command == 'import_csv':
        import_csv(args)
    elif args.command == 'web':
        webserver(args)
    elif args.command == 'clean_db':
        clean_db(args)
    elif args.command == 'clear_views':
        clear_views(args)
    elif args.command == 'recreate_views':
        recreate_views(args)
    elif args.command == 'db_functions':
        db_functions(verbose=args.verbose)
