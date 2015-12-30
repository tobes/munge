import argparse


def export_all(verbose=False):
    if verbose:
        print('Exporting all tables')
    from csv_util import dump_all
    dump_all(verbose=verbose)


def export_custom(verbose=False):
    if verbose:
        print('Exporting custom tables')
    import custom_output


def import_all(verbose=False):
    if verbose:
        print('Importing all tables')
    from csv_util import import_all
    import_all(verbose=verbose)


def postcode(verbose=False):
    if verbose:
        print('Importing postcodes')
    from postcode_import import import_postcodes
    import_postcodes(verbose=verbose)


def import_csv(args):
    verbose = args.v
    delimiter = args.delimiter
    if delimiter == '\\t':
        delimiter = '\t'
    if verbose:
        print('Importing %s' % args.filename)
    from csv_util import import_single, swap_tables
    import_single(args.filename, args.tablename, encoding=args.encoding,
                  delimiter=delimiter, verbose=verbose)
    swap_tables(verbose=verbose)


def webserver(verbose=False):
    from munge.app import app
    app.run(debug=True)


def vao(verbose=False):
    from vao_importers import build_summaries, build_views, swap_tables
    build_views(verbose=verbose)
    build_summaries(verbose=verbose)
    swap_tables(verbose=verbose)


def vao_full(verbose=False):
    from vao_importers import import_vao_full
    import_vao_full(verbose)


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
        'import_all',
        'postcode',
        'web',
        'vao',
        'vao_full',
        'clean_db',
    ]

    parser = argparse.ArgumentParser(
        description='Command line interface for munge'
    )
    parser.add_argument("-v", help="verbose output", action='store_true')

    subparsers = parser.add_subparsers(help='commands', dest='command')

    for command in commands:
        subparsers.add_parser(command)

    import_csv_parser = subparsers.add_parser('import_csv')
    import_csv_parser.add_argument("--encoding", default='utf-8')
    import_csv_parser.add_argument("--delimiter", default=',')
    import_csv_parser.add_argument('tablename')
    import_csv_parser.add_argument('filename')

    args = parser.parse_args()
    if args.command == 'export_all':
        export_all(verbose=args.v)
    elif args.command == 'import_all':
        import_all(verbose=args.v)
    elif args.command == 'postcode':
        postcode(verbose=args.v)
    elif args.command == 'export_custom':
        export_custom(verbose=args.v)
    elif args.command == 'import_csv':
        import_csv(args)
    elif args.command == 'web':
        webserver(verbose=args.v)
    elif args.command == 'vao':
        vao(verbose=args.v)
    elif args.command == 'vao_full':
        vao_full(verbose=args.v)
    elif args.command == 'clean_db':
        clean_db(verbose=args.v)
