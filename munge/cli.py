import argparse


def export_all(verbose=False):
    if verbose:
        print 'Exporting all tables'
    from csv_util import dump_all
    dump_all(verbose=verbose)


def import_all(verbose=False):
    if verbose:
        print 'Exporting all tables'
    from csv_util import import_all
    import_all(verbose=verbose)


def webserver(verbose=False):
    from munge.app import app
    app.run(debug=True)


def vao(verbose=False):
    from vao_importers import import_vao_full
    import_vao_full(verbose)


def main():
    parser = argparse.ArgumentParser(
        description='Command line interface for munge'
    )
    parser.add_argument("-v", help="verbose output", action='store_true')
    parser.add_argument("command", type=str, help="command to run")
    parser.add_argument('vars', default=None, nargs='*')
    args = parser.parse_args()
    if args.command == 'export_all':
        export_all(verbose=args.v)
    elif args.command == 'import_all':
        import_all(verbose=args.v)
    elif args.command == 'web':
        webserver(verbose=args.v)
    elif args.command == 'vao':
        vao(verbose=args.v)
