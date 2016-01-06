from munge.csv_util import import_all

DIRECTORY = 'codes'


def importer(verbose=0):
    import_all(DIRECTORY, verbose=verbose)
