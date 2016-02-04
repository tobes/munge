from munge.csv_util import import_all, csv_table_list


DIRECTORY = 'codes'
IMPORTER = 'codes'


def importer(verbose=0):
    import_all(DIRECTORY, verbose=verbose, keep_table=True, importer=IMPORTER)


def tables():
    return csv_table_list(DIRECTORY)
