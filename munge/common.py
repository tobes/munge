import re


def process_header(row):
    fields = []
    pk = None
    for col in row:
        # null ignored fields
        if col == '':
            col = {'name': '', 'type': None}
            fields.append(col)
            continue
        # defaults
        pk = False
        index = False
        index_key = None
        missing = False
        fn = None
        fn_field = None
        # field datatype
        if ':' in col:
            field, type_ = col.split(':')
        else:
            field = col
            type_ = 'text'
        # ignored fields
        if col[0] == '-':
            field = field[1:]
            type_ = None
        # primary key
        if field[0] == '*':
            field = field[1:]
            pk = True
        # index
        if field[0] == '+':
            field = field[1:]
            index = True
            reg_ex = '\{(\d+)\}'
            m = re.match(reg_ex, field)
            if m:
                index_key = m.group(1)
                field = re.sub(reg_ex, '', field)
        # field not supplied in data
        if field[0] == '@':
            field = field[1:]
            missing = True
        # conversion function
        if type_ and '~' in type_:
            type_, fn = type_.split('~')
            if '|' in fn:
                fn, fn_field = fn.split('|')

        col = {
            'name': field,
            'type': type_,
            'pk': pk,
            'index': index,
            'index_key': index_key,
            'fn': fn,
            'fn_field': fn_field,
            'missing': missing,
        }
        fields.append(col)
    return fields


