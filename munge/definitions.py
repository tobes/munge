_CACHE = {}
initialized = False


def init():
    _CACHE.clear()
    import importers
    for module in importers.__all__:
        m = getattr(importers, module)
        importer = getattr(m, 'IMPORTER', None)
        auto_sql = getattr(m, 'AUTO_SQL', {})
        tables_fn = getattr(m, 'tables', None)
        tables = tables_fn() if tables_fn else []
        definition = {
            'name': importer,
            'module_name': module,
            'module': m,
            'auto_sql': auto_sql,
            'importer_function': m.importer,
            'tables': tables,
        }
        _CACHE[module] = definition
    global initialized
    initialized = True


def get_tables(module):
    if not initialized:
        init()
    return _CACHE[module]['tables']


def get_importer(module):
    if not initialized:
        init()
    return _CACHE[module]['importer_function']


def get_module_def(module):
    if not initialized:
        init()
    return _CACHE[module]


def defined_tables(include_created=True):
    if not initialized:
        init()
    tables = []
    for item in _CACHE.values():
        tables += item['tables']
        if include_created:
            for info in item['auto_sql']:
                tables.append(info['name'])
    return tables


def get_definition(name):
    if not initialized:
        init()
    for item in _CACHE.values():
        for info in item['auto_sql']:
            if info['name'] == name:
                return info


def defined_dependencies(disabled=False):
    if not initialized:
        init()
    deps = {}
    for item in _CACHE.values():
        for info in item['auto_sql']:
            if disabled or not info.get('disabled', False):
                dependencies = set(info['tables'])
                dependencies |= set(info.get('dependencies', []))
                deps[info['name']] = dependencies
    return deps
