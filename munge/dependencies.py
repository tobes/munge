from definitions import defined_tables, defined_dependencies


class DependenciesManager(object):

    def __init__(self):
        self.get_dependencies()

    def sort_deps(self, deps):
        deps = list(deps)
        deps.sort(key=lambda x: self.items.index(x))
        return deps

    def get_needed_updates(self, item):
        return self.needs.get(item) or []

    def updates_for(self, items, include=True):
        updates = set()
        for item in items:
            if include:
                updates.add(item)
            updates |= set(self.get_needed_updates(item))
        return self.sort_deps(updates)

    def get_dependencies(self):

        deps = defined_dependencies()

        items = set(deps.keys())  # name of each table/view

        def sort_dependencies(deps_dict):
            # order dependencies for each view/table
            for d in deps_dict:
                x = list(deps_dict[d])
                x.sort(key=lambda x: items.index(x))
                deps_dict[d] = x

        def cmp_dependency(x, y):
            d = deps.get(y, [])
            if x in d or len(d) > len(deps.get(x, [])):
                return False
            return True

        def insertion_sort(items):
            """ Implementation of insertion sort """
            for i in range(1, len(items)):
                j = i
                while j > 0 and not cmp_dependency(items[j], items[j-1]):
                    items[j], items[j-1] = items[j-1], items[j]
                    j -= 1

        # add the dependencies of all dependencies
        for d in deps:
            x = deps.get(d, set()).copy()
            for y in deps[d]:
                x |= deps.get(y, set())
                items |= x
            deps[d] = x

        # make item list so we can order it
        items = list(items)
        # order dependencies list
        insertion_sort(items)

        sort_dependencies(deps)

        reverse_deps = {}
        for key in items:
            for value in deps.get(key, []):
                if value not in reverse_deps:
                    reverse_deps[value] = set()
                reverse_deps[value].add(key)

        imported_tables = set(defined_tables(include_created=False))
        # add the dependencies of all reverse dependencies
        revs = {}
        for d in reverse_deps:
            x = reverse_deps[d].copy()
            for y in reverse_deps[d]:
                deps_list = deps.get(y, [])
                idx = items.index(d)
                # only get dependencies if they are later ones
                deps_list = [a for a in deps_list if items.index(a) < idx]
                x |= set(deps_list)
            revs[d] = x - imported_tables

        sort_dependencies(revs)

        # sort needs
        needs = {}
        for item in revs:
            needs[item] = []
            for k, v in deps.items():
                if item in v:
                    needs[item].append(k)


        self.revs = revs
        self.needs = needs
        self.deps = deps
        self.items = items

dependencies_manager = DependenciesManager()
