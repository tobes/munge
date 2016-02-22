from definitions import defined_tables, defined_dependencies


class DependenciesManager(object):

    def __init__(self):
        self.get_dependencies()

    def sort_deps(self, deps):
        deps = list(deps)
        deps.sort(key=lambda x: len(self.deps_ordered) - self.deps_ordered.index(x))
        return deps

    def get_needed_updates(self, item):
        return self.sort_deps(self.deps_full.get(item) or [])

    def updates_for(self, items, include=True):
        updates = set()
        for item in items:
            if include:
                updates.add(item)
            updates |= set(self.get_needed_updates(item))
        updates = self.sort_deps(updates)
        return updates

    def get_dependencies(self):

        def cmp_dependency(x, y):
            d = deps_full.get(y, [])
            if x in d:
                return True
            return False

        def sort_bubblesort(my_list):

            for pos_upper in xrange(len(my_list) - 1, 0, -1):
                for i in xrange(pos_upper):
                    if cmp_dependency(my_list[i], my_list[i + 1]):
                        my_list[i], my_list[i + 1] = my_list[i + 1], my_list[i]
            return my_list

        deps = defined_dependencies()
        deps_partial = {}
        for k, v in deps.items():
            if k not in deps_partial:
                deps_partial[k] = set()
            for i in v:
                if i not in deps_partial:
                    deps_partial[i] = set()
                deps_partial[i].add(k)
        deps_full = {}
        for k, v in deps_partial.items():
            deps_full[k] = set(v)
            for i in v:
                deps_full[k] |= deps_partial.get(i, set())

        # make item list so we can order it
        deps_ordered = list(deps_full)
        deps = deps_full
        # order dependencies list
        sort_bubblesort(deps_ordered)

        self.deps_ordered = deps_ordered
        self.deps_full = deps_full

dependencies_manager = DependenciesManager()
