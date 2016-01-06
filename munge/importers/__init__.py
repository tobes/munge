import os

__all__ = []

for module in os.listdir(os.path.dirname(__file__)):
    if module == '__init__.py' or module[-3:] != '.py':
        continue
    name = module[:-3]
    __import__(name, locals(), globals())
    __all__.append(name)

del module
del name
del os
