from setuptools import setup

setup(
    name='munge',
    version='0.1',
    description='pikhaya',
    url='http://github.com/tobes/munge',
    author='Toby Dacre',
    author_email='toby.dacre@whythawk.co.uk',
    license='AGPL',
    packages=['munge'],
    entry_points={
        'console_scripts': ['munge=munge.cli:main'],
    },
    zip_safe=False,
)
