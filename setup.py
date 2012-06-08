from distutils.core import setup

setup(
    name='OplogReplay',
    version='0.1.01',
    author='Mihnea Giurgea',
    author_email='GiurgeaMihnea@gmail.com',
    packages=['oplogreplay', 'oplogreplay.test'],
    scripts=['bin/oplogreplay'],
    url='http://pypi.python.org/pypi/OplogReplay/',
    license='LICENSE.txt',
    description='MongoDB oplog replay utility.',
    long_description=open('README.txt').read(),
    install_requires=[
        "pymongo == 2.1.1"
    ],
)