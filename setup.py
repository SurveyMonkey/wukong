import os
from setuptools import setup, find_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

try:
    import pypandoc
    README = pypandoc.convert('README.md', 'rst')
    CHANGES = pypandoc.convert('CHANGES.md', 'rst')
except:
    README = read('README.md')
    CHANGES = read('CHANGES.md')

version = '1.1.0'

setup(
    name="wukong",
    version=version,
    author="Da Kuang",
    author_email="dkuang1980@gmail.com",
    description="An ORM Client library for SolrCloud",
    keywords="solr cloud solrcloud client python orm",
    url="https://github.com/SurveyMonkey/wukong",
    long_description_content_type="text/markdown",
    long_description='%s\n\n%s' % (README, CHANGES),
    install_requires=[
        "requests",
        "kazoo",
        "six>=1.6.1"
    ],
    tests_require=read('test-requirements.txt'),
    packages=find_packages(exclude=['tests']),
    classifiers=[
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8"
    ],
)
