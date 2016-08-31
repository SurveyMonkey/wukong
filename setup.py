import os
from setuptools import setup, find_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

version = '0.0.1'


setup(
    name="wukong",
    version=version,
    author="Da Kuang",
    author_email="dak@surveymonkey.com",
    maintainer="Da Kuang, Thomas Knickman, Joel Marcotte, Tyler Lubeck",
    maintainer_email="dak@surveymonkey.com, thomask@surveymonkey.com, joelm@surveymonkey.com, tylerl@surveymonkey.com",
    description="An ORM Client library for SolrCloud",
    keywords="solr cloud solrcloud client python orm",
    url="https://github.com/SurveyMonkey/wukong",
    long_description=read('README.md'),
    install_requires=[
        "requests",
        "kazoo",
        "six>=1.6.1"
    ],
    tests_require=read('test-requirements.txt'),
    packages=find_packages(),
    classifiers=[
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
    ],
)
