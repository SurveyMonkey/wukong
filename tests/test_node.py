import mock
from mock import PropertyMock
from wukong.query import *
from wukong.errors import *
import pytest

from parameterizedtestcase import ParameterizedTestCase

try:
    import unittest2 as unittest
except ImportError:
    import unittest

class TestNode(ParameterizedTestCase):
    def test_node_comp_node_init(self):
        node = Comparator("eq", "name", "Test Name")

        assert node.operator == "eq"

        assert node.key == "name"

        assert node.value == "Test Name"

    @ParameterizedTestCase.parameterize(
        ('operator', 'key', 'value', 'expected_query', 'expected_error'),
        (
            ("eq", "name", "Test Name", "name:\"Test Name\"", None),
            ("eq", "name", 100, "name:100", None),
            ("eq", "name", None, "-name:[* TO *]", None),
            ("in", "name", ["Test1", "Test2"], "name:(\"Test1\" \"Test2\")", None),
            ("in", "name", [1, 2], "name:(1 2)", None),
            ("in", "name", ["Test1", None], "name:(\"Test1\")", None),
            ("in", "name", "Test1", None, "Operator:in only takes list type"),
            ("in", "name", 1, None, "Operator:in only takes list type"),
            ("ne", "name", "Test Name", "-name:\"Test Name\"", None),
            ("ne", "name", 100, "-name:100", None),
            ("ne", "name", None, "name:*", None),
            ("wc", "name", "Test Name", "name:*Test Name*", None),
            ("wc", "name", "Test*", "name:Test*", None),
            ("wc", "name", "*Test", "name:*Test", None),
            ("wc", "name", None, "name:*", None),
            ("ge", "name", 100, "name:[100 TO *]", None),
            ("ge", "name", None, "name:[* TO *]", None),
            ("le", "name", 100, "name:[* TO 100]", None),
            ("le", "name", None, "name:[* TO *]", None),
            ("op", "name", "Test", None, "The operator:op is not supported"),
            ("op", None, "Test", "", None)
        )
    )
    def test_node_comp_node_parsed_solr_query(self, operator, key, value,
        expected_query, expected_error):
        node = Comparator(operator, key, value)

        try:
            parsed_query = node.parsed_solr_query
        except SolrError as e:
            if expected_error:
                assert str(e) == expected_error
        else:
            assert parsed_query == expected_query

    @ParameterizedTestCase.parameterize(
        ('args', 'kwargs', 'expected_query', 'expected_error'),
        (
            (AND, [], {"name__eq":"Test Name","city__eq":"Test City"},
            ["(name:\"Test Name\" AND city:\"Test City\")",
            "(city:\"Test City\" AND name:\"Test Name\")"], None),
            (AND, [], {"name__nin":["name1", "name2"],"city__eq":"Test City"},
             ['(city:"Test City" AND -name:("name1" "name2"))',
              '(-name:("name1" "name2") AND city:"Test City")'],
             None),
            (AND, [], {"name__nin":["name1", 2],"city__eq":"Test City"},
             ['(city:"Test City" AND -name:("name1" 2))',
              '(-name:("name1" 2) AND city:"Test City")'],
             None),
            (AND, [], {"name__nin":"name1", "city__eq":"Test City"},
             ['(city:"Test City" AND -name:("name1"))'],
             'Operator:not in only takes list type'),
            (AND, [], {"name__in":["name1", "name2"],"city__eq":"Test City"},
             ['(city:"Test City" AND name:("name1" "name2"))',
              '(name:("name1" "name2") AND city:"Test City")'],
             None),
            (AND, [], {"name__ex":"test","city__eq":"Test City"},
             ['(city:"Test City" AND name:[* TO *])',
              '(name:[* TO *] AND city:"Test City")'],
             None),
            (AND, [], {"name__nex":"test","city__eq":"Test City"},
             ['(-name:[* TO *] AND city:"Test City")',
              '(city:"Test City" AND -name:[* TO *])'],
             None),
            (AND, [], {"name_eq":"Test Name"},
            None, "The operator is not specified in name_eq"),
            (AND, [Comparator("eq","name", "Test Name")],{},
            ["name:\"Test Name\""], None),
            (AND, [Comparator("le","size", 999)],{},
            ["size:[* TO 999]"], None),
            (AND, [AND(name__eq="Test Name")],
            {"city__in":["Test City1", "Test City2"]},
            ["(name:\"Test Name\" AND city:(\"Test City1\" \"Test City2\"))",
            "(city:(Test City1,Test City2) AND name:\"Test Name\")"], None),
            (OR, [], {"name__eq":"Test Name","city__ge":1},
            ["(name:\"Test Name\" OR city:[1 TO *])",
            "(city:[1 TO *] OR name:\"Test Name\")"], None),
            (OR, [], {"testkey":"Test Name"},
            None, "The operator is not specified in testkey"),
            (OR, [Comparator("eq","size", 500)],{},
            ["size:500"], None),
            (OR, [Comparator("wc","name", "Test Name")],{},
            ["name:*Test Name*"], None),
            (OR, [OR(name__eq="Test Name")], {"city__wc":"*City"},
            ["(name:\"Test Name\" OR city:*City)",
            "(city:*City OR name:\"Test Name\")"], None),
            (AND, [NOT(OR(name__eq="Test Name", size__ge=100))],
            {"city__eq":"Test City"},
            ["(!((name:\"Test Name\" OR size:[100 TO *])) AND city:\"Test City\")",
            "(!((size:[100 TO *] OR name:\"Test Name\")) AND city:\"Test City\")",
            "(city:\"Test City\" AND !((name:\"Test Name\" OR size:[100 TO *])))",
            "(city:\"Test City\" AND !((size:[100 TO *] OR name:\"Test Name\")))"],
            None),
            (OR, [NOT(AND(name__eq="Test Name", size__ge=100))],
            {"city__eq":"Test City"},
            ["(!((name:\"Test Name\" AND size:[100 TO *])) OR city:\"Test City\")",
            "(!((size:[100 TO *] AND name:\"Test Name\")) OR city:\"Test City\")",
            "(city:\"Test City\" OR !((name:\"Test Name\" AND size:[100 TO *])))",
            "(city:\"Test City\" OR !((size:[100 TO *] AND name:\"Test Name\")))"],
            None)
        )
    )
    def test_node_and_or_node_parsed_solr_query(self, node_class, args, kwargs,
        expected_queries, expected_error):
        try:
            node = node_class(*args, **kwargs)
            parsed_query = node.parsed_solr_query
        except SolrError as e:
            if expected_error:
                assert str(e) == expected_error
        else:
            assert parsed_query in expected_queries

    @ParameterizedTestCase.parameterize(
        ('args', 'kwargs', 'expected_query', 'expected_error'),
        (
            ([], {}, [""], None),
            ([Comparator("eq","name", "Test Name")],{},
            ["!(name:\"Test Name\")"], None),
             ([], {"name__eq":"Test Name","city__eq":"Test City"},
            None, "Logic node:NOT only supports one operand"),
            ([Comparator("eq","name", "Test Name")], {"name__eq":"Test Name"},
            None, "Logic node:NOT only supports one operand"),
            ([Comparator("eq","name", "Test Name"),Comparator("eq","name", "Test Name")], {},
            None, "Logic node:NOT only supports one operand"),
            ([AND(name__eq="Test Name",city__in=["Test City1", "Test City2"])],{},
            ["!((name:\"Test Name\" AND city:(\"Test City1\" \"Test City2\")))",
            "!((city:(\"Test City1\" \"Test City2\") AND name:\"Test Name\"))"], None),
            ([OR(name__eq="Test Name",city__in=["Test City1", "Test City2"])],{},
            ["!((name:\"Test Name\" OR city:(\"Test City1\" \"Test City2\")))",
            "!((city:(\"Test City1\" \"Test City2\") OR name:\"Test Name\"))"], None),
        )
    )
    def test_node_not_node_parsed_solr_query(self, args, kwargs,
        expected_queries, expected_error):
        try:
            node = NOT(*args, **kwargs)
            parsed_query = node.parsed_solr_query
        except SolrError as e:
            if expected_error:
                assert str(e) == expected_error
        else:
            assert parsed_query in expected_queries

    @ParameterizedTestCase.parameterize(
        ('key', 'operator', 'value','expected_query', 'expected_error'),
        (
            (None, None, "Test Text", "", None),
            ("name", "eq", "Test Text", "name:\"Test Text\"", None),
            ("name", "eq", None, "-name:[* TO *]", None),
            ("name", "eq", 100, "name:100", None),
            ("name", "in", ["a","b"], "name:(\"a\" \"b\")", None),
            ("name", "in", [1,2], "name:(1 2)", None),
            ("name", "in", ["a",None], "name:(\"a\")", None),
            ("name", "in", "not list", None, "Operator:in only takes list type"),
            ("name", "ne", "Test Text", "-name:\"Test Text\"", None),
            ("name", "ne", None, "name:*", None),
            ("name", "ne", 100, "-name:100", None),
            ("name", "wc", "test", "name:*test*", None),
            ("name", "wc", "test*", "name:test*", None),
            ("name", "wc", "*test", "name:*test", None),
            ("name", "wc", None, "name:*", None),
            ("name", "nwc", "test*", "-name:test*", None),
            ("name", "nwc", "test", "-name:*test*", None),
            ("name", "wc", "", "name:*", None),
            ("name", "ge", 100, "name:[100 TO *]", None),
            ("name", "ge", "a", "name:[a TO *]", None),
            ("name", "ge", None, "name:[* TO *]", None),
            ("name", "ge", "", "name:[* TO *]", None),
            ("name", "le", 100, "name:[* TO 100]", None),
            ("name", "le", "a", "name:[* TO a]", None),
            ("name", "le", None, "name:[* TO *]", None),
            ("name", "le", "", "name:[* TO *]", None),
            ("size", "l", 10, "size:[* TO 10}", None),
            ("size", "le", 10, "size:[* TO 10]", None),
            ("size", "g", 10, "size:{10 TO *]", None),
            ("size", "ge", 10, "size:[10 TO *]", None),
        )
    )
    def test_node_comparator_parsed_solr_query(self, key, operator,
        value, expected_query, expected_error):
        try:
            node = Comparator(operator, key, value)
            parsed_query = node.parsed_solr_query
        except SolrError as e:
            if expected_error:
                assert str(e) == expected_error
        else:
            assert parsed_query == expected_query
