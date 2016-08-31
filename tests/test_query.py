import mock
from mock import PropertyMock
from wukong.query import *
from wukong.models import *
from wukong.errors import *
import pytest

from parameterizedtestcase import ParameterizedTestCase

try:
    import unittest2 as unittest
except ImportError:
    import unittest


class FakeSolrDoc(SolrDoc):
    collection_name = "fake_collection"
    solr_hosts = "fake_host"

class TestQuery(unittest.TestCase):
    def test_query_init__normal(self):
    	node = OR(name__eq="Test Name")
        qm = SolrQueryManager(
        	doc_class=FakeSolrDoc,
        	node=node,
        	weight_dict={"name":5},
        	returned_fields=["name"],
        	edismax=True,
        	rows=10,
            start=5,
        	sort_str="-entity_id"
        )

        assert qm.doc_class == FakeSolrDoc
        assert qm.node == node
        assert qm.weight_dict == {"name":5}
        assert qm.returned_fields == ["name"]
        assert qm.edismax == True
        assert qm.rows == 10
        assert qm.sort_str == "-entity_id"

    def test_query_init__missing_args(self):
        qm = SolrQueryManager(FakeSolrDoc)

        assert qm.doc_class == FakeSolrDoc
        assert isinstance(qm.node, AND)
        assert qm.weight_dict is None
        assert qm.returned_fields is None
        assert qm.edismax == False
        assert qm.rows == INFINITE_ROWS
        assert qm.start == 0
        assert qm.sort_str == None

    def test_query_order_by(self):
    	qm = SolrQueryManager(FakeSolrDoc)

    	qm = qm.sort_by("-test_name")

    	assert qm.sort_str == "-test_name"

    def test_query_search(self):
    	qm = SolrQueryManager(FakeSolrDoc)

    	qm = qm.search("Test", name=10, city=5)

    	assert qm.weight_dict == {"name":10, "city":5}
        assert qm.text_keywords == 'Test'
        assert qm.edismax

    def test_query_only_normal(self):
        qm = SolrQueryManager(FakeSolrDoc)

        qm = qm.only("name", "city")

        assert qm.returned_fields == ("name", "city")

    def test_query_limit_normal(self):
        qm = SolrQueryManager(FakeSolrDoc)

        qm = qm.limit(100)

        assert qm.rows == 100

    def test_query_limit_normal(self):
        qm = SolrQueryManager(FakeSolrDoc)

        qm = qm.offset(50)

        assert qm.start == 50

    def test_query_groupby_normal(self):
        qm = SolrQueryManager(FakeSolrDoc)

        qm = qm.group_by("city", group_limit=2)

        assert qm.group_fields == "city"
        assert qm.group_limit == 2

    def test_query_filter_default_node(self):
        qm = SolrQueryManager(FakeSolrDoc)

        qm = qm.filter(name__eq="test")

        assert qm.node.logic == 'COMP'
        assert qm.node.operator == 'eq'
        assert qm.node.key == 'name'
        assert qm.node.value == 'test'

    def test_query_filter_and_node(self):
        qm = SolrQueryManager(FakeSolrDoc)

        qm = qm.filter(AND(name__eq="Test Name",size__ge=100))

        assert qm.node.logic == 'AND'
        operators = [item.operator for item in qm.node.items]
        keys = [item.key for item in qm.node.items]
        values = [item.value for item in qm.node.items]
        assert 'eq' in  operators
        assert 'ge' in  operators
        assert 'name' in  keys
        assert 'size' in  keys
        assert "Test Name" in  values
        assert 100 in  values

    def test_query_filter_or_node(self):
        qm = SolrQueryManager(FakeSolrDoc)

        qm = qm.filter(OR(name__eq="Test Name",size__ge=100))

        assert qm.node.logic == 'OR'
        operators = [item.operator for item in qm.node.items]
        keys = [item.key for item in qm.node.items]
        values = [item.value for item in qm.node.items]
        assert 'eq' in  operators
        assert 'ge' in  operators
        assert 'name' in  keys
        assert 'size' in  keys
        assert "Test Name" in  values
        assert 100 in  values

    def test_query_filter_not_node(self):
        qm = SolrQueryManager(FakeSolrDoc)

        qm = qm.filter(NOT(name__eq="Test Name"))

        assert qm.node.logic == 'NOT'
        assert qm.node.items[0].operator == 'eq'
        assert qm.node.items[0].key == 'name'
        assert qm.node.items[0].value == 'Test Name'



    def test_query_filter_mix_node(self):
        qm = SolrQueryManager(FakeSolrDoc)

        qm = qm.filter(
            AND(
                OR(city__eq="Test City",size__le=1000),
                name__eq="Test Name"
            ),
            NOT(country__eq="Test Country"),
            state__eq="Test State",
            zip__eq="Test Zip"
        )

        assert qm.node.logic == 'AND'
        assert qm.node.items[0].logic == 'AND'
        assert qm.node.items[0].items[1].operator == 'eq'
        assert qm.node.items[0].items[1].key == 'name'
        assert qm.node.items[0].items[1].value == 'Test Name'
        assert qm.node.items[0].items[0].logic == 'OR'
        assert (
            qm.node.items[0].items[0].items[0].operator == 'eq' and
            qm.node.items[0].items[0].items[0].key == 'city' and
            qm.node.items[0].items[0].items[0].value == 'Test City' and
            qm.node.items[0].items[0].items[1].operator == 'le' and
            qm.node.items[0].items[0].items[1].key == 'size' and
            qm.node.items[0].items[0].items[1].value == 1000 or
            qm.node.items[0].items[0].items[0].operator == 'le' and
            qm.node.items[0].items[0].items[0].key == 'size' and
            qm.node.items[0].items[0].items[0].value == 1000 and
            qm.node.items[0].items[0].items[1].operator == 'eq' and
            qm.node.items[0].items[0].items[1].key == 'city' and
            qm.node.items[0].items[0].items[1].value == 'Test City'
            )
        assert qm.node.items[1].logic == 'NOT'
        assert qm.node.items[1].items[0].operator == 'eq'
        assert qm.node.items[1].items[0].key == 'country'
        assert qm.node.items[1].items[0].value == "Test Country"
        assert (
            qm.node.items[2].logic == 'COMP' and
            qm.node.items[2].operator == 'eq' and
            qm.node.items[2].key == 'state' and
            qm.node.items[2].value == "Test State" and
            qm.node.items[3].logic == 'COMP' and
            qm.node.items[3].operator == 'eq' and
            qm.node.items[3].key == 'zip' and
            qm.node.items[3].value == "Test Zip" or
            qm.node.items[3].logic == 'COMP' and
            qm.node.items[3].operator == 'eq' and
            qm.node.items[3].key == 'state' and
            qm.node.items[3].value == "Test State" and
            qm.node.items[2].logic == 'COMP' and
            qm.node.items[2].operator == 'eq' and
            qm.node.items[2].key == 'zip' and
            qm.node.items[2].value == "Test Zip"
        )


    def test_query_filter_chaining_filters(self):
        qm = SolrQueryManager(FakeSolrDoc)
        qm = qm.filter(name__eq="Test Name").filter(size__ge=100)

        assert qm.node.logic == 'AND'
        operators = [item.operator for item in qm.node.items]
        keys = [item.key for item in qm.node.items]
        values = [item.value for item in qm.node.items]
        assert 'eq' in  operators
        assert 'ge' in  operators
        assert 'name' in  keys
        assert 'size' in  keys
        assert "Test Name" in  values
        assert 100 in  values

    def test_query_filter_chaining_mix(self):
        qm = SolrQueryManager(FakeSolrDoc)
        qm = qm.filter(name__eq="Test Name").only("name").limit(100).offset(50)

        assert qm.node.logic == 'COMP'

        assert qm.node.operator == 'eq'
        assert qm.node.key == 'name'
        assert qm.node.value == 'Test Name'

        assert qm.rows == 100
        assert qm.start == 50

        assert qm.returned_fields == ("name",)

    def test_query_query_no_params(self):
        qm = SolrQueryManager(FakeSolrDoc)

        params = qm.query
        assert params['q'] == '*:*'
        assert params['rows'] == INFINITE_ROWS
        assert params['start'] == 0
        assert params['wt'] == 'json'

    def test_query_query_asc(self):
        qm = SolrQueryManager(FakeSolrDoc)
        qm = qm.sort_by("name")

        params = qm.query
        assert params['sort'] == 'name asc'

    def test_query_query_desc(self):
        qm = SolrQueryManager(FakeSolrDoc)
        qm = qm.sort_by("-name")

        params = qm.query
        assert params['sort'] == 'name desc'

    def test_query_query_chained_search(self):
        qm = SolrQueryManager(FakeSolrDoc)
        qm = qm.filter(id__eq=999).only("id","name")\
            .limit(100)\
            .offset(50)\
            .search("Test  Name ", minimum_matches="3<50%", name=10,city=5)\
            .boost_by_func('{!func}geodist()', bf_weight=2)\
            .boost_by_query('id:100', bq_weight=2)\
            .group_by('city', group_limit=2).sort_by("-name")

        params = qm.query
        assert params['sort'] == 'name desc'
        assert params['defType'] == 'edismax'
        assert params['rows'] == 100
        assert params['start'] == 50
        assert params['q'] == 'Test Name'
        assert params['fq'] ==  'id:999'
        assert params['wt'] == 'json'
        assert params['qf'] in ['city^5 name^10', 'name^10 city^5']
        assert params['fl'] in ['id,name', 'name,id']
        assert params['group'] == 'on'
        assert params['group.ngroups'] == "true"
        assert params['group.field'] == 'city'
        assert params['bf'] == '{!func}geodist()^2'
        assert params['bq'] == 'id:100^2'
        assert params['mm'] == '3<50%'

    def test_query_query_chained_regular(self):
        qm = SolrQueryManager(FakeSolrDoc)
        qm = qm.filter(id__eq=999).only("id","name")\
            .limit(100)\
            .offset(50)\
            .boost_by_func('{!func}geodist()', bf_weight=2)\
            .boost_by_query('id:100', bq_weight=2)\
            .group_by('city', group_limit=2, facet="true")\
            .facet(['country'], mincount=3)\
            .sort_by("-name")

        params = qm.query
        assert params['sort'] == 'name desc'
        assert params['rows'] == 100
        assert params['start'] == 50
        assert params['q'] ==  'id:999'
        assert params['wt'] == 'json'
        assert params['fl'] in ['id,name', 'name,id']
        assert params['group'] == 'on'
        assert params['group.field'] == 'city'
        assert params['group.limit'] == 2
        assert params['group.facet'] == 'true'
        assert params['facet'] == 'on'
        assert params['facet.field'] == ['country']
        assert params['facet.mincount'] == 3
        assert params['bf'] == '{!func}geodist()^2'
        assert params['bq'] == 'id:100^2'

    def test_query_query__facet_group_off1(self):
        qm = SolrQueryManager(FakeSolrDoc)
        qm = qm.filter(id__eq=999)\
            .group_by('city', group_limit=2)\
            .facet(['country'], mincount=3, group=False)\

        params = qm.query
        assert 'group.facet' not in params

    def test_query_query__facet_group_off2(self):
        qm = SolrQueryManager(FakeSolrDoc)
        qm = qm.filter(id__eq=999)\
            .group_by('city', group_limit=2)\

        params = qm.query
        assert 'group.facet' not in params

    def test_query_query__facet_group_off3(self):
        qm = SolrQueryManager(FakeSolrDoc)
        qm = qm.filter(id__eq=999)\
            .facet(['country'], mincount=3)\

        params = qm.query
        assert 'group.facet' not in params


    def test_query_fetch_all_normal(self):
        with mock.patch('wukong.api.SolrAPI.select') as mock_select:
            mock_select.return_value = {
                'docs': [
                    {
                        "id": 1,
                        "name": "Test Name 1"
                    },
                    {
                        "id": 2,
                        "name": "Test Name 2"
                    }
                ]
            }
            with mock.patch('wukong.api.SolrAPI.get_schema') as mock_schema:
                mock_schema.return_value = {
                    "uniqueKey": "id",
                    "fields": [
                        {
                            "name": "id",
                            "type": "int"
                        },
                        {
                            "name": "name",
                            "type": "string"
                        }
                    ]
                }

                qm = SolrQueryManager(FakeSolrDoc)

                docs = qm.all(metadata=True)

                assert docs[0].id == 1
                assert docs[0].name == "Test Name 1"
                assert docs[1].id == 2
                assert docs[1].name == "Test Name 2"

    def test_query_fetch_all_with_error(self):
        with mock.patch('wukong.api.SolrAPI.select') as mock_select:
            def fake_select(*args, **kwargs):
                raise SolrError((4, "Select fetch failed"))

            mock_select.side_effect = fake_select

            with mock.patch('wukong.api.SolrAPI.get_schema') as mock_schema:
                mock_schema.return_value = {
                    "uniqueKey": "id",
                    "fields": [
                        {
                            "name": "id",
                            "type": "int"
                        },
                        {
                            "name": "name",
                            "type": "string"
                        }
                    ]
                }

                with self.assertRaises(SolrError) as cm:
                    qm = SolrQueryManager(FakeSolrDoc)
                    docs = qm.all()

                schema_error = cm.exception
                self.assertEqual(schema_error.message, "Select fetch failed")


    def test_query_fetch_one_with_one_returned(self):
        with mock.patch('wukong.api.SolrAPI.select') as mock_select:
            mock_select.return_value = {
                'docs': [
                    {
                        "id": 1,
                        "name": "Test Name 1"
                    }
                ]
            }
            with mock.patch('wukong.api.SolrAPI.get_schema') as mock_schema:
                mock_schema.return_value = {
                    "uniqueKey": "id",
                    "fields": [
                        {
                            "name": "id",
                            "type": "int"
                        }
                    ]
                }

                qm = SolrQueryManager(FakeSolrDoc)

                doc = qm.one()

                assert doc.id == 1
                assert doc.name == "Test Name 1"

    def test_query_fetch_one_with_none_returned(self):
        with mock.patch('wukong.api.SolrAPI.select') as mock_select:
            mock_select.return_value = {
                'docs': []
            }
            with mock.patch('wukong.api.SolrAPI.get_schema') as mock_schema:
                mock_schema.return_value = {
                    "uniqueKey": "id",
                    "fields": [
                        {
                            "name": "id",
                            "type": "int"
                        }
                    ]
                }

                qm = SolrQueryManager(FakeSolrDoc)

                doc = qm.one()

                assert doc is None

    def test_query_fetch_one_with_multi_returned(self):
        with mock.patch('wukong.api.SolrAPI.select') as mock_select:
            mock_select.return_value = {
                'docs': [
                    {
                        "id": 1,
                        "name": "Test Name 1"
                    },
                    {
                        "id": 2,
                        "name": "Test Name 2"
                    }
                ]
            }
            with mock.patch('wukong.api.SolrAPI.get_schema') as mock_schema:
                mock_schema.return_value = {
                    "uniqueKey": "id",
                    "fields": [
                        {
                            "name": "id",
                            "type": "int"
                        }
                    ]
                }

                qm = SolrQueryManager(FakeSolrDoc)

                doc = qm.one()

                assert doc.id == 1
                assert doc.name == "Test Name 1"

    def test_query_get(self):
        with mock.patch('wukong.api.SolrAPI.select') as mock_select:
            mock_select.return_value = {
                'docs': [
                    {
                        "id": 1,
                        "name": "Test Name 1"
                    }
                ]
            }
            with mock.patch('wukong.api.SolrAPI.get_schema') as mock_schema:
                mock_schema.return_value = {
                    "uniqueKey": "id",
                    "fields": [
                        {
                            "name": "id",
                            "type": "int"
                        }
                    ]
                }

                qm = SolrQueryManager(FakeSolrDoc)

                doc = qm.get(id__eq=1)

                assert doc.id == 1
                assert doc.name == "Test Name 1"

    def test_query_create_normal(self):
        with mock.patch('wukong.api.SolrAPI.select') as mock_select:
            mock_select.return_value = {
                'docs': []
            }
            with mock.patch('wukong.api.SolrAPI.get_schema') as mock_schema:
                mock_schema.return_value = {
                    "uniqueKey": "id",
                    "fields": [
                        {
                            "name": "id",
                            "type": "int"
                        },
                        {
                            "name": "name",
                            "type": "string"
                        }
                    ]
                }
                with mock.patch('wukong.api.SolrAPI.update') as mock_add:
                    with mock.patch('wukong.api.SolrAPI.commit') as mock_commit:
                        qm = SolrQueryManager(FakeSolrDoc)
                        doc = qm.create(id=1, name="Test Name")

                        assert doc.id == 1
                        assert doc.name == "Test Name"

    def test_query_create_duplicated_unique_ke(self):
        with mock.patch('wukong.api.SolrAPI.select') as mock_select:
            mock_select.return_value = {
                'docs': [
                    {
                        "id": 1,
                        "name": "Test Name 1"
                    }
                ]
            }
            with mock.patch('wukong.api.SolrAPI.get_schema') as mock_schema:
                mock_schema.return_value = {
                    "uniqueKey": "id",
                    "fields": [
                        {
                            "name": "id",
                            "type": "int"
                        },
                        {
                            "name": "name",
                            "type": "string"
                        }
                    ]
                }

                with self.assertRaises(SolrDuplicateUniqueKeyError) as cm:
                    qm = SolrQueryManager(FakeSolrDoc)
                    doc = qm.create(id=1, name="Test Name")

                schema_error = cm.exception
                self.assertEqual(schema_error.message, "The unique key 1 already exists" )

    def test_query_update_normal(self):
        with mock.patch('wukong.api.SolrAPI.select') as mock_select:
            mock_select.return_value =  {
                'docs': [{
                        "id": 1,
                        "name": "Test Name 1"
                    }
                ]
            }
            with mock.patch('wukong.api.SolrAPI.get_schema') as mock_schema:
                mock_schema.return_value = {
                    "uniqueKey": "id",
                    "fields": [
                        {
                            "name": "id",
                            "type": "int"
                        },
                        {
                            "name": "name",
                            "type": "string"
                        }
                    ]
                }
                with mock.patch('wukong.api.SolrAPI.update') as mock_add:
                    with mock.patch('wukong.api.SolrAPI.commit') as mock_commit:
                        qm = SolrQueryManager(FakeSolrDoc)
                        doc = qm.update(id=1, name="Test Name")

                        assert doc.id == 1
                        assert doc.name == "Test Name"

    def test_query_create_no_doc_exist(self):
        with mock.patch('wukong.api.SolrAPI.select') as mock_select:
            mock_select.return_value = {
                'docs': []
            }
            with mock.patch('wukong.api.SolrAPI.get_schema') as mock_schema:
                mock_schema.return_value = {
                    "uniqueKey": "id",
                    "fields": [
                        {
                            "name": "id",
                            "type": "int"
                        },
                        {
                            "name": "name",
                            "type": "string"
                        }
                    ]
                }

                with self.assertRaises(SolrDocumentNotExistError) as cm:
                    qm = SolrQueryManager(FakeSolrDoc)
                    doc = qm.update(id=1, name="Test Name")

                schema_error = cm.exception
                self.assertEqual(schema_error.message, "The document with unique key 1 does not exist" )