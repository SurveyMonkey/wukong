import mock
from mock import Mock, PropertyMock, patch
from wukong.models import *
from wukong.errors import *
import pytest


try:
    import unittest2 as unittest
except ImportError:
    import unittest

class FakeSolrDoc(SolrDoc):
    solr_hosts = "fake_host"
    zookeeper_hosts = "fake_zookeeper_hosts"
    collection_name = "fake_collection"

class FakeSolrDoc1(SolrDoc):
    solr_hosts = "fake_host"
    zookeeper_hosts = "fake_zookeeper_hosts"
    collection_name = "fake_collection1"

class FakeCollection(object):
    name = "fake_collection"

mock_schema = Mock(return_value = {
    "uniqueKey": "id",
    "fields": [
        {
            "name": "id",
            "type": "int"
        },
        {
            "name": "name",
            "type": "string"
        },
        {
            "name": "zip",
            "type": "string"
        },
        {
            "name": "test_solrdoc_list",
            "type": "string",
            "multiValued": True
        }
    ],
    "dynamicFields": [
        {
            "name": "*_d1",
            "type": "int"
        },
        {
            "name": "*_d2",
            "type": "string"
        }
    ]

})

class TestModels(unittest.TestCase):
    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_init(self):
        doc = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=100,
            name="Test Name"
        )

        assert doc.partial_update == True
        assert doc.field_weights == {"name":10}
        assert doc.id == 100
        assert doc.name == "Test Name"
        assert doc.collection_name == "fake_collection"
        assert doc.__class__.documents.doc_class == FakeSolrDoc
        assert doc.unique_key == "id"
        assert (doc.solr.solr_hosts ==
            ['http://fake_host/solr/'])
        assert (doc.solr.solr_collection == 'fake_collection')

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_add_schema_fields(self):
        with mock.patch('wukong.api.SolrAPI.add_schema_fields') as mock_add_fields:

            FakeSolrDoc.add_schema_fields([
                {
                    "name": "id",
                    "type": "int"
                },
                {
                    "name": "city",
                    "type": "string"
                }
            ])
            mock_add_fields.assert_called_once_with([{"name": "city","type": "string"}])

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_from_json_docs(self):

        docs = FakeSolrDoc.from_json_docs([
            {
                "id": 1,
                "name": "Test Name 1"
            },
            {
                "id": 2,
                "name": "Test Name 2"
            }
        ])
        assert docs[0].id == 1
        assert docs[0].name == "Test Name 1"
        assert docs[1].id == 2
        assert docs[1].name == "Test Name 2"

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_to_str(self):

        doc = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=100,
            name="Test Name"
        )

        assert str(doc)in ["{'name': 'Test Name', 'id': 100}", "{'id': 100, 'name': 'Test Name'}"]

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_repr(self):

        doc = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=100,
            name="Test Name"
        )

        assert repr(doc) == "FakeSolrDoc(id=100)"

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_equal_to_true(self):

        doc1 = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=100,
            name="Test Name",
            zip="Test Zip"
        )

        doc2 = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=100,
            name="Test Name",
            zip="Test Zip"
        )


        assert doc1 == doc2

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_equal_to_false1(self):

        doc1 = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=100,
            name="Test Name",
            zip="Test Zip"
        )

        doc2 = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=100,
            name="Test Name"
        )


        assert not doc1 == doc2

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_equal_to_false2(self):

        doc1 = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=100,
            name="Test Name 1",
            zip="Test Zip"
        )

        doc2 = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=100,
            name="Test Name 2",
            zip="Test Zip"
        )

        assert not doc1 == doc2

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_equal_to_false3(self):

        doc1 = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=100,
            name="Test Name",
            zip="Test Zip"
        )

        doc2 = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=101,
            name="Test Name",
            zip="Test Zip"
        )
        assert not doc1 == doc2

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_not_equal_to_false(self):

        doc1 = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=100,
            name="Test Name 1",
            zip="Test Zip"
        )

        doc2 = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=100,
            name="Test Name",
            zip="Test Zip"
        )


        assert doc1 != doc2

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_not_equal_to_true1(self):

        doc1 = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=100,
            name="Test Name",
            zip="Test Zip"
        )

        doc2 = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=100,
            name="Test Name"
        )


        assert doc1 != doc2

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_not_equal_to_true2(self):

        doc1 = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=100,
            name="Test Name 1",
            zip="Test Zip"
        )

        doc2 = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=100,
            name="Test Name 2",
            zip="Test Zip"
        )

        assert doc1 != doc2

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_not_equal_to_true3(self):

        doc1 = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=100,
            name="Test Name",
            zip="Test Zip"
        )

        doc2 = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=101,
            name="Test Name",
            zip="Test Zip"
        )
        assert doc1 != doc2

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_set_item_update(self):

        doc = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=100,
            name="Test Name"
        )

        assert doc.name == "Test Name"
        doc['name'] = "Test Name 1"
        assert doc.name == "Test Name 1"

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_set_item_new(self):

        doc = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=100,
            name="Test Name"
        )
        assert doc.zip == None
        doc['zip'] = "Test Zip"
        assert doc.zip ==  "Test Zip"

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_get_item_none(self):

        doc = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=100,
            name="Test Name"
        )
        assert doc['zip'] == None

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_get_item_exist(self):

        doc = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=100,
            name="Test Name"
        )
        assert doc['name'] == "Test Name"

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_get_attr_none(self):

        doc = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=100,
            name="Test Name"
        )
        assert doc.zip == None

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_get_item_exist(self):

        doc = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=100,
            name="Test Name"
        )
        assert doc.name == "Test Name"

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_get_item_builtin_attr(self):

        doc = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=100,
            name="Test Name"
        )
        assert doc.fields == {"name":"Test Name", "id":100}

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_length_method(self):

        doc = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=100,
            name="Test Name"
        )
        assert len(doc.fields) == 2


    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_iter_items(self):

        doc = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=100,
            name="Test Name"
        )
        fields = {}
        for name, field in doc:
            fields[name]= field

        assert fields == {"name": "Test Name", "id": 100}

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_set_field_update(self):

        doc = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=100,
            name="Test Name"
        )

        assert doc.name == "Test Name"
        doc.set_field('name', "Test Name 1")
        assert doc.name == "Test Name 1"

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_set_field_new(self):

        doc = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=100,
            name="Test Name"
        )
        assert doc.zip == None
        doc.set_field('zip', "Test Zip")
        assert doc.zip ==  "Test Zip"

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_del_field_normal(self):

        doc = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=100,
            name="Test Name"
        )

        assert doc.name == "Test Name"
        doc.del_field('name')
        assert doc.name == None

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_del_field_error(self):

        doc = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=100,
            name="Test Name"
        )

        with self.assertRaises(SolrDeleteUniqueKeyError) as cm:
            doc.del_field('id')

        schema_error = cm.exception
        self.assertEqual(str(schema_error), "The unique key id can not be deleted" )

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_is_partial_update(self):

        doc = FakeSolrDoc(
            partial_update=True,
            field_weights={"name":10},
            id=100,
            name="Test Name"
        )

        assert doc.is_partial_update()

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_set_partial_update(self):

        doc = FakeSolrDoc(
            partial_update=False,
            field_weights={"name":10},
            id=100,
            name="Test Name"
        )
        assert doc.partial_update == False
        doc.set_partial_update(True)
        assert doc.partial_update == True

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_set_field_weight(self):

        doc = FakeSolrDoc(
            partial_update=False,
            field_weights={"name":10},
            id=100,
            name="Test Name"
        )
        assert doc.field_weights['name'] == 10
        doc.set_field_weight('name', 20)
        assert doc.field_weights['name'] == 20


    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_set_field_weight_no_key(self):

        doc = FakeSolrDoc(
            partial_update=False,
            id=100,
            name="Test Name"
        )
        assert doc.field_weights is None
        doc.set_field_weight('name', 20)
        assert doc.field_weights['name'] == 20

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_get_field_weight(self):

        doc = FakeSolrDoc(
            partial_update=False,
            field_weights={"name":10},
            id=100,
            name="Test Name"
        )

        assert doc.get_field_weight('name') == 10


    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_get_field_weight_no_key(self):

        doc = FakeSolrDoc(
            partial_update=False,
            id=100,
            name="Test Name"
        )

        assert doc.get_field_weight('name') is None

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_validate_schema_fields(self):

        FakeSolrDoc.validate_schema_fields({
            "id": 100,
            "name": "Test Name",
            "zip": "Test Zip",
            "test_solrdoc_list": ["test1", "test2"],
            "test_solrdoc_d1": 1,
            "test_solrdoc_d2": "test d2"
        })

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_validate_schema_fields_no_pk(self):
        with self.assertRaises(SolrSchemaValidationError) as cm:
            FakeSolrDoc.validate_schema_fields({
                "name": "Test Name",
                "zip": "Test Zip",
                "test_solrdoc_list": ["test1", "test2"],
                "test_solrdoc_d1": 1,
                "test_solrdoc_d2": "test d2"
            })

        schema_error = cm.exception
        self.assertEqual(str(schema_error), "Unique key is not specified" )

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_validate_schema_fields_none_pk(self):
        with self.assertRaises(SolrSchemaValidationError) as cm:
            FakeSolrDoc.validate_schema_fields({
                "id": None,
                "name": "Test Name",
                "zip": "Test Zip",
                "test_solrdoc_list": ["test1", "test2"],
                "test_solrdoc_d1": 1,
                "test_solrdoc_d2": "test d2"
            })

        schema_error = cm.exception
        self.assertEqual(str(schema_error), "Unique key is not specified" )

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_validate_schema_fields_multivalue_error(self):
        with self.assertRaises(SolrSchemaValidationError) as cm:
            FakeSolrDoc.validate_schema_fields({
                "id": 1,
                "name": "Test Name",
                "zip": "Test Zip",
                "test_solrdoc_list": "test1",
                "test_solrdoc_d1": 1,
                "test_solrdoc_d2": "test d2"
            })

        schema_error = cm.exception
        self.assertEqual(str(schema_error), "test_solrdoc_list is not list type" )

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_validate_schema_fields_error(self):
        with self.assertRaises(SolrSchemaValidationError) as cm:
            FakeSolrDoc.validate_schema_fields({
                "id": 1,
                "name": "Test Name",
                "zip": "Test Zip",
                "test_solrdoc_list": ["test1", "test2"],
                "test_solrdoc_d3": 1,
                "test_solrdoc_d2": "test d2"
            })

        schema_error = cm.exception
        self.assertEqual(str(str(schema_error)), "Field (test_solrdoc_d3) is not in SOLR schema fields" )

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_get_data_for_solr_full(self):

        doc = FakeSolrDoc(
            partial_update=False,
            id=100,
            name="Test Name"
        )

        data = doc.get_data_for_solr()

        assert data == {"id": 100, "name": "Test Name"}

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_get_data_for_solr_partial(self):

        doc = FakeSolrDoc(
            partial_update=True,
            id=100,
            name="Test Name"
        )

        data = doc.get_data_for_solr()

        assert data == {"id": 100, "name": {"set":"Test Name"}}

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_get_data_for_solr_weighted(self):

        doc = FakeSolrDoc(
            field_weights={"name":10},
            id=100,
            name="Test Name"
        )

        data = doc.get_data_for_solr()

        assert data == {"id": 100, "name": {"boost":10,"value":"Test Name"}}

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_index_with_no_commit(self):
        with mock.patch('wukong.api.SolrAPI.update') as mock_update:
            mock_update.return_value = {}
            with mock.patch('wukong.api.SolrAPI.commit') as mock_commit:
                mock_commit.return_value = {}

                doc = FakeSolrDoc(
                    id=100,
                    name="Test Name"
                )

                doc.index()

                mock_update.assert_called_once_with([{"id":100,"name":"Test Name"}])
                assert not mock_commit.called

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_index_with_commit(self):
        with mock.patch('wukong.api.SolrAPI.update') as mock_update:
            mock_update.return_value = {}
            with mock.patch('wukong.api.SolrAPI.commit') as mock_commit:
                mock_commit.return_value = {}

                doc = FakeSolrDoc(
                    id=100,
                    name="Test Name"
                )

                doc.index(commit=True)

                mock_update.assert_called_once_with([{"id":100,"name":"Test Name"}])
                assert mock_commit.called


    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_delete_with_no_commit(self):
        with mock.patch('wukong.api.SolrAPI.delete') as mock_delete:
            mock_delete.return_value = {}

            doc = FakeSolrDoc(
                id=100,
                name="Test Name"
            )

            doc.delete()
            mock_delete.assert_called_once_with("id", 100, commit=False)

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_delete_with_commit(self):
        with mock.patch('wukong.api.SolrAPI.delete') as mock_delete:
            mock_delete.return_value = {}

            doc = FakeSolrDoc(
                id=100,
                name="Test Name"
            )

            doc.delete(commit=True)
            mock_delete.assert_called_once_with("id", 100, commit=True)


    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdocs_init_no_docs(self):
        docs = SolrDocs()

        assert docs.docs == []
        assert docs.doc_class == None

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdocs_init_no_docs(self):
        solr_docs = [
            FakeSolrDoc(
                id=1,
                name="Test Name 1"
            ),
            FakeSolrDoc(
                id=2,
                name="Test Name 2"
            )
        ]
        docs = SolrDocs(docs=solr_docs)

        assert docs.docs[0].id == 1
        assert docs.docs[0].name == "Test Name 1"
        assert docs.docs[1].id == 2
        assert docs.docs[1].name == "Test Name 2"
        assert docs.doc_class == FakeSolrDoc

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdocs_to_str(self):

        solr_docs = [
            FakeSolrDoc(
                id=1,
                name="Test Name 1"
            ),
            FakeSolrDoc(
                id=2,
                name="Test Name 2"
            )
        ]
        docs = SolrDocs(docs=solr_docs)

        assert str(docs) == "[FakeSolrDoc(id=1), FakeSolrDoc(id=2)]"

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdocs_repr(self):
        solr_docs = [
            FakeSolrDoc(
                id=1,
                name="Test Name 1"
            ),
            FakeSolrDoc(
                id=2,
                name="Test Name 2"
            )
        ]
        docs = SolrDocs(docs=solr_docs)
        assert repr(docs) == "SolrDocs(len(FakeSolrDoc)=2)"

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdocs_set_item(self):

        solr_docs = [
            FakeSolrDoc(
                id=1,
                name="Test Name 1"
            ),
            FakeSolrDoc(
                id=2,
                name="Test Name 2"
            )
        ]
        docs = SolrDocs(docs=solr_docs)

        assert docs.docs[0].name == "Test Name 1"
        docs[0] = FakeSolrDoc(
            id=3,
            name="Test Name 3"
        )
        assert docs.docs[0].name == "Test Name 3"

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdocs_get_item(self):

        solr_docs = [
            FakeSolrDoc(
                id=1,
                name="Test Name 1"
            ),
            FakeSolrDoc(
                id=2,
                name="Test Name 2"
            )
        ]
        docs = SolrDocs(docs=solr_docs)

        assert docs[0].name == "Test Name 1"

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdocs_length(self):

        solr_docs = [
            FakeSolrDoc(
                id=1,
                name="Test Name 1"
            ),
            FakeSolrDoc(
                id=2,
                name="Test Name 2"
            )
        ]
        docs = SolrDocs(docs=solr_docs)

        assert len(docs)== 2

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdocs_iter_items(self):

        solr_docs = [
            FakeSolrDoc(
                id=1,
                name="Test Name 1"
            ),
            FakeSolrDoc(
                id=2,
                name="Test Name 2"
            )
        ]
        docs = SolrDocs(docs=solr_docs)

        solr_docs = []
        for doc in docs:
            solr_docs.append(doc)

        assert solr_docs[0].id == 1
        assert solr_docs[0].name == "Test Name 1"
        assert solr_docs[1].id == 2
        assert solr_docs[1].name == "Test Name 2"

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdocs_add(self):
        docs = SolrDocs()
        docs.add(
            FakeSolrDoc(
                id=1,
                name="Test Name 1"
            )
        )
        docs.add(
            FakeSolrDoc(
                id=2,
                name="Test Name 2"
            )
        )
        assert len(docs.docs) == 2
        assert docs.doc_class == FakeSolrDoc

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdocs_add_error(self):
        with self.assertRaises(SolrError) as cm:
            docs = SolrDocs()
            docs.add(
                FakeSolrDoc(
                    id=1,
                    name="Test Name 1"
                )
            )
            docs.add(
                FakeSolrDoc1(
                    id=2,
                    name="Test Name 2"
                )
            )
        error = cm.exception
        self.assertEqual(str(error),
            "The types of documents in a container should be the same" )

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdocs_index_no_docs(self):

        docs = SolrDocs()

        with mock.patch('wukong.api.SolrAPI.update') as mock_update:
            mock_update.return_value = {}
            docs.index()

        assert not mock_update.called

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdocs_index_with_docs(self):

        docs = SolrDocs()
        docs.add(
            FakeSolrDoc(
                id=1,
                name="Test Name 1"
            )
        )
        docs.add(
            FakeSolrDoc(
                id=2,
                partial_update=True,
                name="Test Name 2"
            )
        )

        with mock.patch('wukong.api.SolrAPI.update') as mock_update:
            mock_update.return_value = {}
            docs.index()

        mock_update.assert_called_once_with([
            {"id":1, "name":"Test Name 1"},
            {"id":2, "name":{"set":"Test Name 2"}}
        ])

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdocs_index_commit(self):

        docs = SolrDocs()
        docs.add(
            FakeSolrDoc(
                id=1,
                name="Test Name 1"
            )
        )
        docs.add(
            FakeSolrDoc(
                id=2,
                partial_update=True,
                name="Test Name 2"
            )
        )
        with mock.patch('wukong.api.SolrAPI.update') as mock_update:
            with mock.patch('wukong.api.SolrAPI.commit') as mock_commit:
                mock_commit.return_value = {}
                docs.index(commit=True)

        assert mock_commit.called

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdocs_delete_no_docs(self):

        docs = SolrDocs()

        with mock.patch('wukong.api.SolrAPI.delete') as mock_delete:
            mock_delete.return_value = {}
            docs.delete()

        assert not mock_delete.called

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdocs_delete_with_docs(self):

        docs = SolrDocs()
        docs.add(
            FakeSolrDoc(
                id=1,
                name="Test Name 1"
            )
        )
        docs.add(
            FakeSolrDoc(
                id=2,
                partial_update=True,
                name="Test Name 2"
            )
        )

        with mock.patch('wukong.api.SolrAPI.delete') as mock_delete:
            mock_delete.return_value = {}
            docs.delete()

        mock_delete.assert_called_once_with("id", "(1 2)", commit=False)

    @patch('wukong.api.SolrAPI.get_schema', mock_schema)
    def test_models_solrdoc_schema(self):

        doc = FakeSolrDoc(id=1)
        assert doc.schema == {
            'fields': [
                {'type': 'int', 'name': 'id'},
                {'type': 'string', 'name': 'name'},
                {'type': 'string', 'name': 'zip'},
                {'type': 'string', 'name': 'test_solrdoc_list', 'multiValued': True}],
                'uniqueKey': 'id',
                'dynamicFields': [
                    {'type': 'int', 'name': '*_d1'},
                    {'type': 'string', 'name': '*_d2'}]
        }
