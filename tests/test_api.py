import mock
from wukong.api import SolrAPI
from wukong.errors import *
import pytest
import json

try:
    import unittest2 as unittest
except ImportError:
    import unittest

class TestSolrAPI(unittest.TestCase):

    api = SolrAPI(
        "localsolr:7070,localsolr:8080",
        "test_collection",
        "localzook01:2181,localzook02:2181"
    )

    def test_api_constructor___one_node(self):
        api = SolrAPI(
            "localsolr:8080",
            "test_collection",
            "localzook01:2181,localzook02:2181"
        )

        assert api.solr_hosts == \
               ["http://localsolr:8080/solr/"]

        assert api.solr_collection == "test_collection"

    def test_api_constructor__node_list(self):
        api = SolrAPI(
            "localsolr:7070,localsolr:8080",
            "test_collection",
            "localzook01:2181,localzook02:2181"
        )

        assert api.solr_hosts == \
               ["http://localsolr:7070/solr/","http://localsolr:8080/solr/"]

        assert api.solr_collection == "test_collection"

    def test_api_constructor__error_no_solr_host(self):
        with self.assertRaises(SolrError) as cm:
            SolrAPI(
                None,
                "test_collection",
                "localzook01:2181,localzook02:2181"
            )

        solr_error = cm.exception
        self.assertEqual(solr_error.message, "Either solr_hosts or solr_collection can not be None")

    def test_api_constructor__no_zook_host(self):
        api = SolrAPI(
            "localsolr:7070,localsolr:8080",
            "test_collection",
            None
        )

        assert api.solr_hosts == \
               ["http://localsolr:7070/solr/","http://localsolr:8080/solr/"]
        assert api.solr_collection == "test_collection"
        assert api.zookeeper_hosts == None

    def test_api_constructor__error_no_collection(self):
        with self.assertRaises(SolrError) as cm:
            SolrAPI(
                "localsolr:8080",
                None,
                "localzook01:2181,localzook02:2181"
            )

        solr_error = cm.exception
        self.assertEqual(solr_error.message, "Either solr_hosts or solr_collection can not be None")

    def test_api_update(self):
        with mock.patch('wukong.request.SolrRequest.post') as mock_method:
            mock_method.return_value={}

            docs = [
                {
                    "pk": "Test PK 1",
                    "test_field": "Test Value 1"
                },
                {
                    "pk": "Test PK 2",
                    "test_field": "Test Value 2"
                }
            ]

            self.api.update(docs, commit=True)

            mock_method.assert_called_once_with(
                'test_collection/update/json',
                body=json.dumps(docs),
                params={"commit": "true"}
            )

    def test_api_update__no_docs(self):
        with mock.patch('wukong.request.SolrRequest.post') as mock_method:

            mock_method.return_value={}

            docs = []

            self.api.update(docs)

            assert not mock_method.called

    def test_api_select(self):
        with mock.patch('wukong.request.SolrRequest.get') as mock_method:
            fake_response = {
                "response":{
                    "docs":[
                        {
                            "pk": "Test PK",
                            "test_field": "Test Value"
                        }
                    ]
                }
            }

            mock_method.return_value = fake_response

            query_dict = {
                "q": "test_field:test_value",
                "rows": 10
            }
            result = self.api.select(query_dict, extra="extra_value")
            mock_method.assert_called_once_with(
                'test_collection/select',
                params={
                    "q":"test_field:test_value",
                    "rows":10,
                    "extra": "extra_value"
                }
            )
            self.assertEqual(result['docs'][0]["pk"], "Test PK")
            self.assertEqual(result['docs'][0]["test_field"], "Test Value")

    def test_api_select__group_by(self):
        with mock.patch('wukong.request.SolrRequest.get') as mock_method:
            fake_response = {
                "grouped":{
                    "city":{
                        "groups":[
                            {
                                "groupValue": 100,
                                "doclist": []
                            }
                        ]
                    }
                }
            }

            mock_method.return_value = fake_response

            query_dict = {
                "q": "test_field:test_value",
                "rows": 10,
                "group": "on",
                "group.field": "city"
            }
            result = self.api.select(query_dict, groups=True, extra="extra_value")
            mock_method.assert_called_once_with(
                'test_collection/select',
                params={
                    "q":"test_field:test_value",
                    "rows":10,
                    "group": "on",
                    "group.field": "city",
                    "extra": "extra_value"
                }
            )

            self.assertEqual(result['groups']['city']['groups'][0]["groupValue"], 100)
            self.assertEqual(result['groups']['city']['groups'][0]["doclist"], [])

    def test_api_select__group_by(self):
        with mock.patch('wukong.request.SolrRequest.get') as mock_method:
            fake_response = {
                "grouped":{
                    "city":{
                        "ngroups": 5,
                        "groups":[
                            {
                                "groupValue": 100,
                                "doclist": []
                            }
                        ]
                    }
                },
                "facet_counts":{
                    "facet_fields":{
                        "template_ids":{
                            "60": 50
                        }
                    }
                }
            }

            mock_method.return_value = fake_response

            query_dict = {
                "q": "test_field:test_value",
                "rows": 10,
                "group": "on",
                "group.field": "city",
                "facet": "on",
                "facet.field": ["template_ids"]
            }
            result = self.api.select(query_dict, groups=True, facets=True)
            mock_method.assert_called_once_with(
                'test_collection/select',
                params={
                    "q":"test_field:test_value",
                    "rows":10,
                    "group": "on",
                    "group.field": "city",
                    "facet": "on",
                    "facet.field": ["template_ids"]
                }
            )
            self.assertEqual(result['facets']['facet_fields']["template_ids"]["60"], 50)
            self.assertEqual(result['groups']['city']['groups'][0]["groupValue"], 100)
            self.assertEqual(result['groups']['city']['groups'][0]["doclist"], [])

    def test_api_delete(self):
        with mock.patch('wukong.request.SolrRequest.post') as mock_method:

            schema = self.api.delete("pk", 1, commit=True)

            mock_method.assert_called_once_with(
                'test_collection/update/json',
                body=json.dumps({
                    "delete":{
                        "query": "pk:1"
                    }
                }),
                params={"commit": "true"}
            )


    def test_api_commit(self):
        with mock.patch('wukong.request.SolrRequest.post') as mock_method:

            schema = self.api.commit()

            mock_method.assert_called_once_with(
                'test_collection/update/json',
                params={
                    "commit": "true"
                }
            )

    def test_api_get_schema_fields(self):
        with mock.patch('wukong.request.SolrRequest.get') as mock_method:

            mock_method.return_value={
                "schema": {
                    "uniqueKey": "Test PK",
                    "fields":[
                        {"name": "Test Field 1"},
                        {"name": "Test Field 2"}
                    ]
                }
            }

            schema = self.api.get_schema()

            mock_method.assert_called_once_with(
                'test_collection/schema'
            )
            self.assertEqual(schema["uniqueKey"], "Test PK")
            self.assertEqual(schema["fields"][0]["name"], "Test Field 1")
            self.assertEqual(schema["fields"][1]["name"], "Test Field 2")

    def test_api_add_schema_fields(self):
        with mock.patch('wukong.request.SolrRequest.post') as mock_method:

            mock_method.return_value={}

            fields = [{
                "name":"test_field",
                "type":"long",
                "indexed": "true",
                "stored": "true",
                "multiValued": "true"
            }]

            self.api.add_schema_fields(fields)

            mock_method.assert_called_once_with(
                'test_collection/schema/fields',
                body=json.dumps(fields)
            )

    def test_api_add_schema_fields__no_docs(self):
        with mock.patch('wukong.request.SolrRequest.post') as mock_method:

            mock_method.return_value={}

            fields = []

            self.api.add_schema_fields(fields)

            assert not mock_method.called

    def test_api_add_schema_fields__error(self):
        with mock.patch('wukong.request.SolrRequest.post') as mock_method:
            def request(*args, **kwargs):
                raise SolrError(None)

            mock_method.side_effect=request

            fields = [{
                "name":"test_field1",
                "type":"long",
                "indexed": "true",
                "stored": "true",
                "multiValued": "true"
            },{
                "name":"test_field2",
                "type":"long",
                "indexed": "true",
                "stored": "true",
                "multiValued": "true"
            }]

            with self.assertRaises(SolrSchemaUpdateError) as cm:
                self.api.add_schema_fields(fields)

        solr_error = cm.exception
        self.assertEqual(solr_error.fields, [field['name'] for field in fields])

    def test_api_is_alive(self):

        with mock.patch('wukong.request.SolrRequest.get') as mock_get:
            mock_get.return_value = {
                "znode": {
                    "data": json.dumps({
                        "test_collection": {
                            "shards":{
                                "test_collection_shard1":{
                                    "replicas":{
                                        "test_collection_shard1_replica1": {
                                            "state": "active"
                                        }
                                    }
                                }
                            }
                        },
                        "test_collection1": {
                            "shards":{
                                "test_collection1_shard1":{
                                    "replicas":{
                                        "test_collection1_shard1_replica1": {
                                            "state": "active"
                                        }
                                    }
                                }
                            }
                        }
                    })

                }

            }

            result = self.api.is_alive()

        assert result == True

    def test_api_is_alive__down(self):

        with mock.patch('wukong.request.SolrRequest.get') as mock_get:
            mock_get.return_value = {
                "znode": {
                    "data": json.dumps({
                        "test_collection": {
                            "shards":{
                                "test_collection_shard1":{
                                    "replicas":{
                                        "test_collection_shard1_replica1": {
                                            "state": "down"
                                        }
                                    }
                                }
                            }
                        },
                        "test_collection1": {
                            "shards":{
                                "test_collection1_shard1":{
                                    "replicas":{
                                        "test_collection1_shard1_replica1": {
                                            "state": "active"
                                        }
                                    }
                                }
                            }
                        }
                    })

                }

            }

            result = self.api.is_alive()

        assert result == False

    def test_api_is_alive__server_down(self):

        with mock.patch('wukong.request.SolrRequest.get') as mock_get:

            def get(*args, **kwargs):
                raise SolrError("Server down!")

            mock_get.side_effect = get

            result = self.api.is_alive()

        assert result == False

    def test_api_is_alive__malformed_response(self):

        with mock.patch('wukong.request.SolrRequest.get') as mock_get:
            mock_get.return_value = {
                "znode": {
                    "data": "Malformed Json"
                }
            }

            result = self.api.is_alive()

        assert result == False
