from mock import MagicMock, patch
from wukong.zookeeper import Zookeeper
import requests

try:
    import unittest2 as unittest
except ImportError:
    import unittest


class TestZookeeper(unittest.TestCase):

    zook_client = Zookeeper("http://localzook01:2181,http://localzook02:2181")

    @patch('kazoo.client.KazooClient')
    def test_valid_clustersate_file(self, mock_kazoo):

        class MockKazoo(object):
            def __init__(self):
                pass

            def start(self, *args, **kwargs):
                return True

            def get(self, *args, **kwargs):
                return ('{\n  "acsvc_test_collection":{\n    "shards":{"shard1":{\n        "range":"80000000-7fffffff",\n        "state":"active",\n        "replicas":{\n          "core_node1":{\n            "state":"active",\n            "core":"acsvc_test_collection_shard1_replica2",\n            "node_name":"mt3-bmsolr102:8080_solr",\n            "base_url":"http://mt3-bmsolr102:8080/solr"},\n          "core_node2":{\n            "state":"active",\n            "core":"acsvc_test_collection_shard1_replica1",\n            "node_name":"mt3-bmsolr101:8080_solr",\n            "base_url":"http://mt3-bmsolr101:8080/solr",\n            "leader":"true"}}}},\n    "maxShardsPerNode":"1",\n    "router":{"name":"compositeId"},\n    "replicationFactor":"2",\n    "autoAddReplicas":"false"},\n  "psvc_test_collection":{\n    "shards":{"shard1":{\n        "range":"80000000-7fffffff",\n        "state":"active",\n        "replicas":{\n          "core_node1":{\n            "state":"active",\n            "core":"psvc_test_collection_shard1_replica2",\n            "node_name":"mt3-bmsolr102:8080_solr",\n            "base_url":"http://mt3-bmsolr102:8080/solr"},\n          "core_node2":{\n            "state":"active",\n            "core":"psvc_test_collection_shard1_replica1",\n            "node_name":"mt3-bmsolr101:8080_solr",\n            "base_url":"http://mt3-bmsolr101:8080/solr",\n            "leader":"true"}}}},\n    "maxShardsPerNode":"1",\n    "router":{"name":"compositeId"},\n    "replicationFactor":"2",\n    "autoAddReplicas":"false"},\n  "bm_solr_entity_collection":{\n    "shards":{"shard1":{\n        "range":"80000000-7fffffff",\n        "state":"active",\n        "replicas":{\n          "core_node1":{\n            "state":"active",\n            "core":"bm_solr_entity_collection_shard1_replica1",\n            "node_name":"mt3-bmsolr101:8080_solr",\n            "base_url":"http://mt3-bmsolr101:8080/solr",\n            "leader":"true"},\n          "core_node2":{\n            "state":"active",\n            "core":"bm_solr_entity_collection_shard1_replica2",\n            "node_name":"mt3-bmsolr102:8080_solr",\n            "base_url":"http://mt3-bmsolr102:8080/solr"}}}},\n    "maxShardsPerNode":"1",\n    "router":{"name":"compositeId"},\n    "replicationFactor":"2",\n    "autoAddReplicas":"false"},\n  "qb_category_collection":{\n    "shards":{"shard1":{\n        "range":"80000000-7fffffff",\n        "state":"active",\n        "replicas":{\n          "core_node1":{\n            "state":"active",\n            "core":"qb_category_collection_shard1_replica1",\n            "node_name":"mt3-bmsolr101:8080_solr",\n            "base_url":"http://mt3-bmsolr101:8080/solr",\n            "leader":"true"},\n          "core_node2":{\n            "state":"active",\n            "core":"qb_category_collection_shard1_replica2",\n            "node_name":"mt3-bmsolr102:8080_solr",\n            "base_url":"http://mt3-bmsolr102:8080/solr"}}}},\n    "maxShardsPerNode":"1",\n    "router":{"name":"compositeId"},\n    "replicationFactor":"2",\n    "autoAddReplicas":"false"},\n  "mt3_qb_category_collection":{\n    "shards":{"shard1":{\n        "range":"80000000-7fffffff",\n        "state":"active",\n        "replicas":{\n          "core_node1":{\n            "state":"active",\n            "core":"mt3_qb_category_collection_shard1_replica1",\n            "node_name":"mt3-bmsolr102:8080_solr",\n            "base_url":"http://mt3-bmsolr102:8080/solr"},\n          "core_node2":{\n            "state":"active",\n            "core":"mt3_qb_category_collection_shard1_replica2",\n            "node_name":"mt3-bmsolr101:8080_solr",\n            "base_url":"http://mt3-bmsolr101:8080/solr",\n            "leader":"true"}}}},\n    "maxShardsPerNode":"1",\n    "router":{"name":"compositeId"},\n    "replicationFactor":"2",\n    "autoAddReplicas":"false"},\n  "mt3_qb_question_collection":{\n    "shards":{"shard1":{\n        "range":"80000000-7fffffff",\n        "state":"active",\n        "replicas":{\n          "core_node1":{\n            "state":"active",\n            "core":"mt3_qb_question_collection_shard1_replica1",\n            "node_name":"mt3-bmsolr102:8080_solr",\n            "base_url":"http://mt3-bmsolr102:8080/solr"},\n          "core_node2":{\n            "state":"active",\n            "core":"mt3_qb_question_collection_shard1_replica2",\n            "node_name":"mt3-bmsolr101:8080_solr",\n            "base_url":"http://mt3-bmsolr101:8080/solr",\n            "leader":"true"}}}},\n    "maxShardsPerNode":"1",\n    "router":{"name":"compositeId"},\n    "replicationFactor":"2",\n    "autoAddReplicas":"false"},\n  "mt2_qb_category_collection":{\n    "shards":{"shard1":{\n        "range":"80000000-7fffffff",\n        "state":"active",\n        "replicas":{\n          "core_node1":{\n            "state":"active",\n            "core":"mt2_qb_category_collection_shard1_replica1",\n            "node_name":"mt3-bmsolr102:8080_solr",\n            "base_url":"http://mt3-bmsolr102:8080/solr"},\n          "core_node2":{\n            "state":"active",\n            "core":"mt2_qb_category_collection_shard1_replica2",\n            "node_name":"mt3-bmsolr101:8080_solr",\n            "base_url":"http://mt3-bmsolr101:8080/solr",\n            "leader":"true"}}}},\n    "maxShardsPerNode":"1",\n    "router":{"name":"compositeId"},\n    "replicationFactor":"2",\n    "autoAddReplicas":"false"},\n  "mt2_qb_question_collection":{\n    "shards":{"shard1":{\n        "range":"80000000-7fffffff",\n        "state":"active",\n        "replicas":{\n          "core_node1":{\n            "state":"active",\n            "core":"mt2_qb_question_collection_shard1_replica2",\n            "node_name":"mt3-bmsolr102:8080_solr",\n            "base_url":"http://mt3-bmsolr102:8080/solr"},\n          "core_node2":{\n            "state":"active",\n            "core":"mt2_qb_question_collection_shard1_replica1",\n            "node_name":"mt3-bmsolr101:8080_solr",\n            "base_url":"http://mt3-bmsolr101:8080/solr",\n            "leader":"true"}}}},\n    "maxShardsPerNode":"1",\n    "router":{"name":"compositeId"},\n    "replicationFactor":"2",\n    "autoAddReplicas":"false"},\n  "qb_question_test_collection":{\n    "shards":{"shard1":{\n        "range":"80000000-7fffffff",\n        "state":"active",\n        "replicas":{\n          "core_node1":{\n            "state":"active",\n            "core":"qb_question_test_collection_shard1_replica2",\n            "node_name":"mt3-bmsolr101:8080_solr",\n            "base_url":"http://mt3-bmsolr101:8080/solr",\n            "leader":"true"},\n          "core_node2":{\n            "state":"active",\n            "core":"qb_question_test_collection_shard1_replica1",\n            "node_name":"mt3-bmsolr102:8080_solr",\n            "base_url":"http://mt3-bmsolr102:8080/solr"}}}},\n    "maxShardsPerNode":"1",\n    "router":{"name":"compositeId"},\n    "replicationFactor":"2",\n    "autoAddReplicas":"false"}}', )

            def stop(self):
                return True

        mock_kazoo.return_value = MockKazoo()
        result = self.zook_client.get_active_hosts()
        self.assertEqual(result.sort(),
                         [u'http://mt3-bmsolr102:8080',
                          u'http://mt3-bmsolr101:8080'].sort())

    @patch('kazoo.client.KazooClient')
    def test_no_clustersate_file(self, mock_kazoo):

        class MockKazoo(object):
            def __init__(self):
                pass

            def start(self, *args, **kwargs):
                return True

            def get(self, *args, **kwargs):
                return None

            def stop(self):
                return True

        mock_kazoo.return_value = MockKazoo()
        result = self.zook_client.get_active_hosts()
        self.assertEqual(result, [])

    @patch('kazoo.client.KazooClient')
    def test_bad_connection(self, mock_kazoo):

        class MockKazoo(object):
            def __init__(self):
                pass

            def start(self, *args, **kwargs):
                raise requests.exceptions.ConnectionError

            def get(self, *args, **kwargs):
                return None

            def stop(self):
                return True

        mock_kazoo.return_value = MockKazoo()
        result = self.zook_client.get_active_hosts()
        self.assertEqual(result, [])
