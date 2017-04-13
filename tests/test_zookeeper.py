from mock import MagicMock, patch
from wukong.zookeeper import Zookeeper
import requests
import json

try:
    import unittest2 as unittest
except ImportError:
    import unittest


class TestZookeeper(unittest.TestCase):

    zook_client = Zookeeper("http://localzook01:2181,http://localzook02:2181")

    @patch('kazoo.client.KazooClient')
    def test_valid_clusterstate_file(self, mock_kazoo):

        class MockKazoo(object):
            def __init__(self):
                pass

            def start(self, *args, **kwargs):
                return True

            def get_children(self, *args, **kwargs):
                return ['test_collection_one']

            def get(self, *args, **kwargs):
                data =  json.dumps({
                    'test_collection_one':{
                        'shards':{
                            'shard1':{
                                'range': '80000000-7fffffff',
                                'state': 'active',
                                'replicas':{
                                    'core_node1':{
                                        'state': 'active',
                                        'core': 'test_collection_one_shard1_replica2',
                                        'node_name': '127.0.0.1:8080_solr',
                                        'base_url': 'http://127.0.0.1:8080/solr',
                                        'leader': 'true',
                                    },
                                    'core_node2':{
                                        'state': 'active',
                                        'core': 'test_collection_one_shard1_replica1',
                                        'node_name': '127.0.0.1:9090_solr',
                                        'base_url': 'http://127.0.0.1:9090/solr',
                                    }
                                }
                            }
                        },
                        'maxShardsPerNode': '1',
                        'router':{'name': 'compositeId'},
                        'replicationFactor': '2',
                        'autoAddReplicas': 'false'}
                    })

                return (data,) # Note that this is a tuple on purpose

            def stop(self):
                return True

        mock_kazoo.return_value = MockKazoo()
        result = self.zook_client.get_active_hosts()
        self.assertEqual(
            result.sort(),
            [u'http://127.0.0.1:8080', u'http://127.0.0.1:9090'].sort()
        )

    @patch('kazoo.client.KazooClient')
    def test_no_clusterstate_file(self, mock_kazoo):

        class MockKazoo(object):
            def __init__(self):
                pass

            def start(self, *args, **kwargs):
                return True

            def get_children(self, *args, **kwargs):
                return []

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

            def get_children(self, *args, **kwargs):
                return []

            def get(self, *args, **kwargs):
                return None

            def stop(self):
                return True

        mock_kazoo.return_value = MockKazoo()
        result = self.zook_client.get_active_hosts()
        self.assertEqual(result, [])

    @patch('kazoo.client.KazooClient')
    def test_get_aliases(self, mock_kazoo):
        class MockKazoo(object):
            def __init__(self):
                pass

            def start(self, *args, **kwargs):
                return True

            def get_children(self, *args, **kwargs):
                return ['test_collection_one']

            def get(self, *args, **kwargs):
                data = ''
                if args and args[0] == '/aliases.json':
                    data = json.dumps({
                        'collection': {
                            'my_alias': 'test_collection_one',
                        }
                    })
                else:
                    data = json.dumps({
                        'test_collection_one':{
                            'shards':{
                                'shard1':{
                                    'range': '80000000-7fffffff',
                                    'state': 'active',
                                    'replicas':{
                                        'core_node1':{
                                            'state': 'active',
                                            'core': 'test_collection_one_shard1_replica2',
                                            'node_name': '127.0.0.1:8080_solr',
                                            'base_url': 'http://127.0.0.1:8080/solr',
                                            'leader': 'true',
                                        },
                                        'core_node2':{
                                            'state': 'active',
                                            'core': 'test_collection_one_shard1_replica1',
                                            'node_name': '127.0.0.1:9090_solr',
                                            'base_url': 'http://127.0.0.1:9090/solr',
                                        }
                                    }
                                }
                            },
                            'maxShardsPerNode': '1',
                            'router':{'name': 'compositeId'},
                            'replicationFactor': '2',
                            'autoAddReplicas': 'false'}
                        })

                return (data,) # Note that this is a tuple on purpose

            def stop(self):
                return True

        mock_kazoo.return_value = MockKazoo()
        result = self.zook_client._get_active_hosts()
        print(result)
        self.assertEqual(
            result['test_collection_one'],
            result['my_alias']
        )
