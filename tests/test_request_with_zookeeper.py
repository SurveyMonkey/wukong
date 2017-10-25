import mock
from requests.exceptions import ConnectionError
from wukong.request import SolrRequest
from wukong.errors import *
import time
import json

try:
    import unittest2 as unittest
except ImportError:
    import unittest

class Response(object):
    pass

class TestSolrZookRequest(unittest.TestCase):

    client = SolrRequest(["http://localsolr:7070/solr/","http://localsolr:8080/solr/"],
                         zookeeper_hosts=["http://localzook:2181",
                                          "http://localzook:2181"])

    def test_request_post(self):

        client = SolrRequest(["http://localsolr:7070/solr/","http://localsolr:8080/solr/"],
                             zookeeper_hosts=["http://localzook:2181", "http://localzook:2181"])

        with mock.patch('wukong.request.SolrRequest.request') as mock_request:
            client.post(
                "fake_path",
                params={
                    "fake_param": "fake_value"
                },
                body={
                    "fake_data": "fake_value"
                }
            )

        mock_request.assert_called_once_with(
            'fake_path', {"fake_param": "fake_value"}, 'POST',
            body={
                "fake_data": "fake_value"
            },
            headers=None
        )

    def test_request_get(self):

        client = SolrRequest(["http://localsolr:7070/solr/","http://localsolr:8080/solr/"],
                             zookeeper_hosts=["http://localzook:2181", "http://localzook:2181"])

        with mock.patch('wukong.request.SolrRequest.request') as mock_request:
            client.get(
                "fake_path",
                params={
                    "fake_param": "fake_value"
                }
            )

        mock_request.assert_called_once_with(
            'fake_path', {"fake_param": "fake_value"}, 'GET', headers=None
        )

    def test_request_request__server_down(self):

        client = SolrRequest(["http://localsolr:7070/solr/","http://localsolr:8080/solr/"],
                             zookeeper_hosts=["http://localzook:2181", "http://localzook:2181"])

        with mock.patch('requests.sessions.Session.request') as mock_request:
            def request(*args, **kwargs):
                raise ConnectionError("Server down!")

            mock_request.side_effect = request

            with mock.patch('wukong.zookeeper.Zookeeper.get_active_hosts') as mock_zookeeper:
                def get_active_hosts():
                    return ["http://localsolr:7070/solr/"]

                mock_zookeeper.side_effect = get_active_hosts

                with self.assertRaises(SolrError) as cm:
                    response = client.request(
                        'fake_path',
                        {"fake_params": "fake_value"},
                        'GET',
                        body={"fake_body": "fake_value"}
                    )

                    mock_request.assert_any_call(
                        'GET', 'http://localsolr:8080/solr/fake_path',
                        params={
                            "fake_params": "fake_value",
                            'wt': 'json',
                            'omitHeader': 'true',
                            'json.nl': 'map'
                        },
                        headers={'content-type': 'application/json'},
                        data={"fake_body": "fake_value"},
                        timeout=15
                    )

                    self.assertEqual(client.current_hosts,
                                     ["http://localsolr:7070/solr/"])

                    mock_request.assert_any_call(
                        'GET', 'http://localsolr:7070/solr/fake_path',
                        params={
                            "fake_params": "fake_value",
                            'wt': 'json',
                            'omitHeader': 'true',
                            'json.nl': 'map'
                        },
                        headers={'content-type': 'application/json'},
                        data={"fake_body": "fake_value"},
                        timeout=15
                    )

            solr_error = cm.exception
            self.assertEqual(str(solr_error), "Unable to fetch from any SOLR nodes")

    def test_request_request__all_servers_down(self):

        client = SolrRequest(["http://localsolr:7070/solr/","http://localsolr:8080/solr/"],
                             zookeeper_hosts=["http://localzook:2181", "http://localzook:2181"])

        with mock.patch('requests.sessions.Session.request') as mock_request:
            def request(*args, **kwargs):
                raise ConnectionError("Server down!")

            mock_request.side_effect = request

            with mock.patch('wukong.zookeeper.Zookeeper.get_active_hosts') as mock_zookeeper:
                def get_active_hosts():
                    return []

                mock_zookeeper.side_effect = get_active_hosts

                with self.assertRaises(SolrError) as cm:
                    response = client.request(
                        'fake_path',
                        {"fake_params": "fake_value"},
                        'GET',
                        body={"fake_body": "fake_value"}
                    )

                    self.assertEqual(client.current_hosts, [])
                    solr_error = cm.exception
                    self.assertEqual(str(solr_error),
                                     "Unable to fetch from any SOLR nodes")

                with self.assertRaises(SolrError) as cm:
                    response2 = client.request(
                        'fake_path',
                        {"fake_params": "fake_value"},
                        'GET',
                        body={"fake_body": "fake_value"}
                    )

            solr_error = cm.exception
            self.assertEqual(str(solr_error),
                             "Unable to fetch from any SOLR nodes")

    def test_request_no_active_pool(self):
        with mock.patch('requests.sessions.Session.request') as mock_request:
            fake_response =  Response()
            fake_response.status_code = 200
            fake_response.text = json.dumps({'fake_data': 'fake_value'})
            mock_request.return_value = fake_response

            with mock.patch('wukong.zookeeper.Zookeeper.get_active_hosts') as mock_zookeeper:
                def get_active_hosts():
                    return []

                mock_zookeeper.side_effect = get_active_hosts
                client = SolrRequest(["http://localsolr:7070/solr/","http://localsolr:8080/solr/"],
                                     zookeeper_hosts=["http://localzook:2181", "http://localzook:2181"])
                with self.assertRaises(SolrError) as cm:
                    response = client.request(
                        'fake_path',
                        {"fake_params": "fake_value"},
                        'GET',
                        body={"fake_body": "fake_value"}
                    )

                solr_error = cm.exception
                self.assertEqual(str(solr_error),
                                 "Unable to fetch from any SOLR nodes")

            with mock.patch('wukong.zookeeper.Zookeeper.get_active_hosts') as mock_zookeeper:
                def get_active_hosts():
                    return ["http://localsolr:7070/solr/","http://localsolr:8080/solr/"]

                mock_zookeeper.side_effect = get_active_hosts
                response = client.request(
                    'fake_path',
                    {"fake_params": "fake_value"},
                    'GET',
                    body={"fake_body": "fake_value"}
                )

    def test_should_refresh(self):
        with mock.patch('requests.sessions.Session.request') as mock_request:
            fake_response =  Response()
            fake_response.status_code = 200
            fake_response.text = json.dumps({'fake_data': 'fake_value'})
            mock_request.return_value = fake_response

            with mock.patch('wukong.zookeeper.Zookeeper.get_active_hosts') as mock_zookeeper:
                def get_empty_active_hosts():
                    return []

                def get_active_hosts():
                    return ["http://localsolr:7070/solr/","http://localsolr:8080/solr/"]

                mock_zookeeper.side_effect = get_empty_active_hosts
                client = SolrRequest(["http://localsolr:7070/solr/","http://localsolr:8080/solr/"],
                                     zookeeper_hosts=["http://localzook:2181", "http://localzook:2181"])

                mock_zookeeper.side_effect = get_active_hosts
                assert client.master_hosts == []
                client._last_request = time.time() - (client.refresh_frequency * 1000 + 3000)
                response = client.request(
                    'fake_path',
                    {"fake_params": "fake_value"},
                    'GET',
                    body={"fake_body": "fake_value"},
                    headers={'foo': 'bar'}
                )
                assert client.master_hosts == get_active_hosts()
