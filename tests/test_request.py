import mock
from requests.exceptions import ConnectionError, ReadTimeout
from wukong.request import SolrRequest
from wukong.errors import *
import pytest
import json

try:
    import unittest2 as unittest
except ImportError:
    import unittest

class Response(object):
    pass

class TestSolrRequest(unittest.TestCase):

    client = SolrRequest(["http://localsolr:7070/solr/","http://localsolr:8080/solr/"])

    def test_request_post(self):
        with mock.patch('wukong.request.SolrRequest.request') as mock_request:
         	self.client.post(
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
     		}
        )

    def test_request_get(self):
        with mock.patch('wukong.request.SolrRequest.request') as mock_request:
         	self.client.get(
         		"fake_path",
         		params={
         			"fake_param": "fake_value"
         		}
         	)

        mock_request.assert_called_once_with(
                'fake_path', {"fake_param": "fake_value"}, 'GET'
        )

    def test_request_request(self):
    	client = SolrRequest(["http://localsolr:8080/solr/"])

    	with mock.patch('requests.sessions.Session.request') as mock_request:
            fake_response =  Response()
            fake_response.status_code = 200
            fake_response.text = json.dumps({'fake_data': 'fake_value'})
            mock_request.return_value = fake_response
            response = client.request(
            	'fake_path',
            	{"fake_params": "fake_value"},
            	'GET',
            	body={"fake_body": "fake_value"}
            )

            mock_request.assert_called_once_with(
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

            assert response == {'fake_data': 'fake_value'}

    def test_request_request__empty_params(self):
        client = SolrRequest(["http://localsolr:8080/solr/"])

        with mock.patch('requests.sessions.Session.request') as mock_request:
            fake_response =  Response()
            fake_response.status_code = 200
            fake_response.text = json.dumps({'fake_data': 'fake_value'})
            mock_request.return_value = fake_response
            response = client.request(
                'fake_path',
                None,
                'GET',
                body={"fake_body": "fake_value"}
            )

        mock_request.assert_called_once_with(
            'GET', 'http://localsolr:8080/solr/fake_path',
            params={
                'wt': 'json',
                'omitHeader': 'true',
                'json.nl': 'map'
            },
            headers={'content-type': 'application/json'},
            data={"fake_body": "fake_value"},
            timeout=15
        )


    def test_request_request__status_code(self):

        with mock.patch('requests.sessions.Session.request') as mock_request:
            fake_response =  Response()
            fake_response.status_code = 400
            fake_response.reason = "Test Error"
            mock_request.return_value = fake_response
            with self.assertRaises(SolrError) as cm:
                response = self.client.request(
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
            self.assertEqual(str(solr_error), "Test Error" )

    def test_request_request__malformed_response(self):
        client = SolrRequest(["http://localsolr:8080/solr/"])

        with mock.patch('requests.sessions.Session.request') as mock_request:
            fake_response =  Response()
            fake_response.status_code = 200
            fake_response.text = "Malformed Response"
            mock_request.return_value = fake_response
            with self.assertRaises(SolrError) as cm:
                response = client.request(
                    'fake_path',
                    {"fake_params": "fake_value"},
                    'GET',
                    body={"fake_body": "fake_value"}
                )

            mock_request.assert_called_once_with(
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

            solr_error = cm.exception
            self.assertEqual(str(solr_error), "Parsing Error: Malformed Response" )

    def test_request_request__server_down(self):

        with mock.patch('requests.sessions.Session.request') as mock_request:
            def request(*args, **kwargs):
                raise ConnectionError("Server down!")

            mock_request.side_effect = request
            with self.assertRaises(SolrError) as cm:
                response = self.client.request(
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
            self.assertEqual(str(solr_error), "Server down!" )
