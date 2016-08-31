import json

try:
    import unittest2 as unittest
except ImportError:
    import unittest


class BaseTestCase(unittest.TestCase):
    def _make_request(self):
        from pyramid.testing import DummyRequest
        request = DummyRequest()
        return request

    def _make_response(self, data, status_code=200):
        from requests.models import Response
        response = Response()
        response._content = json.dumps(data).encode('utf-8')
        response.status_code = status_code
        return response