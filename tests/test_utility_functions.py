try:
    import unittest2 as unittest
except ImportError:
    import unittest

from wukong.zookeeper import _zk_data_to_dict
import json

class TestZKDataToDict(unittest.TestCase):
    def test_returns_dictionary(self):
        data = {'a': 'b'}
        self.assertDictEqual(data, _zk_data_to_dict(json.dumps(data)))


