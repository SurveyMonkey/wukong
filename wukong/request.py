from wukong.zookeeper import Zookeeper
from requests.exceptions import ConnectionError
from wukong.errors import SolrError
import requests
import random
import json
import time

try:
    from urlparse import urljoin
except:
    from urllib.parse import urljoin


def process_response(response):
    if response.status_code != 200:
        raise SolrError(response.reason)
    try:
        response_content = json.loads(response.text)
    except:
        raise SolrError("Parsing Error: %s" % response.text)

    return response_content


class SolrRequest(object):
    """
    Handle requests to SOLR and response from SOLR
    """
    def __init__(self, solr_hosts, zookeeper_hosts=None, timeout=15):
        self.client = requests.Session()
        self.master_hosts = solr_hosts
        self.current_hosts = solr_hosts
        self.servers = []
        self.timeout = timeout
        if zookeeper_hosts is not None:
            self.zookeeper = Zookeeper(zookeeper_hosts)
        else:
            self.zookeeper = None

        self.last_error = None
        # time to revert to old host list (in minutes) after an error
        self.check_hosts = 5

    def request(self, path, params, method, body=None):
        """
        Prepare data and send request to SOLR servers
        """
        def handle_error():
            # if the sdk knows where zookeeper lives
            if self.zookeeper is not None:
                self.last_error = time.time()
                # get the current list of active nodes from zookeeper
                curr_hosts = self.zookeeper.get_active_hosts()
                if len(curr_hosts) == 0:
                    # all nodes are down - so raise an error
                    raise SolrError("SOLR reporting all nodes as down")
                else:
                    # if all nodes are not down, update the current_hosts list
                    # and the servers list
                    self.current_hosts = curr_hosts
                    self.servers = curr_hosts

            host = self.servers.pop(0)
            return make_request(host, path)

        headers = {'content-type': 'application/json'}
        extraparams = {'wt': 'json',
                       'omitHeader': 'true',
                       'json.nl': 'map'}

        if params is None:
            params = {}

        params.update(extraparams)

        # if there hasn't been an error in 5 minutes, reset the solr_hosts
        if self.last_error is not None and \
                (time.time() - self.last_error) % 60 > self.check_hosts:

            self.current_hosts = self.master_hosts
            self.last_error = None

        def make_request(host, path):
            fullpath = urljoin(host, path)
            try:
                response = self.client.request(
                    method,
                    fullpath,
                    params=params,
                    headers=headers,
                    data=body,
                    timeout=self.timeout
                )

                # Connected to the node, but didn't get a successful response
                if (len(self.servers) > 0 and
                        hasattr(response, 'status_code') and
                        response.status_code != 200):
                    # try with another node
                    handle_error()

                return response

            # Didn't successfully connect to the node
            except ConnectionError as e:
                if len(self.servers) > 0:
                    # try with another node
                    handle_error()

                raise SolrError(str(e))

        self.servers = list(self.current_hosts)
        if len(self.servers) == 0:
            handle_error()

        random.shuffle(self.servers)
        host = self.servers.pop(0)

        response = make_request(host, path)
        return process_response(response)

    def post(self, path, params=None, body=None):
        """
        Send a POST request to the SOLR servers
        """
        return self.request(path, params, 'POST', body=body)

    def get(self, path, params=None):
        """
        Send a GET request to the SOLR servers
        """
        return self.request(path, params, 'GET')
