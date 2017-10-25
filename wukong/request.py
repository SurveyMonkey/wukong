import logging

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

logger = logging.getLogger()

def process_response(response):
    if response.status_code != 200:
        raise SolrError(response.reason)
    try:
        response_content = json.loads(response.text)
    except:
        logger.exception('Failed to parse solr text')
        raise SolrError("Parsing Error: %s" % response.text)

    return response_content


class SolrRequest(object):
    """
    Handle requests to SOLR and response from SOLR
    """

    REFRESH_FREQUENCY = 2

    def __init__(self, solr_hosts, zookeeper_hosts=None, timeout=15):
        self.client = requests.Session()
        self.master_hosts = solr_hosts
        self.zookeeper_hosts = zookeeper_hosts
        self.servers = []
        self.timeout = timeout
        self.current_hosts = self.master_hosts  # Backwards Compat
        self._zookeeper = None
        self._last_request = None
        self.attempt_zookeeper_refresh()

    @property
    def zookeeper(self):
        if self._zookeeper is None and self.zookeeper_hosts:
            self._zookeeper = Zookeeper(self.zookeeper_hosts)
        return self._zookeeper

    def attempt_zookeeper_refresh(self):
        zk = self.zookeeper
        if zk:
            logger.debug('Fetching solr from zookeeper')
            try:
                self.master_hosts = zk.get_active_hosts()
                self.current_hosts = self.master_hosts  # backward compat
                logger.info(
                    'Got solr nodes from zookeeper: %s',
                    ','.join(self.master_hosts)
                )
                if not self.master_hosts:
                    logger.error('Unable to find any solr nodes to make requests to')
                    raise SolrError("zookeeper reporting all SOLR nodes as down")
                return True
            except Exception as e:
                logger.exception('Failing to retrieve new SOLR hosts from zookeeper')
        return False

    def request(self, path, params, method, body=None, headers=None, is_retry=False):
        """
        Prepare data and send request to SOLR servers
        """
        request_headers = {
            'content-type': 'application/json',
        }
        if headers:
            request_headers.update(headers)

        # Refresh our list of hosts
        if (self.zookeeper and
            not is_retry and
            self._last_request and
            ((time.time() - self._last_request) / 60) > self.REFRESH_FREQUENCY):
            self.attempt_zookeeper_refresh()

        request_params = {
            'wt': 'json',
            'omitHeader': True,
            'json.nl': 'map'
        }
        if params:
            request_params.update(params)

        response = None
        for host in random.sample(self.master_hosts, len(self.master_hosts)):
            full_path = urljoin(host, path)
            try:
                logger.debug(
                    'Sending request to solr. host="%s" path="%s"',
                    host,
                    path
                )

                self._last_request = time.time()
                response = self.client.request(
                    method,
                    full_path,
                    params=request_params,
                    headers=request_headers,
                    data=body,
                    timeout=self.timeout
                )

                logger.debug(
                    'Retrieved response from SOLR. host="%s" path="%s" '
                    'status_code="%s"',
                    host,
                    path,
                    response.status_code
                )

                if response.status_code == 200:
                    # We've had a successful request. No need to keep trying
                    break
                else:
                    logger.info(
                        'Unsucessful request to SOLR'
                        'status_code="%s" reason="%s"',
                        response.status_code,
                        response.reason
                    )
                    response = None

            except ConnectionError:
                response = None
                logger.info(
                    'Failed to connect to SOLR',
                    exc_info=True
                )

        if not response:
            if not is_retry:
                refreshed = self.attempt_zookeeper_refresh()
                if refreshed:
                    return self.request(path, params, method, body=body,
                                        headers=headers, is_retry=True)
            raise SolrError('Unable to fetch from any SOLR nodes')

        return process_response(response)

    def post(self, path, params=None, body=None, headers=None):
        """
        Send a POST request to the SOLR servers
        """
        return self.request(path, params, 'POST', body=body, headers=headers)

    def get(self, path, params=None, headers=None):
        """
        Send a GET request to the SOLR servers
        """
        return self.request(path, params, 'GET', headers=headers)
