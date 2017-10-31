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
except ImportError: # pragma: no cover
    from urllib.parse import urljoin

logger = logging.getLogger(__name__)


def process_response(response):
    try:
        response_content = json.loads(response.text)
    except Exception:
        logger.exception('Failed to parse solr text')
        raise SolrError("Parsing Error: %s" % response.text)

    return response_content


class SolrRequest(object):
    """
    Handle requests to SOLR and response from SOLR
    """

    def __init__(
        self,
        solr_hosts,
        zookeeper_hosts=None,
        timeout=15,
        refresh_frequency=2,
        zookeeper_timeout=5
    ):
        """
        Initialize our Request interface instance.
            :param solr_hosts: [(str)] List of SOLR hostnames.
            :param zookeeper_hosts: [(str)] (Optional) List of zookeeper hostnames.
            :param timeout: int - Timeout in seconds for requests to SOLR. (Default: 15s) 
            :param refresh_frequency: int - Frequency in minutes to refresh the SOLR hostnames from zookeeper 
                (time since the last refresh, but synchronous with a request).(Default: 2m)
            :param zookeeper_timeout: int - Timeout in seconds for requests to SOLR (Default: 5s)
        """
        self.client = requests.Session()
        self.master_hosts = solr_hosts
        self.zookeeper_hosts = zookeeper_hosts
        self.refresh_frequency = refresh_frequency  # minutes
        self.servers = []
        self.timeout = timeout
        self._zookeeper = None
        self._last_request = None
        self.zookeeper_timeout = zookeeper_timeout
        self.attempt_zookeeper_refresh()

    @property
    def zookeeper(self):
        if self._zookeeper is None and self.zookeeper_hosts:
            self._zookeeper = Zookeeper(
                self.zookeeper_hosts,
                self.zookeeper_timeout
            )
        return self._zookeeper

    @property
    def current_hosts(self):
        return self.master_hosts

    def attempt_zookeeper_refresh(self):
        if self.zookeeper:
            logger.debug('Fetching solr hosts from zookeeper')
            try:
                self.master_hosts = self.zookeeper.get_active_hosts()
                logger.info(
                    'Got solr nodes from zookeeper: %s',
                    ','.join(self.master_hosts)
                )
                if not self.master_hosts:
                    logger.error('Unable to find any solr nodes to make requests to. Zookeeper reporting all SOLR nodes as down')
                return bool(self.master_hosts)
            except Exception:
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

        request_params = {
            'wt': 'json',
            'omitHeader': 'true',
            'json.nl': 'map'
        }
        if params:
            request_params.update(params)

        # Refresh our list of hosts
        should_refresh = (
            self.zookeeper and
            not is_retry and
            self._last_request and
            ((time.time() - self._last_request) / 60) > self.refresh_frequency
        )
        if should_refresh:
            self.attempt_zookeeper_refresh()

        response = None
        for host in random.sample(self.master_hosts, len(self.master_hosts)):
            full_path = '/'.join(s.strip('/') for s in [host, path])
            try:
                logger.debug('Sending request to solr. route="%s"', full_path)

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
                    'Retrieved response from SOLR. route="%s" status_code="%s"',
                    full_path,
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
