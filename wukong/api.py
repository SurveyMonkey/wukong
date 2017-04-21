import logging
import datetime as dt
import wukong.errors as solr_errors
from wukong.request import SolrRequest
from wukong.zookeeper import Zookeeper
import json

logger = logging.getLogger(__name__)


class SolrAPI(object):

    def __init__(self, solr_hosts, solr_collection,
                 zookeeper_hosts=None, timeout=15):
        """
        Do all the interactions with SOLR server
        (e.g. update, select, get and delete)

        :param solr_hosts: the hosts for SOLR.
        :type server: str

        :param solr_collection: the name of the collection in SOLR.
        :type solr_collection: str

        :param zookeeper_hosts: the hosts for zookeeper.
        :type zookeeper_hosts: str

        :param timeout: the timeout for request to SOLR.
        :type timeout: int

        """

        if solr_hosts is None and zookeeper_hosts is not None:
            logger.info(
                'Getting solr hosts from zookeeper for collection %s',
                solr_collection
            )
            zk = Zookeeper(zookeeper_hosts)
            solr_hosts = zk.get_active_hosts(collection_name=solr_collection)

        if solr_hosts is None or solr_collection is None:
            logger.error('Neither solr_hosts nor solr_collection has been set')
            raise solr_errors.SolrError(
                "Either solr_hosts or solr_collection can not be None"
            )

        if not isinstance(solr_hosts, list):
            solr_hosts = solr_hosts.split(",")

        if zookeeper_hosts is not None:
            hostnames, sep, chroot = zookeeper_hosts.rpartition('/')

            # If hostnames is empty then there is no chroot. Set it to empty.
            if not hostnames:
                chroot = ''
            else:
                chroot = '/%s' % chroot

            logger.debug('Using solr via zookeeper at chroot %s', chroot)

            self.zookeeper_hosts = [
                "http://%s%s" % (host, chroot,)
                for host in zookeeper_hosts.split(",")
            ]

            logger.info(
                'Connected to zookeeper hosts at %s',
                self.zookeeper_hosts
            )

        else:
            logger.debug('Not using zookeeper for SolrCloud')
            self.zookeeper_hosts = None

        logger.info('Connected to solr hosts %s', solr_hosts)
        self.solr_hosts = ["http://%s/solr/" % host for host in solr_hosts]

        self.solr_collection = solr_collection

        self.client = SolrRequest(
            solr_hosts=self.solr_hosts,
            zookeeper_hosts=zookeeper_hosts,
            timeout=timeout
        )

    def _get_collection_url(self, path):
        return "%s/%s" % (self.solr_collection, path)

    def is_alive(self):
        """
        Check if current collection is live from zookeeper.

        :return: weather or not if the collection is live
        :rtype: boolean
        """
        params = {'detail': 'true', 'path': '/clusterstate.json'}

        try:
            response = self.client.get('zookeeper', params)
        except solr_errors.SolrError:
            logger.exception('Failed to check zookeeper')
            return False
        else:
            try:
                data = json.loads(response['znode']['data'])
            except ValueError:
                return False

            for name, collection in data.items():
                shards = collection['shards']
                for shard, shard_info in shards.items():
                    replicas = shard_info['replicas']
                    for replica, info in replicas.items():
                        state = info['state']
                        if name == self.solr_collection and state != 'active':
                            return False

            return True

    def update(self, docs, commit=False):
        """
        Add new docs or updating existing docs.

        :param docs: a list of instances of SolrDoc.
        :type server: list

        :param commit: whether or not we should commit the documents.
        :type server: boolean

        """
        if not docs:
            return

        data = json.dumps(
            docs,
            default=lambda obj: obj.isoformat() if isinstance(
                obj, dt.datetime) else None
        )

        params = {}

        if commit:
            params['commit'] = 'true'

        return self.client.post(
            self._get_collection_url('update/json'),
            params=params,
            body=data
        )

    def select(self,
               query_dict,
               groups=False,
               facets=False,
               stats=False,
               **kwargs
               ):
        """
        Query documents from SOLR.

        :param query_dict: a dict containing the query params to SOLR
        :type query_dict: dict

        :param metadata: whether or not solr metadata should be returned
        :type metadata: boolean

        :param kwargs: a dict of additional params for SOLR
        :type kwargs: dict

        :return: reformatted response from SOLR
        :rtype: dict
        """

        if kwargs:
            query_dict.update(kwargs)

        response = self.client.get(
            self._get_collection_url('select'),
            params=query_dict
        )

        data = {}
        if groups and 'grouped' in response:
            data['groups'] = response['grouped']

        if facets and 'facet_counts' in response:
            data['facets'] = response['facet_counts']

        if stats and 'stats' in response:
            data['stats'] = response['stats']

        if 'response' in response and 'docs' in response['response']:
            response_data = response['response']
            data['docs'] = response_data['docs']
            data['total'] = response_data.get('numFound', len(data['docs']))

        return data

    def delete(self, unique_key, unique_key_value, commit=False):
        """
        Deleting a document from SOLR.

        :param unique_key: the unique key for the doc to delete
        :param unique_key_value: the value for the unique_key
        :param commit: whether or not we should commit the documents.
        :type server: boolean

        """
        params = {}

        if commit:
            params['commit'] = 'true'

        data = json.dumps({"delete": {"query": "%s:%s" %
                                      (unique_key, unique_key_value)}})

        return self.client.post(
            self._get_collection_url('update/json'),
            params=params,
            body=data
        )

    def commit(self):
        """
        Hard commit documents to SOLR.
        """
        params = {'commit': 'true'}

        return self.client.post(
            self._get_collection_url('update/json'), params=params)

    def get_schema(self):
        """
        Get the SOLR schema for the solr collection.

        :return: the schema for the current collection
        :rtype: dict
        """
        response = self.client.get(self._get_collection_url('schema'))

        return response.get('schema', {})

    def add_schema_fields(self, fields):
        """
        Add new fields to the schema of current collection

        :param fields: a list of dicts of fields.
        :type fields: list

        """
        if not fields:
            return

        data = json.dumps(fields)

        try:
            return self.client.post(
                self._get_collection_url('schema/fields'),
                body=data
            )
        except solr_errors.SolrError as e:
            raise solr_errors.SolrSchemaUpdateError(fields, message=e.args[0])
