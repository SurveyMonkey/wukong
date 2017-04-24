import kazoo.client
from kazoo.exceptions import NoNodeError
import json
from collections import defaultdict
import itertools

import logging
logger = logging.getLogger(__name__)


def _get_hosts_from_state(state):
    """
    Given a SOLR state json blob, extract the active hosts

    :param state dict: SOLR state blob
    :returns: list[str]
    """
    active_nodes = set()
    for shard, shard_data in state.get('shards', {}).items():
        replicas = shard_data['replicas']
        for replica, replica_data in replicas.items():
            if replica_data['state'] == 'active':
                node_url = replica_data['base_url'][:-5]  # strip /solr
                node_url = node_url.replace('http://', '')
                active_nodes.add(node_url)

    return active_nodes


def _zk_data_to_dict(data):
    """json load data retreived with zk_client.get()"""
    if not isinstance(data, str):
        data = data.decode('utf-8')
    return json.loads(data)


class Zookeeper(object):
    """
    Retrieve the status of SOLR servers from Zookeeper
    """
    def __init__(self, hosts):
        self.hosts = hosts

    def _get_active_hosts(self):
        active_hosts = defaultdict(set)

        try:
            zk_client = kazoo.client.KazooClient(
                hosts=self.hosts,
                read_only=True
            )
            zk_client.start(timeout=5)
        except Exception:
            return active_hosts

        try:
            collections = zk_client.get_children('/collections')
        except NoNodeError:
            # No collections have been created on the zookeeper host yet.
            logger.debug('No collections found')
            collections = []

        # Fetch any potential collection aliases from solr
        aliases = {}
        try:
            aliases = _zk_data_to_dict(zk_client.get('/aliases.json')[0])
            if aliases is None:  # If there are no aliases, we'll get None
                aliases = {}
        except:
            logger.debug('No /aliases.json file found')

        # Handle SOLR 6+ style state.json paths
        for collection in collections:
            try:
                state_path = '/collections/{}/state.json'.format(collection)
                state_json = zk_client.get(state_path)[0]
            except NoNodeError:
                logger.debug(
                    'No SOLR 6 state found for collection [%s]',
                    collection
                )
            else:
                state = _zk_data_to_dict(state_json)
                active_hosts[collection] |= _get_hosts_from_state(
                    state.get(collection, {})
                )

        # Handle SOLR <6 style clusterstate.json
        try:
            cluster_state_str = zk_client.get('/clusterstate.json')[0]
        except Exception:
            cluster_state_str = '{}'

        cluster_state = _zk_data_to_dict(cluster_state_str)

        for collection_name, state in cluster_state.items():
            hosts = _get_hosts_from_state(state)
            active_hosts[collection_name] |= hosts

        logger.error('Got aliases: %s', aliases)
        for alias_name, member_string in aliases.get('collection', {}).items():
            members = member_string.split(',')

            # Only support single member aliases for now, because we need to
            # be able to fetch schemas.
            member = members[0]

            active_hosts[alias_name] = active_hosts[member].copy()

        zk_client.stop()

        return active_hosts

    def get_active_hosts(self, collection_name=None):
        """
        Get the current active SOLR hosts from Zookeeper

        :param collection_name: If provided, the name of a SOLR collection to
                                get the hosts for. If not provided, all solr
                                hosts will be returned.
                                Optional.

        :returns list[str]: A list of solr nodes in the form `http://hostname`
        """
        logger.debug('Getting active hosts for collection %s', collection_name)
        active_hosts = self._get_active_hosts()

        if collection_name is not None:
            return list(active_hosts.get(collection_name, []))
        else:
            return list(itertools.chain.from_iterable(
                self._get_active_hosts().values()
            ))
