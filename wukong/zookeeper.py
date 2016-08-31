import kazoo.client
import json


class Zookeeper(object):
    """
    Retrieve the status of SOLR servers from Zookeeper
    """
    def __init__(self, hosts):
        self.hosts = hosts

    def get_active_hosts(self):
        """
        Get the current active SOLR hosts from Zookeeper
        """
        try:
            zk_client = kazoo.client.KazooClient(
                hosts=self.hosts,
                read_only=True
            )
            zk_client.start(timeout=5)
        except Exception:
            return []

        try:
            cluster_state_str = zk_client.get('/clusterstate.json')[0]
        except Exception:
            cluster_state_str = '{}'

        cluster_state = json.loads(cluster_state_str)

        active_nodes = set()
        for collection in cluster_state:
            for shard, shard_data in cluster_state[collection]['shards']\
                    .iteritems():
                for replica, replica_data in shard_data['replicas']\
                        .iteritems():
                    if replica_data['state'] == 'active':
                        node_url = replica_data['base_url'][:-5]
                        active_nodes.add(node_url)

        zk_client.stop()

        return list(active_nodes)
