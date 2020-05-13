import bisect

import mmh3

from pickle_hash import hash_code_hex
from server_config import NODES

VIRTUAL_LAYERS = 3
REPLICATION = 3


class NodeRing:

    def __init__(self, nodes=NODES, hashSize=3600):
        assert len(nodes) > 0
        self.nodes = nodes
        self.hashSize = hashSize - 1
        self.ring = {}
        self.sorted_keys = []
        self.virtualLayers = VIRTUAL_LAYERS
        self.replication = REPLICATION
        self.generate_ring()

    def generate_ring(self):

        for node in self.nodes:
            to_hash = (str(node['port'])).encode('utf-8')
            for i in range(0, self.virtualLayers):
                key = mmh3.hash(to_hash, i) % self.hashSize
                while key in self.ring:
                    key += 1
                self.sorted_keys.append(key)
                self.ring[key] = node
        self.sorted_keys.sort()

    def get_node(self, key_hash):
        encoded_hash = key_hash.encode('utf-8')
        key = int(hash_code_hex(encoded_hash), 16) % self.hashSize
        pos = bisect.bisect(self.sorted_keys, key)
        if pos >= len(self.sorted_keys) - 1:
            pos = 0
        replication_pos = pos + 1
        if replication_pos >= len(self.sorted_keys) - 1:
            replication_pos = 0
        nodeKey = self.sorted_keys[pos]
        replicationNodeKey = self.sorted_keys[replication_pos]
        return self.ring[nodeKey], self.ring[replicationNodeKey]


def test():
    ring = NodeRing(nodes=NODES, hashSize=3600)
    node = ring.get_node('9ad5794ec94345c4873c4e591788743a')
    print(node)
    print(ring.get_node('ed9440c442632621b608521b3f2650b8'))

# Uncomment to run the above local test via: python3 rhw_node_ring.py
# test()
