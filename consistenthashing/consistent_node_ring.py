import bisect

import mmh3

from pickle_hash import hash_code_hex
from server_config import NODES


class NodeRing:

    def __init__(self, nodes=NODES, hashSize=3600):
        assert len(nodes) > 0
        self.nodes = nodes
        self.hash_size = hashSize - 1
        self.ring = {}
        self.sorted_keys = []
        self.virtual_nodes = 3
        self.generate_ring()

    def generate_ring(self):

        for node in self.nodes:
            to_hash = (str(node['port'])).encode('utf-8')
            for i in range(0, self.virtual_nodes):
                key = mmh3.hash(to_hash, i) % self.hash_size
                while key in self.ring:
                    key += 1
                self.sorted_keys.append(key)
                self.ring[key] = node
        self.sorted_keys.sort()

    def get_node(self, hash_key):
        encoded_hash = hash_key.encode('utf-8')
        key = int(hash_code_hex(encoded_hash), 16) % self.hash_size
        position = bisect.bisect(self.sorted_keys, key)
        if position >= len(self.sorted_keys) - 1:
            position = 0
        data_rep_position = position + 1
        if data_rep_position >= len(self.sorted_keys) - 1:
            data_rep_position = 0
        node_key = self.sorted_keys[position]
        rep_node_key = self.sorted_keys[data_rep_position]
        return self.ring[node_key], self.ring[rep_node_key]


def test():
    ring = NodeRing(nodes=NODES, hashSize=3600)
    node = ring.get_node('9ad5794ec94345c4873c4e591788743a')
    print(node)
    print(ring.get_node('ed9440c442632621b608521b3f2650b8'))

# Uncomment to run the above local test via: python3 rhw_node_ring.py
# test()
