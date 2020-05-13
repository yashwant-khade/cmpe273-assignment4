from pickle_hash import hash_code_hex
from server_config import NODES


class NodeRing:

    def __init__(self, nodes=NODES):
        assert len(nodes) > 0
        self.nodes = nodes

    def get_node(self, key):
        max_hash = None
        max_node = None
        for node in self.nodes:
            encoded_key = (str(node['port']) + key).encode('utf-8')
            temp_hash = int(hash_code_hex(encoded_key), 16)
            if max_hash is None or temp_hash > max_hash:
                max_hash = temp_hash
                max_node = node

        return max_node


def test():
    ring = NodeRing(nodes=NODES)
    node = ring.get_node('9ad5794ec94345c4873c4e591788743a')
    print(node)
    print(ring.get_node('ed9440c442632621b608521b3f2650b8'))


# Uncomment to run the above local test via: python3 rhw_node_ring.py
# test()
