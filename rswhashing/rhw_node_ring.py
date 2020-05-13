from pickle_hash import hash_code_hex
from server_config import NODES


class NodeRing:

    def __init__(self, nodes=NODES):
        assert len(nodes) > 0
        self.nodes = nodes

    def get_node(self, key):
        maximum_hash_value = None
        maximum_node = None
        for n in self.nodes:
            temp_key = (str(n['port']) + key).encode('utf-8')
            temporary_hash = int(hash_code_hex(temp_key), 16)
            if maximum_hash_value is None or temporary_hash > maximum_hash_value:
                maximum_hash_value = temporary_hash
                maximum_node = n

        return maximum_node


def test():
    ring = NodeRing(nodes=NODES)
    node = ring.get_node('9ad5794ec94345c4873c4e591788743a')
    print(node)
    print(ring.get_node('ed9440c442632621b608521b3f2650b8'))


# Uncomment to run the above local test via: python3 rhw_node_ring.py
# test()
