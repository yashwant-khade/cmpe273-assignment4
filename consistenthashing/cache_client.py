import socket

from consistent_node_ring import NodeRing
from pickle_hash import serialize_GET, serialize_PUT
from sample_data import USERS

BUFFER_SIZE = 1024


class UDPClient:

    def send(self, request, server):
        print('Connecting to server at {}:{}'.format(
            server['host'], server['port']))
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.sendto(request, (server['host'], server['port']))
            response, ip = s.recvfrom(BUFFER_SIZE)
            return response
        except socket.error:
            print("Error! {}".format(socket.error))
            exit()


def process():
    node_ring = NodeRing()
    hash_codes = set()
    # PUT all users.
    for u in USERS:
        data_bytes, key = serialize_PUT(u)
        server, replication_server = node_ring.get_node(key)
        response = UDPClient().send(data_bytes, server)
        replication_response = UDPClient().send(data_bytes, replication_server)
        print(response)
        print(replication_response)
        hash_codes.add(str(response.decode()))
        hash_codes.add(str(replication_response.decode()))

    print(f"Number of Users={len(USERS)}\nNumber of Users Cached={len(hash_codes)}")

    # GET all users.
    for hc in hash_codes:
        print(hc)
        data_bytes, key = serialize_GET(hc)
        server, replication_server = node_ring.get_node(key)
        response = UDPClient().send(data_bytes, server)
        print(response)


if __name__ == "__main__":
    process()
