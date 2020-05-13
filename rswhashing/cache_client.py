import socket

from pickle_hash import serialize_GET, serialize_PUT
from rhw_node_ring import NodeRing
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
        server = node_ring.get_node(key)
        response = UDPClient().send(data_bytes, server)
        print(response)
        hash_codes.add(str(response.decode()))

    print(
        f"Number of Users={len(USERS)}\nNumber of Users Cached={len(hash_codes)}")

    # GET all users.
    for hc in hash_codes:
        print(hc)
        data_bytes, key = serialize_GET(hc)
        server = node_ring.get_node(key)
        response = UDPClient().send(data_bytes, server)
        print(response)


if __name__ == "__main__":
    process()
