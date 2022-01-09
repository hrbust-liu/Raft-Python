# coding: utf-8

import sys

from common.log import Logger
from common.network import Network
from raft import str2ip_port

class Client:
    def __init__(self, port, peers):
        self.addrNode = ('127.0.0.1', port)
        self.addr = "127.0.0.1:" + str(port)
        self.net = Network(self.addrNode)

        self.peers = []
        self.peersAddr = {}
        self.peersNum = 0
        for ip_port in peers.split(','):
            node = str2ip_port(ip_port)
            self.peers.append(node)
            self.peersAddr[self.peersNum] = node
            self.peersNum += 1

    def put(self, key, value):
        data = {
            "rpcType" : "UserRequest",
            "addr": self.addr,
            "key": key,
            "value": value,
            "end": "end"
        }
        index = 0
        while True:
            self.net.send(data, self.peersAddr[index])
            try:
                data, addr = self.net.recv()
            except Exception as e:
                data = None
                addr = None

            if data == None:
                index += 1
            elif data['error'] == 'OK':
                return
            elif data['error'] == 'Candidating':
                index += 1
            elif data['error'] == 'NotLeader':
                index += 1
                # index = self.Node2Index[data['']]
            if index == self.peersNum:
                index = 0


if __name__ == '__main__':
    client = Client(int(sys.argv[1]), sys.argv[2])
    client.put("hello", "world")