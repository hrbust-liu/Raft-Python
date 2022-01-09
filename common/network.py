# coding: utf-8

import socket
import json

class Network:
    def __init__(self, addr, timeout = 2, maxBytes = 65536):
        self.addr = addr
        self.timeout = timeout
        self.maxBytes = maxBytes
        self.init_for_recv()

    def init_for_recv(self):
        self.s_recv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.s_recv.bind(self.addr)
        self.s_recv.settimeout(self.timeout)

        self.s_send = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def recv(self):
        msg, addr = self.s_recv.recvfrom(self.maxBytes)
        return json.loads(msg), addr
    
    def send(self, msg, addr):
        msg = json.dumps(msg).encode('utf-8')
        self.s_send.sendto(msg, addr)

