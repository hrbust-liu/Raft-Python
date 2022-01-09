
# coding: utf-8

import os
from random import randint
import time
import socket
import json
import sys
import random

from common.log import Logger

'''
Follower: 接收数据，反馈
Candidate:
Leader:

msg {
    "rpcType" : "RequestForVote/ResponseForVote/AppendEntries/AppendEntriesResponse"
    "sendRole" : "leader/follower/candidate/client"
    "ip"" : ""
    "data": ""
}

'''

class RaftServer:
    def __init__(self, port, peers):
        self.status = "Follower"
        self.logger = Logger().logger

        self.ip = "127.0.0.1"
        self.leader_ip_port = ("", 0)
        self.port = int(port)
        self.me = (self.ip, self.port)
        self.peers = []
        for ip_port in peers.split(','):
            node = self.str2ip_port(ip_port)
            if node == self.me:
                continue
            self.peers.append(node)

        self.logger.info("peers: %s"%(self.peers))
        # self.peers = [("127.0.0.1", 7800), ("127.0.0.1", 7801), ("127.0.0.1", 7802)]

        self.addr = ('127.0.01', self.port)
        self.init_for_recv()

        self.voteFor = ""
        self.log = []
        self.currentTerm = 0

        self.sendHeartTime = 1
        self.voteTime = 1

        self.select_leader_time = 0

    def str2ip_port(self, ip_port):
        return (ip_port.split(':')[0], int(ip_port.split(':')[1]))

    def init_for_recv(self):
        self.s_recv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.s_recv.bind(self.addr)
        self.s_recv.settimeout(2)

        self.s_send = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def recv(self):
        msg, addr = self.s_recv.recvfrom(65535)
        return json.loads(msg), addr
    
    def send(self, msg, addr):
        msg = json.dumps(msg).encode('utf-8')
        self.s_send.sendto(msg, addr)

    def become_follower(self):
        self.updateHeart()
        self.status = 'Follower'

    def become_candidate(self):
        self.updateHeart()
        self.status = "Candidate"
        self.voteNum = 1
        self.currentTerm += 1

        self.voteTime = {}
        for node in self.peers:
            self.voteTime[node] = 0

        self.vote_id = {}
        for node in self.peers:
            self.vote_id[node] = 0

        for node in self.peers:
            # if self.vote_id[node] == 0:
            request = {
                'rpcType': 'RequestForVote',
                'ip': self.ip,
                'port': self.port,
                'term': self.currentTerm
            }
            self.send(request, node)

    def become_leader(self):
        self.status = "Leader"
        self.beartTime = {}
        for node in self.peers:
            self.beartTime[node] = 0

    def updateHeart(self):
        self.select_leader_time = time.time() + 2 + random.randint(0, 1000)/1000.0

    def ShortToString(self):
        # Leader(127.0.0.1:1234): term(11)
        return "%s(%s:%s): term(%s)"%(self.status, self.ip, self.port, self.currentTerm)

    def do_follower(self):
        '''
            send: 无
            recv: 如果收到 vote -> 更高term则投票
                  如果收到 append -> 更新主，追加数据
                  如果收到 client -> 反错，返回Leader
        '''
        try:
            msg, addr = self.recv()
        except Exception as e:
            # 心跳超时，准备选主
            if time.time() > self.select_leader_time:
                self.become_candidate()
            return

        # 投票/追加数据
        if msg['rpcType'] == "RequestForVote":
            if msg['term'] > self.currentTerm:
                self.currentTerm = msg['term']
                self.voteFor = msg['ip'] + ":" + str(msg['port'])
                self.become_follower()
                self.updateHeart()
            elif msg['term'] < self.currentTerm:
                pass
            else:
                pass
        elif msg['rpcType'] == "AppendEntries":
            if msg['term'] > self.currentTerm:
                self.currentTerm = msg['term']
                self.leader_ip_port = (msg['leader_ip'], int(msg['leader_port']))
                self.voteFor = msg['ip'] + ":" + str(msg['port'])
                self.updateHeart()
            elif msg['term'] == self.currentTerm:
                self.updateHeart()
                if (msg['leader_ip'], int(msg['leader_port'])) == self.leader_ip_port:
                    pass
                else:
                    pass
            else:
                pass

    def do_candidate(self):
        '''
            send: 给每个成员发送 vote
            recv: 如果收到 vote -> 是否投票/是否存在更高term
                  如果收到 append -> 判断是否是更高term
                  如果收到 client -> 反错，通知NotLeader
        '''

        try:
            msg, addr = self.recv()
        except Exception as e:
            if time.time() > self.select_leader_time:
                self.become_candidate()
            msg = None
            addr = None

        if msg != None:
            if msg['rpcType'] == "RequestForVote":
                if msg['term'] > self.currentTerm:
                    request = msg
                    self.currentTerm = msg['term']
                    self.voteFor = msg['ip'] + ":" + str(msg['port'])
                    self.voteResponse(request)
                    self.become_follower()
                elif msg['term'] == self.currentTerm:
                    pass
                else:
                    pass
            if msg['rpcType'] == "ResponseForVote":
                if msg['term'] == self.currentTerm:
                    node = (msg['ip'], int(msg['port']))
                    self.vote_id[node] = msg['voteFor']
                    if (self.str2ip_port(msg['voteFor'])) == self.me:
                        print("I get %s vote"%(str(node)))
                        self.voteNum += 1
                        if self.voteNum * 2  >= len(self.peers) + 1:
                            self.become_leader()
                elif msg['term'] > self.currentTerm:
                    request = msg
                    self.currentTerm = msg['term']
                    self.voteFor = msg['ip'] + ":" + str(msg['port'])
                    self.voteResponse(request)
                    self.become_follower()
                else:
                    pass
            elif msg['rpcType'] == "AppendEntries":
                if msg['term'] >= self.currentTerm:
                    self.currentTerm = msg['term']
                    self.leader_ip_port = (msg['leader_ip'], int(msg['leader_port']))
                    self.voteFor = msg['ip'] + ":" + str(msg['port'])
                    self.become_follower()
                elif msg['term'] < self.currentTerm:
                    pass

    def do_leader(self):
        '''
            send: 定期发送数据
            recv: 如果收到 vote -> 更高term则投票
                  如果收到 append -> 更高term则降级为follower，更新主，追加数据
                  如果收到 client -> 接收数据，转发给follower
        '''
        now = time.time()
        for node in self.peers:
            if now > self.beartTime[node] + self.sendHeartTime:
                self.beartTime[node] = now
                request = {
                    "rpcType": "AppendEntries",
                    "log" : "",
                    "term" : self.currentTerm,
                    "ip" : self.ip,
                    "port" : self.port,
                    "leader_ip" : self.me[0],
                    "leader_port" : self.me[1],
                    "end" : "end"
                }
                self.send(request, node)

        try:
            msg, addr = self.recv()
        except Exception as e:
            msg = None
            addr = None
        if msg != None:
            if msg['rpcType'] == "AppendEntries":
                if msg['term'] > self.currentTerm:
                    self.currentTerm = msg['term']
                    self.leader_ip_port = (msg['leader_ip'], int(msg['leader_port']))
                    self.voteFor = msg['ip'] + ":" + str(msg['port'])
                    self.become_follower()
                elif msg['term'] < self.currentTerm:
                    pass
                else:
                    self.logger.error("[same term && double leader]: msg: %s"%(msg))
            elif msg['rpcType'] == "RequestForVote":
                request = msg
                if msg['term'] > self.currentTerm:
                    self.currentTerm = msg['term']
                    self.voteFor = msg['ip'] + ":" + str(msg['port'])
                    self.voteResponse(request)
                    self.become_follower()
                elif msg['term'] <= self.currentTerm:
                    pass

    def voteResponse(self, request):
        addr = (request["ip"], request["port"])
        data = {
            'rpcType': "ResponseForVote",
            'ip': self.ip,
            "port": self.port,
            "voteFor": self.voteFor,
            "term": self.currentTerm,
            "end": "end"
        }
        self.send(data, addr)

    def run(self):
        while True:
            print(self.ShortToString())
            if self.status == "Follower":
                self.do_follower()
            elif self.status == "Candidate":
                self.do_candidate()
            elif self.status == "Leader":
                self.do_leader()
            else:
                self.logger.error("status is %s"%(self.status))
                pass

if __name__ == "__main__":
    raft = RaftServer(sys.argv[1], sys.argv[2])
    raft.run()

    # test send/recv
    # raft.send({'hello':'world'}, ('127.0.0.1', int(sys.argv[1])))
    # raft.send({'hello':'world'}, ('127.0.0.1', int(sys.argv[1])))
    # print(raft.recv())
    # print(raft.recv())
    