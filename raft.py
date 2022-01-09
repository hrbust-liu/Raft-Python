
# coding: utf-8

from random import randint
import time
import json
import sys
import random

from common.log import Logger
from common.network import Network

'''
Follower: 接收数据，反馈
Candidate:
Leader:

msg {
    "rpcType" : "RequestForVote/ResponseForVote/AppendEntries/AppendEntriesResponse/UserRequest/UserResponse"
    "sendRole" : "leader/follower/candidate/client"
    "ip"" : ""
    "data": ""
}

'''

def str2ip_port(ip_port):
    return (ip_port.split(':')[0], int(ip_port.split(':')[1]))

class RaftServer:
    def __init__(self, port, peers):
        self.status = "Follower"
        self.logger = Logger().logger

        self.ip = "127.0.0.1"
        self.port = int(port)
        self.me = (self.ip, self.port)
        self.peers = []

        for ip_port in peers.split(','):
            node = str2ip_port(ip_port)
            if node == self.me:
                continue
            self.peers.append(node)
        
        self.peersNum = len(self.peers) + 1

        self.logger.info("peers: %s"%(self.peers))

        self.addrNet = ('127.0.01', self.port)
        self.addrStr = "127.0.0.1:" + str(self.port)

        self.net = Network(self.addrNet)

        self.voteFor = ""
        self.log = []
        self.currentTerm = 0

        self.sendHeartTime = 1
        self.voteTime = 1

        self.select_leader_time = 0

        self.commitIndex = 0
        self.applyIndex = 0
        self.nodeCommitIndex = {}
        self.nodeApplyIndex = {}

        self.applyNum = {}

        self.kv = {}


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
                'term': self.currentTerm,
                "addr": self.addrStr,
                'end': 'end'
            }
            self.net.send(request, node)

    def become_leader(self):
        self.status = "Leader"
        self.beartTime = {}
        for node in self.peers:
            self.beartTime[node] = 0
            self.nodeCommitIndex[node] = 0
        self.clientMap = {}

    def updateHeart(self):
        self.select_leader_time = time.time() + 2 + random.randint(0, 1000)/1000.0

    def ShortToString(self):
        # Leader(127.0.0.1:1234): term(11)
        return "%s(%s:%s): term(%s)"%(self.status, self.ip, self.port, self.currentTerm)

    def updateData(self, data):
        startLogIndex = int(data['startLogIndex'])
        endLogIndex = int(data['endLogIndex'])
        if startLogIndex > self.commitIndex or self.commitIndex >= endLogIndex:
            return
        logOffset = self.commitIndex - startLogIndex
        for index in range(self.commitIndex, endLogIndex):
            self.log.append(data['log'][logOffset + index])
        self.commitIndex = endLogIndex

    def AppendDateResponse(self):
        data = {
            'addr': self.me,
            "commitIndex": self.commitIndex,
            "applyIndex": self.applyIndex,
            "end": "end"
        }

    def do_follower(self):
        '''
            send: 无
            recv: 如果收到 vote -> 更高term则投票
                  如果收到 append -> 更新主，追加数据
                  如果收到 client -> 反错，返回Leader
        '''
        try:
            msg, addr = self.net.recv()
        except Exception as e:
            # 心跳超时，准备选主
            if time.time() > self.select_leader_time:
                self.become_candidate()
            msg = None
            addr = None
            return

        # 投票/追加数据
        if msg == None:
            pass
        elif msg['rpcType'] == "RequestForVote":
            if msg['term'] > self.currentTerm:
                self.currentTerm = msg['term']
                self.voteFor = msg['ip'] + ":" + str(msg['port'])
                self.become_follower()
                self.updateHeart()
            elif msg['term'] < self.currentTerm:
                pass
            else:
                pass
        elif msg['rpcType'] == "ResponseForVote":
            pass
        elif msg['rpcType'] == "AppendEntries":
            if msg['term'] >= self.currentTerm:
                self.currentTerm = msg['term']
                self.voteFor = msg['addr']
                self.updateHeart()
                self.updateData(msg)
                self.AppendDateResponse()
            else:
                pass
        elif msg['rpcType'] == "AppendEntriesResponse":
            pass
        elif msg['rpcType'] == 'UserRequest':
            self.UserResponse(msg['addr'], error = "NotLeader")

    def do_candidate(self):
        '''
            send: 给每个成员发送 vote
            recv: 如果收到 vote -> 是否投票/是否存在更高term
                  如果收到 append -> 判断是否是更高term
                  如果收到 client -> 反错，通知NotLeader
        '''

        try:
            msg, addr = self.net.recv()
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
                    self.voteFor = msg['addr']
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
                    if (str2ip_port(msg['voteFor'])) == self.me:
                        print("I get %s vote"%(str(node)))
                        self.voteNum += 1
                        if self.voteNum * 2  >= len(self.peers) + 1:
                            self.become_leader()
                elif msg['term'] > self.currentTerm:
                    request = msg
                    self.currentTerm = msg['term']
                    self.voteFor = msg['addr']
                    self.voteResponse(request)
                    self.become_follower()
                else:
                    pass
            elif msg['rpcType'] == "AppendEntries":
                if msg['term'] >= self.currentTerm:
                    self.currentTerm = msg['term']
                    self.voteFor = msg['addr']
                    self.become_follower()
                elif msg['term'] < self.currentTerm:
                    pass
            elif msg['rpcType'] == 'UserRequest':
                self.UserResponse(msg['addr'], error = "Candaditing")

    def do_leader(self):
        '''
            send: 定期发送数据
            recv: 如果收到 vote -> 更高term则投票
                  如果收到 append -> 更高term则降级为follower，更新主，追加数据
                  如果收到 client -> 接收数据，转发给follower
        '''
        now = time.time()
        for node in self.peers:
            startLogIndex = self.nodeCommitIndex[node]
            if startLogIndex < self.commitIndex or (now > self.beartTime[node] + self.sendHeartTime):
                self.AppendDateRequest(node, startLogIndex, self.commitIndex)

        try:
            msg, addr = self.net.recv()
        except Exception as e:
            msg = None
            addr = None
        if msg != None:
            if msg['rpcType'] == "AppendEntries":
                if msg['term'] > self.currentTerm:
                    self.currentTerm = msg['term']
                    self.voteFor = msg['addr']
                    self.become_follower()
                elif msg['term'] < self.currentTerm:
                    pass
                else:
                    self.logger.error("[same term && double leader]: msg: %s"%(msg))
            elif msg['rpcType'] == "RequestForVote":
                request = msg
                if msg['term'] > self.currentTerm:
                    self.currentTerm = msg['term']
                    self.voteFor = msg['addr']
                    self.voteResponse(request)
                    self.become_follower()
                elif msg['term'] <= self.currentTerm:
                    pass
            elif msg['rpcType'] == "ResponseForVote":
                pass
            elif msg['rpcType'] == 'UserRequest':
                print("收到User Request:%s"%(msg))
                # 发送给所有follower，如果过半同意，则回复client成功
                self.log.append(msg['key'] + ":" + msg['value'])
                self.clientMap[self.commitIndex] = msg['addr']
                self.applyNum[self.commitIndex] = 1
                self.commitIndex += 1
            elif msg['rpcType'] == 'AppendEntriesResponse':
                node = str2ip_port(msg['addr'])
                self.nodeCommitIndex[node] = int(msg['commitIndex'])
                self.nodeApplyIndex[node] = int(msg['applyIndex'])
                index = self.nodeCommitIndex[node]
                hasApplyIndex = self.applyIndex
                while index > hasApplyIndex:
                    self.applyNum[index] += 1
                    if self.applyNum[index] > self.peersNum/2:
                        self.applyIndex = hasApplyIndex
                        self.UserResponse(self.clientMap[hasApplyIndex])
                        key = self.log[hasApplyIndex].split(':')[0]
                        value = self.log[hasApplyIndex].split(':')[1]
                        self.kv[key] = value
                    hasApplyIndex += 1

    # append [startLogIndex, endLogIndex)
    def AppendDateRequest(self, node, startLogIndex, endLogIndex):
        self.beartTime[node] = time.time()
        request = {
            "rpcType": "AppendEntries",
            "log" : self.log[startLogIndex:endLogIndex],
            "startLogIndex": startLogIndex,
            "endLogIndex": endLogIndex,
            "applyIndex": self.applyIndex,
            "term" : self.currentTerm,
            "ip" : self.ip,
            "port" : self.port,
            "addr": self.addrStr,
            "end" : "end"
        }
        self.net.send(request, node)
        self.nodeCommitIndex[node] = endLogIndex


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
        self.net.send(data, addr)

    def UserResponse(self, addrStr, error = "OK"):
        addr = str2ip_port(addrStr)
        data = {
            'rpcType': 'UserResponse',
            'error': error,
            'voteFor': self.voteFor,
            "end" : "end"
        }
        self.net.send(data, addr)
        print("res: data: %s, addr: %s"%(data, addr))

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
    