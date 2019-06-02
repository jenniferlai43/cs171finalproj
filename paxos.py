import time
import pickle
import math
import json
import socket
import threading
from messages import createAcceptMsg, createPrepareMsg, createPrepareAck, createAcceptAck, createDecisionMsg, randDelay

#servers = ['A', 'B', 'C', 'D', 'E']
servers = ['A', 'B']

with open('config.json') as f:
	config = json.load(f)

class Ballot:
	def __init__(self, seqNum, pid, depth):
		self.seqNum = seqNum
		self.pid = pid
		self.depth = depth
	def __gt__(self, other):
		if(self.seqNum > other.seqNum):
			return True
		elif(self.seqNum == other.seqNum):
			if (self.pid > other.pid):
				return True
		else:
			return False
	def __ge__(self, other):
		if(self.seqNum >= other.seqNum):
			return True
		elif(self.seqNum == other.seqNum):
			if (self.pid >= other.pid):
				return True
		else:
			return False
	def __lt__(self, other):
		if(self.seqNum < other.seqNum):
			return True
		elif(self.seqNum == other.seqNum):
			if (self.pid < other.pid):
				return True
		else:
			return False
	def __str__(self):
		return "<{},{},{}>".format(self.seqNum, self.pid, self.depth)

class Proposer:
	def __init__(self, sMeta, config, depth = 0, seqNum = 0):
		self.balNum = None
		self.sMeta = sMeta
		self.val = None
		self.name = sMeta["name"]
		self.majority = 2 #math.ceil(config["server-count"]/2)
		self.config = config
		self.prepAckCount = 0
		self.prepAckMsgList = []
		self.acceptAckCount = 0
	def broadcast(self, phase):
		for s in servers:
			#if (s != self.name): <-- send to own acceptor too!
			t1 = threading.Thread(target=Proposer.randDelayMsg, args=(self, phase, self.config[s],))
			t1.start()


	def randDelayMsg(self, phase, proc):
		b = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		b.connect((proc["ip-addr"], proc["port"]))
		msg = {}
		if (phase == "PREPARE"):
			msg = createPrepareMsg(self)
		elif (phase == "ACCEPT"): #accept
			msg = createAcceptMsg(self)
		else: #decision
			msg = createDecisionMsg(self)
		encMsg = pickle.dumps(msg)
		time.sleep(randDelay())
		b.sendall(encMsg)
		b.close()

	def createBallot(self, proposedVal, proposedDepth,):
		self.val = proposedVal
		if (self.balNum is None):
			self.balNum = Ballot(0, self.sMeta["pid"], proposedDepth)
		else:
			newSeqNum = self.balNum.seqNum + 1
			self.balNum = Ballot(newSeqNum, self.sMeta["pid"], proposedDepth)
		print("Sending PREPARE with ballot number ", str(self.balNum))
		self.broadcast("PREPARE")
	def handlePrepAck(self, msg):
		print("Received PREP-ACK with ballot number ", str(self.balNum))
		self.prepAckMsgList.append(msg)
		self.prepAckCount += 1
		if (self.prepAckCount == self.majority):
			maxBalNum = -1;
			#myVal = self.val
			for m in self.prepAckMsgList:
				if m["accept-val"] is not None and m["accept-num"] > maxBalNum:
					maxBalNum = m["accept-num"]
					#myVal = m["accept-val"]
					self.val = myVal
			print("Sending ACCEPT with ballot number ", str(self.balNum))
			self.broadcast("ACCEPT")
	def handleAcceptAck(self):
		print("Received ACCEPT-ACK with ballot number ", str(self.balNum))
		self.acceptAckCount += 1
		if (self.acceptAckCount == self.majority):
			print("Sending DECISION with ballot number ", str(self.balNum))
			self.broadcast("DECISION")
		

class Acceptor:
	def __init__(self, sMeta):
		self.sMeta = sMeta
		self.acceptNum = None
		self.acceptVal = None
		self.minBal = None
	def recvPrepare(self, msg):
		print("Received PREPARE with ballot number ", str(msg["bal-num"]))
		if (self.minBal is None or msg["bal-num"] > self.minBal):
			self.minBal = msg["bal-num"]
			proc = config[msg["src-name"]]
			b = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			b.connect((proc["ip-addr"], proc["port"]))
			msg = createPrepareAck(self)
			encMsg = pickle.dumps(msg)
			time.sleep(randDelay())
			b.sendall(encMsg)
			b.close()
			print("Sent PREP-ACK with ballot number ", str(msg["accept-num"]))
	def recvAccept(self, msg):
		print("Received ACCEPT with ballot number ", str(msg["bal-num"]))
		if (msg["bal-num"] >= self.minBal):
			self.acceptNum = msg["bal-num"]
			self.minBal = msg["bal-num"]
			self.acceptVal = msg["val"]
			proc = config[msg["src-name"]]
			b = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			b.connect((proc["ip-addr"], proc["port"]))
			msg = createAcceptAck(self)
			encMsg = pickle.dumps(msg)
			time.sleep(randDelay())
			b.sendall(encMsg)
			b.close()
			print("Sent ACCEPT-ACK with ballot number ", str(msg["accept-num"]))










