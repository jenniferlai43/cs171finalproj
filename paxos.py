import time
import pickle
import math
import json
import socket
import threading
from messages import createAcceptMsg, createPrepareMsg, createPrepareAck, createAcceptAck, createDecisionMsg, randDelay
import errno
from socket import error as socket_error
from network import PARTITION


servers = ['A', 'B', 'C', 'D', 'E']

lock = threading.Lock()

with open('config.json') as f:
	config = json.load(f)

### Ballot with overloaded comparators
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
		if (self.seqNum == other.seqNum and self.pid == other.pid and self.depth == other.depth):
			return True
		elif(self.seqNum > other.seqNum):
			return True
		elif(self.seqNum == other.seqNum):
			if (self.pid > other.pid):
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

# Proposer class
class Proposer:
	def __init__(self, sMeta, config, depth = 0, seqNum = 0):
		self.curSeqNum = 0
		self.balNum = None
		self.sMeta = sMeta
		self.val = None
		self.name = sMeta["name"]
		self.majority = math.ceil(config["server-count"]/2) 
		self.config = config
		self.prepAckCount = 0
		self.prepAckMsgList = []
		self.acceptAckCount = 0
	# Broadcast function point to point
	def broadcast(self, phase):
		threads = []
		for s in servers:
			if (s in PARTITION[self.sMeta["name"]]):
				t = threading.Thread(target=Proposer.randDelayMsg, args=(self, phase, self.config[s],))
				threads.append(t)
			else:
				print("NW partition - cannot speak to ", s)
		for t in threads:
			t.start()
		for t in threads:
			t.join()

	# Add random delay to sending of messages
	def randDelayMsg(self, phase, proc):
		#print("in rand delay for phase ", phase, " for proc ", proc["name"])
		b = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		try:
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
		except socket.error as sock_err:
			if(sock_err.errno == socket.errno.ECONNREFUSED):
			        print("Server " + proc["name"] + " unreachable.")

	# Creates ballot for proposal
	def createBallot(self, proposedVal, proposedDepth):
		self.val = proposedVal
		self.balNum = Ballot(self.curSeqNum + 1, self.sMeta["pid"], proposedDepth)
		self.prepAckCount = 0
		self.prepAckMsgList = []
		self.acceptAckCount = 0
		print("Sending PREPARE with ballot number ", str(self.balNum))
		self.broadcast("PREPARE")
	# How Proposer deals with acks from prepare request
	def handlePrepAck(self, msg):
		print("Received PREP-ACK with accept ballot number ", str(msg["accept-num"]), " from ", msg["src-name"])
		self.prepAckMsgList.append(msg)
		if (msg["accept-num"] is None or (msg["accept-num"] is not None and msg["accept-num"].seqNum >= self.balNum.seqNum)):
			self.prepAckCount += 1
		if (self.prepAckCount == self.majority):
			maxBalNum = -1;
			changedBallot = False
			for m in self.prepAckMsgList:
				if m["accept-val"] is not None:
					maxBalNum = m["accept-num"]
					self.val = m["accept-val"]
					changedBallot = True
			if (changedBallot == True):
				print("Changed ballot to propose same value!")
			print("Sending ACCEPT with ballot number ", str(self.balNum))
			self.broadcast("ACCEPT")
	# How Proposer deals with acks from accept request
	def handleAcceptAck(self, msg):
		print("Received ACCEPT-ACK with accept ballot number ", str(msg["accept-num"]), " from ", msg["src-name"])
		if (self.balNum is None or msg["accept-num"]>=self.balNum):
			self.acceptAckCount += 1
		if (self.acceptAckCount == self.majority):
			print("Sending DECISION with ballot number ", str(self.balNum))
			self.broadcast("DECISION")
		
# Acceptor Class
class Acceptor:
	def __init__(self, sMeta):
		self.sMeta = sMeta
		self.acceptNum = None
		self.acceptVal = None
		self.minBal = None
	# how acceptor deals with prepare requests
	def recvPrepare(self, msg):
		print("Received PREPARE with ballot number ", str(msg["bal-num"]), " from ", msg["src-name"])
		prepareReqBalNum = msg["bal-num"]
		if (self.minBal is None or msg["bal-num"] > self.minBal):
			self.minBal = Ballot(msg["bal-num"].seqNum, msg["bal-num"].pid, msg["bal-num"].depth)
			proc = config[msg["src-name"]]
			b = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			try:
				b.connect((proc["ip-addr"], proc["port"]))
				msg = createPrepareAck(self)
				encMsg = pickle.dumps(msg)
				print("Sent PREP-ACK with ballot number ", str(msg["accept-num"]), " to ", proc["name"], " of balNum ", str(prepareReqBalNum), " minBal now: ", str(self.minBal))
				time.sleep(randDelay())
				b.sendall(encMsg)
				b.close()
			except socket.error as sock_err:
			    if(sock_err.errno == socket.errno.ECONNREFUSED):
			        print("Server " + msg["src-name"] + " unreachable.")
		# else:
		# 	print("Ignoring ACCEPT with ballot number ", str(msg["bal-num"]), " from ", msg["src-name"])
	# how acceptor deals with accept requests
	def recvAccept(self, msg):
		print("Received ACCEPT with ballot number ", str(msg["bal-num"]), " from ", msg["src-name"])
		acceptReqBalNum = msg["bal-num"]
		if (self.minBal is None or (msg["bal-num"] >= self.minBal)):
			self.acceptNum = Ballot(msg["bal-num"].seqNum, msg["bal-num"].pid, msg["bal-num"].depth)
			self.minBal = Ballot(msg["bal-num"].seqNum, msg["bal-num"].pid, msg["bal-num"].depth)
			self.acceptVal = msg["val"]
			proc = config[msg["src-name"]]
			b = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			try:
				b.connect((proc["ip-addr"], proc["port"]))
				msg = createAcceptAck(self)
				encMsg = pickle.dumps(msg)
				print("Sent ACCEPT-ACK with ballot number ", str(msg["accept-num"]), " to ", proc["name"], " of balNum ", str(acceptReqBalNum), " minBal now: ", str(self.minBal))
				time.sleep(randDelay())
				b.sendall(encMsg)
				b.close()
			except socket.error as sock_err:
			    if(sock_err.errno == socket.errno.ECONNREFUSED):
			        print("Server " + msg["src-name"] + " unreachable.")
		# else:
		# 	print("Ignoring ACCEPT with ballot number ", str(msg["bal-num"]), " from ", msg["src-name"])
	# how acceptor deals with decision requests
	def recvDecision(self, msg):
		print("Received DECISION with ballot number ", str(msg["bal-num"]), " from ", msg["src-name"])
		print("Resetting acceptor values.")
		self.minBal = None
		self.acceptNum = None
		self.acceptVal = None










