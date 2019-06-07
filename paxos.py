import time
import pickle
import math
import json
import socket
import threading
from messages import createAcceptMsg, createPrepareMsg, createPrepareAck, createAcceptAck, createDecisionMsg, randDelay
import errno
from socket import error as socket_error


servers = ['A', 'B', 'C', 'D', 'E']
#servers = ['A', 'B', 'C']

lock = threading.Lock()

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
		#self.sentDecision = True
		#self.acceptAckDict = {}
		#self.prepAckDict = {}
	def broadcast(self, phase):
		threads = []
		for s in servers:
			#if (s != self.name): <-- send to own acceptor too!
			t = threading.Thread(target=Proposer.randDelayMsg, args=(self, phase, self.config[s],))
			threads.append(t)
		for t in threads:
			t.start()
		for t in threads:
			t.join()


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

	def createBallot(self, proposedVal, proposedDepth):
		self.val = proposedVal
		# if (self.balNum is None):
		# 	self.balNum = Ballot(0, self.sMeta["pid"], proposedDepth)
		# else:
		# 	newSeqNum = self.balNum.seqNum + 1
		# 	self.balNum = Ballot(newSeqNum, self.sMeta["pid"], proposedDepth)
		self.balNum = Ballot(self.curSeqNum + 1, self.sMeta["pid"], proposedDepth)
		self.prepAckCount = 0
		self.prepAckMsgList = []
		self.acceptAckCount = 0
		#self.sentDecision = False;
		print("Sending PREPARE with ballot number ", str(self.balNum))
		self.broadcast("PREPARE")
	def handlePrepAck(self, msg):
		print("Received PREP-ACK with ballot number ", str(msg["accept-num"]), " from ", msg["src-name"])
		self.prepAckMsgList.append(msg)
		# if msg["accept-num"] not in self.prepAckDict:
		# 	self.prepAckDict[msg["accept-num"]] = 1
		# else:
		# 	self.prepAckDict[msg["accept-num"]]+=1
		#if (msg["accept-num"] is not None and msg["accept-num"]<=self.balNum) or msg["accept-num"] == None:
		if (msg["accept-num"] is None or (msg["accept-num"] is not None and msg["accept-num"].seqNum >= self.balNum.seqNum)):
			self.prepAckCount += 1
		print("p-ack count now: ", self.prepAckCount)
		print("self.balNum now: ", str(self.balNum))
		if (self.prepAckCount == self.majority):
		#if (self.prepAckDict[msg["accept-num"]] == self.majority):
			maxBalNum = -1;
			#myVal = self.val
			changedBallot = False
			for m in self.prepAckMsgList:
				if m["accept-val"] is not None: #and m["accept-num"] > maxBalNum:
					maxBalNum = m["accept-num"]
					#self.balNum = Ballot(maxBalNum.seqNum, maxBalNum.pid, maxBalNum.depth)
					self.val = m["accept-val"]
					changedBallot = True
			if (changedBallot == True):
				print("Changed ballot to propose same value!")
				#print("Changing balNum to ", str(self.balNum) ," and val to \n", str(self.val))
				print("Changing val to \n", str(self.val))
			print("Sending ACCEPT with ballot number ", str(self.balNum))
			self.broadcast("ACCEPT")
	def handleAcceptAck(self, msg):
		print("Received ACCEPT-ACK with ballot number ", str(msg["accept-num"]), " from ", msg["src-name"])
		if (self.balNum is None or msg["accept-num"]>=self.balNum):
			self.acceptAckCount += 1
		print("a-ack count now: ", self.acceptAckCount)
		print("self.balNum now: ", str(self.balNum))
		if (self.acceptAckCount == self.majority):
		#if (self.acceptAckDict[msg["accept-num"]] == self.majority):
			print("Sending DECISION with ballot number ", str(self.balNum))
			self.broadcast("DECISION")
			# t1 = threading.Thread(target=Proposer.broadcast, args=(self, "DECISION",))
			# t1.start()
			# t1.join()
			#update instance vars for new thing
			print("Setting own balNum to None")
			#self.balNum = None
		

class Acceptor:
	def __init__(self, sMeta):
		self.sMeta = sMeta
		self.acceptNum = None
		self.acceptVal = None
		self.minBal = None
	def recvPrepare(self, msg):
		print("Received PREPARE with ballot number ", str(msg["bal-num"]), " from ", msg["src-name"])
		print("Comparing recvd balNum ", str(msg["bal-num"]), " with current minbal", str(self.minBal))
		prepareReqBalNum = msg["bal-num"]
		if (self.minBal is None or msg["bal-num"] > self.minBal):
			#self.minBal = msg["bal-num"]
			self.minBal = Ballot(msg["bal-num"].seqNum, msg["bal-num"].pid, msg["bal-num"].depth)
			print("recvBalNum > current minBal - updating current min bal to ", self.minBal)
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
		else:
			print("Ignoring ACCEPT with ballot number ", str(msg["bal-num"]), " from ", msg["src-name"])

	def recvAccept(self, msg):
		print("Received ACCEPT with ballot number ", str(msg["bal-num"]), " from ", msg["src-name"])
		print("Comparing recvd balNum ", str(msg["bal-num"]), " with current minbal", str(self.minBal))
		acceptReqBalNum = msg["bal-num"]
		if (self.minBal is None or (msg["bal-num"] >= self.minBal)):
			#self.acceptNum = msg["bal-num"]
			self.acceptNum = Ballot(msg["bal-num"].seqNum, msg["bal-num"].pid, msg["bal-num"].depth)
			#self.minBal = msg["bal-num"]
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
		else:
			print("Ignoring ACCEPT with ballot number ", str(msg["bal-num"]), " from ", msg["src-name"])
	def recvDecision(self, msg):
		print("Received DECISION with ballot number ", str(msg["bal-num"]), " from ", msg["src-name"])
		### reset acceptor instance vals besides min ballot
		print("Resetting acceptor values.")
		self.minBal = None
		self.acceptNum = None
		self.acceptVal = None










