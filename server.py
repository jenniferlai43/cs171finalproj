#!/usr/bin/env python3
import ast
import pickle
import hashlib
import json
import socket
import sys
import threading
import time
from messages import createServerRes, randDelay, createResyncRequest, createResyncAck
from paxos import Ballot, Proposer, Acceptor
from blockchain import Block
import os

lock = threading.Lock()

def msgFormatTrans(m1):
	s = "{} {} {}".format(m1["sender"], m1["receiver"], m1["amount"])
	return s
def transFormatDict(s):
	l = s.split(" ")
	d = {}
	d["sender"] = l[0]
	d["receiver"] = l[1]
	d["amount"] = l[2]
	return d

class Server:
	def __init__(self, config, globalConfig, client_sock=None):
		self.config = config
		self.globalConfig = globalConfig
		self.init_balance = 100
		self.set = []
		self.blockchain = []
		self.proposer = Proposer(self.config, globalConfig)
		self.acceptor = Acceptor(self.config)
		self.inPaxos = False
		self.ballot = None
		#self.client_sock = client_sock #client socket for client server is connected to
	def run(self):
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.bind((self.config["ip-addr"], self.config["port"]))
		sock.listen()
		print("Server is listening...")
		while True:
			#print("iter while loop")
			conn, addr = sock.accept()
			#print("Start thread")
			t1 = threading.Thread(target=Server.handleReq, args=(self, conn,))
			t1.start()
		#print("server socket closed.")
		sock.close()
	# do server or proposer thread based on message received
	def handleReq(self, conn):
		while True:
			msg = conn.recv(1024)
			#if (msg):
			msg_set = set()
			msg_set.add(msg)
			for m in msg_set:
				if m:
					decodedMsg = pickle.loads(msg)
					#print("handling msg: ", decodedMsg)
					t1 = threading.Thread(target=Server.handleClientMsg, args=(self, decodedMsg, conn,))
					t1.start()
				
			
	def handleClientMsg(self, decodedMsg, conn):
		#print("thread msg recvd", decodedMsg)
		if decodedMsg["msg"] == "TRANSFER":
			#add to set
			self.set.append(decodedMsg)
			if len(self.set) >= 2:
				self.handlePaxos(decodedMsg, conn)
		elif decodedMsg["msg"] == "PRINTBLOCKCHAIN":
			self.printBlockchain(decodedMsg, conn)
		elif decodedMsg["msg"] == "PRINTBALANCE":
			self.printBalance(decodedMsg, conn)
		elif decodedMsg["msg"] == "PRINTSET":
			self.printSet(decodedMsg, conn)
		elif decodedMsg["msg"] == "CRASH":
			print("Emulating server crash.")
			os._exit(1)
		elif decodedMsg["msg"] == "RESYNC":
			print("Received RESYNC request from ", decodedMsg["src-name"])
			startIndex = decodedMsg["cur-depth"]
			partialBlockchain = []
			if (startIndex is None):
				startIndex = 0
			else:
				startIndex = startIndex + 1
			for x in range(startIndex, len(self.blockchain)):
				b = self.blockchain[x]
				copiedB = Block(b.tx1, b.tx2, b.prevHash, b.depth, b.nonce)
				partialBlockchain.append(copiedB)
			print("Sending partial blockchain to ", decodedMsg["src-name"])
			encMsg = pickle.dumps(createResyncAck(self, partialBlockchain))
			conn.sendall(encMsg)
			b = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			try:
				b.connect((self.globalConfig[decodedMsg["src-name"]]["ip-addr"],self.globalConfig[decodedMsg["src-name"]]["port"]))
				time.sleep(randDelay())
				b.sendall(encMsg)
				b.close()
			except socket.error as sock_err:
				if(sock_err.errno == socket.errno.ECONNREFUSED):
				        print("Server " + proc["name"] + " unreachable.")
		elif decodedMsg["msg"] == "RESYNC-ACK":
			self.blockchain.extend(partialBlockchain)
		else:
			self.handlePaxos(decodedMsg, conn)
		# 	t1 = threading.Thread(target=Server.handlePaxos, args=(self, decodedMsg, conn,))
		# 	t1.start()
	def handlePaxos(self, decodedMsg, conn):
		### when implementing with blockchain, don't need decodedMsg, pass in block instead
		lock.acquire()
		if (decodedMsg["msg"] == "TRANSFER" and self.inPaxos == False) or (decodedMsg["msg"] == "RETRY" and self.inPaxos == False):
			if (len(self.set) >= 2):
				self.inPaxos = True
				if (self.checkProposeReady() == True):
					self.inPaxos = False
		lock.release()
		if decodedMsg["msg"] == "PREPARE":
			#print("in here")
			if (decodedMsg["bal-num"] is not None and decodedMsg["bal-num"].depth == len(self.blockchain)):
				if (decodedMsg["src-name"] != self.config["name"] and self.proposer.balNum is not None and decodedMsg["bal-num"].seqNum > self.proposer.curSeqNum):
					self.proposer.curSeqNum = decodedMsg["bal-num"].seqNum
					print("Updated proposer balNum to ", str(self.proposer.balNum))
				self.acceptor.recvPrepare(decodedMsg)
				if (self.inPaxos == True): #and decodedMsg["bal-num"] > self.proposer.balNum):
					prevLen = len(self.blockchain)
					while (prevLen == len(self.blockchain)):
						time.sleep(15)
						if (self.inPaxos == True):
							self.inPaxos = False
							#self.checkProposeReady()
							x = {}
							x["msg"] = "RETRY"
							self.handlePaxos(x, conn)
		elif decodedMsg["msg"] == "PREP-ACK":
			#print("in here")
			if decodedMsg["accept-num"] is None or (decodedMsg["accept-num"] is not None and decodedMsg["accept-num"].depth == len(self.blockchain)):
				self.proposer.handlePrepAck(decodedMsg)
		elif decodedMsg["msg"] == "ACCEPT":
			if (decodedMsg["bal-num"] is not None and decodedMsg["bal-num"].depth == len(self.blockchain)):
				if (decodedMsg["src-name"] != self.config["name"] and self.proposer.balNum is not None and decodedMsg["bal-num"] >= self.proposer.balNum):
					self.proposer.curSeqNum = decodedMsg["bal-num"].seqNum
					print("Updated proposer balNum to ", str(self.proposer.balNum))
				self.acceptor.recvAccept(decodedMsg)
				# if (self.inPaxos == True): # and decodedMsg["bal-num"] > self.proposer.balNum):
				# 	prevLen = len(self.blockchain)
				# 	while (prevLen == len(self.blockchain)):
				# 		time.sleep(10)
				# 		if (self.inPaxos == True):
				# 			self.inPaxos = False
				# 			#self.checkProposeReady()
				# 			x = {}
				# 			x["msg"] = "RETRY"
				# 			self.handlePaxos(x, conn)
		elif decodedMsg["msg"] == "ACCEPT-ACK":
			if decodedMsg["accept-num"] is None or (decodedMsg["accept-num"] is not None and decodedMsg["accept-num"].depth == len(self.blockchain)):
				self.proposer.handleAcceptAck(decodedMsg)
		elif decodedMsg["msg"] == "DECISION":
			#if (decodedMsg["bal-num"] is None) or (decodedMsg["bal-num"] is not None and decodedMsg["bal-num"].depth == len(self.blockchain)):
			if (decodedMsg["bal-num"] is not None and decodedMsg["bal-num"].depth == len(self.blockchain)):
				self.proposer.curSeqNum = 0
				self.handleDecision(decodedMsg)
				print("in Paxos? ", self.inPaxos)
				x = {}
				x["msg"] = "RETRY"
				self.handlePaxos(x, conn)
			elif decodedMsg["bal-num"] and ((len(self.blockchain) == 0 and decodedMsg["bal-num"].depth > 0) or (len(self.blockchain) < decodedMsg["bal-num"].depth)):
				print("Sending RESYNC request to server ", decodedMsg["src-name"])
				encMsg = pickle.dumps(createResyncRequest(self))
				b = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				try:
					b.connect((self.globalConfig[decodedMsg["src-name"]]["ip-addr"],self.globalConfig[decodedMsg["src-name"]]["port"]))
					time.sleep(randDelay())
					b.sendall(encMsg)
					b.close()
				except socket.error as sock_err:
					if(sock_err.errno == socket.errno.ECONNREFUSED):
					        print("Server " + proc["name"] + " unreachable.")
			#self.createBallotThread(test, conn)
			#run handlePaxos here or something that creates new ballot if set >=2

			#encRes = pickle.dumps(res)
			#print("Sending ", res)
			#time.sleep(randDelay())
			#conn.sendall(encRes)
			#print("client socket closed")
			#conn.close()
		#print("outside if")
		#t1 = threading.Thread(target=Server.handleReq, args=(self, conn,))
		#t1.start()

	def checkProposeReady(self):
		block = self.transactionCheck()
		if block is not None:
			#validate top 2 transaction with previous blocks here THEN mine
			#form block here / mining with first 2 items in set THEN create ballotThread
			self.createBallotThread(block)
			return True
		return False
	def transactionCheck(self):
		val = None;
		if (len(self.set)>=2):
			t1 = msgFormatTrans(self.set[0])
			t2 = msgFormatTrans(self.set[1])
			if (self.validateTrans(self.set[0], self.set[1]) == True):
				val = self.mineBlock(t1, t2)
			else:
				print("Previous 2 transactions not valid.")
				#send both transactions to end of list
				temp = self.set.pop(0)
				self.set.append(temp)
				temp = self.set.pop(0)
				self.set.append(temp)
		return val

	def calcBalance(self):
		# return dict of 5 balances
		balance = {
			'A': self.init_balance,
			'B': self.init_balance,
			'C': self.init_balance,
			'D': self.init_balance,
			'E': self.init_balance
		}
		for b in self.blockchain:
			t1 = transFormatDict(b.tx1)
			t2 = transFormatDict(b.tx2)
			balance[t1["sender"]] = balance[t1["sender"]] - int(t1["amount"])
			balance[t2["sender"]] = balance[t2["sender"]] - int(t2["amount"])
			balance[t1["receiver"]] = balance[t1["receiver"]] + int(t1["amount"])
			balance[t2["receiver"]] = balance[t2["receiver"]] + int(t2["amount"])
		return balance

	def validateTrans(self, t1, t2):
		#validate transaction with rest of block chain here using calcBalance
		balance = self.calcBalance()
		#print(balance)
		#print(t1)
		#print(t2)
		# if (balance[t1["sender"]] - t1["amount"] < 0):
		# 	return False
		# if (balance[t2["sender"]] - t2["amount"] < 0):
		# 	return False
		### ASSUMING EACH BLOCK ONLY CONTAINS SENDING FROM ONE CLIENT
		return (balance[t1["sender"]] - t1["amount"] - t2["amount"] >= 0)

	def calcPrevHash(self, b):
		s = str(b.tx1 + b.tx2 + b.nonce)
		shaHash = hashlib.sha256(s.encode())
		digest = shaHash.hexdigest()
		return digest

	def mineBlock(self, t1, t2):
		#form block and mine here until get correct nonce.
		prevHash = None
		depth = 0
		if (len(self.blockchain) > 0):
			prevHash  = self.calcPrevHash(self.blockchain[len(self.blockchain)-1])
			depth = len(self.blockchain)
		b = Block(t1, t2, prevHash, depth)
		b.mine()
		return b

	def createBallotThread(self, block):
		self.proposer.createBallot(block, len(self.blockchain))

	def handleDecision(self, dMsg):
		# ADD TO BLOCKCHAIN
		lock.acquire()
		self.acceptor.recvDecision(dMsg)
		# if (self.proposer.val is not None):
		# 	print(str(self.proposer.val))
		if (dMsg["bal-num"].depth == len(self.blockchain)):
			
			#if (dMsg["bal-num"].pid == self.config["pid"]) or (self.inPaxos == True and self.proposer.val is not None and dMsg["val"].tx1 == self.proposer.val.tx1 and dMsg["val"].tx2 == self.proposer.val.tx2):
			self.inPaxos = False
			if (len(self.set) >= 2 and dMsg["val"].tx1 == msgFormatTrans(self.set[0]) and dMsg["val"].tx2 == msgFormatTrans(self.set[1])):
				self.set.pop(0) # pop first 2 items because committed successfully
				self.set.pop(0)
				print("Popped transactions from set.")
			self.blockchain.append(dMsg["val"])
			print("New blockchain length: ", len(self.blockchain))
			print("Commiting block to blockchain. Block: \n", dMsg["val"])
		else:
			print("Not commiting block. Block Depth < Current Blockchain Depth")
		lock.release()
		# if (self.proposer.balNum is None or dMsg["bal-num"].seqNum > self.proposer.balNum.seqNum):
		# 	self.proposer.balNum = dMsg["bal-num"]
		# 	self.proposer.balNum.pid = self.config["pid"]
		# 	print("Updated seq num to ", self.proposer.balNum.seqNum)
	def printBlockchain(self, dMsg, conn):
		#data = "Test Blockchain."
		msg = createServerRes(self.config, dMsg, self.blockchain, "BLOCKCHAIN-ACK")
		encMsg = pickle.dumps(msg)
		conn.sendall(encMsg)
	def printBalance(self, dMsg, conn):
		balance = self.calcBalance()
		msg = createServerRes(self.config, dMsg, balance, "BALANCE-ACK")
		encMsg = pickle.dumps(msg)
		conn.sendall(encMsg)
	def printSet(self, dMsg, conn):
		print("printing set")
		setList = []
		for tran in self.set:
			setList.append(msgFormatTrans(tran))
		msg = createServerRes(self.config, dMsg, setList, "SET-ACK")
		encMsg = pickle.dumps(msg)
		conn.sendall(encMsg)

if __name__ == "__main__":
	with open('config.json') as f:
		config = json.load(f)

	if (len(sys.argv) == 2):
		server_owner = sys.argv[1]

		server_info = config[server_owner]

		s = Server(server_info, config)
		s.run()

	else:
		print("Format should be 'python server.py < A | B | C >'")