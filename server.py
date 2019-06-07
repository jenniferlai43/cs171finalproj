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
from network import PARTITION

lock = threading.Lock()

servers = ['A', 'B', 'C', 'D', 'E']

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

	def run(self):
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		sock.bind((self.config["ip-addr"], self.config["port"]))
		sock.listen()
		try:
			with open(self.config["name"] + "_blockchain.txt", "rb") as f:
				loadedBlockchain = pickle.load(f)
				self.blockchain = loadedBlockchain
				f.close()
		except:
	   		print("File doesn't exist.")

		self.askResync()
		print("Server is listening...")
		while True:
			conn, addr = sock.accept()
			t1 = threading.Thread(target=Server.handleReq, args=(self, conn,))
			t1.start()
	def handleReq(self, conn):
		while True:
			msg = conn.recv(1024)
			msg_set = set()
			msg_set.add(msg)
			for m in msg_set:
				if m:
					try:
						decodedMsg = pickle.loads(msg)
						t1 = threading.Thread(target=Server.handleClientMsg, args=(self, decodedMsg, conn,))
						t1.start()
					except EOFError:
   						print("pickle EOF")
				
			
	def handleClientMsg(self, decodedMsg, conn):
		#print("thread msg recvd", decodedMsg)
		if decodedMsg["msg"] == "TRANSFER":
			#add to set
			self.client_sock = conn
			self.set.append(decodedMsg)
			if len(self.set) >= 2:
				self.handlePaxos(decodedMsg)
		elif decodedMsg["msg"] == "PRINTBLOCKCHAIN":
			self.printBlockchain(decodedMsg, conn)
		elif decodedMsg["msg"] == "PRINTBALANCE":
			self.printBalance(decodedMsg, conn)
		elif decodedMsg["msg"] == "PRINTSET":
			self.printSet(decodedMsg, conn)
		elif decodedMsg["msg"] == "CRASH":
			msg = {}
			msg["msg"] = "CRASH-ACK"
			encMsg = pickle.dumps(msg)
			conn.sendall(encMsg)
			#conn.shutdown(socket.SHUT_RDWR)
			print("Emulating server crash.")
			#conn.close()
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
			
			encMsg = pickle.dumps(createResyncAck(self, partialBlockchain))
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			try:
				s.connect((self.globalConfig[decodedMsg["src-name"]]["ip-addr"],self.globalConfig[decodedMsg["src-name"]]["port"]))
				time.sleep(randDelay())
				s.sendall(encMsg)
			except socket.error as sock_err:
				if(sock_err.errno == socket.errno.ECONNREFUSED):
				        print("Server " + proc["name"] + " unreachable.")
		elif decodedMsg["msg"] == "RESYNC-ACK":
			#print("Received blockchain from ", decodedMsg["src-name"])
			partialBlockchain = decodedMsg["blockchain"]
			lock.acquire()
			for b in partialBlockchain:
				if (b.depth==len(self.blockchain)):
					self.blockchain.append(b)
			lock.release()
			if (len(self.set) >= 2):
				x = {}
				x["msg"] = "RETRY"
				thp = threading.Thread(target=Server.handlePaxos, args=(self, x,))
				thp.start()
		else:
			self.handlePaxos(decodedMsg)
	def handlePaxos(self, decodedMsg):
		lock.acquire()
		if (decodedMsg["msg"] == "TRANSFER" and self.inPaxos == False) or (decodedMsg["msg"] == "RETRY" and self.inPaxos == False):
			if (len(self.set) >= 2):
				self.inPaxos = True
				if (self.checkProposeReady() == False):
					tempMsg = createServerRes(self.config, decodedMsg, msgFormatTrans(self.set[0]) + " and " + msgFormatTrans(self.set[1]) + " have been added to the blockchain.", "TRANSFER-ACK")
					encMsg = pickle.dumps(tempMsg)
					self.client_sock.sendall(encMsg)
					self.inPaxos = False
		lock.release()
		if decodedMsg["msg"] == "PREPARE":
			if (decodedMsg["bal-num"].depth > len(self.blockchain)):
				self.askResync()
			if (decodedMsg["bal-num"] is not None and decodedMsg["bal-num"].depth == len(self.blockchain)):
				if (decodedMsg["src-name"] != self.config["name"] and self.proposer.balNum is not None and decodedMsg["bal-num"].seqNum > self.proposer.curSeqNum):
					self.proposer.curSeqNum = decodedMsg["bal-num"].seqNum
				self.acceptor.recvPrepare(decodedMsg)
				if (self.inPaxos == True): 
					prevLen = len(self.blockchain)
					time.sleep(15)
					lock.acquire()
					if (prevLen == len(self.blockchain)):
						if (self.inPaxos == True):
							self.inPaxos = False
							print("Timed-out. Retrying. Prev len was ", prevLen, " but len blockchain now ", len(self.blockchain))
							self.proposer.curSeqNum += 1
							x = {}
							x["msg"] = "RETRY"
							thp = threading.Thread(target=Server.handlePaxos, args=(self, x,))
							thp.start()
					lock.release()
			elif decodedMsg["bal-num"] and ((len(self.blockchain) == 0 and decodedMsg["bal-num"].depth > 0) or (len(self.blockchain) < decodedMsg["bal-num"].depth)):
				self.askResync()
		elif decodedMsg["msg"] == "PREP-ACK":
			if decodedMsg["accept-num"] is None or (decodedMsg["accept-num"] is not None and decodedMsg["accept-num"].depth == len(self.blockchain)):
				self.proposer.handlePrepAck(decodedMsg)
		elif decodedMsg["msg"] == "ACCEPT":
			if (decodedMsg["bal-num"] is not None and decodedMsg["bal-num"].depth == len(self.blockchain)):
				if (decodedMsg["src-name"] != self.config["name"] and self.proposer.balNum is not None and decodedMsg["bal-num"] >= self.proposer.balNum):
					self.proposer.curSeqNum = decodedMsg["bal-num"].seqNum
				self.acceptor.recvAccept(decodedMsg)
		elif decodedMsg["msg"] == "ACCEPT-ACK":
			if decodedMsg["accept-num"] is None or (decodedMsg["accept-num"] is not None and decodedMsg["accept-num"].depth == len(self.blockchain)):
				self.proposer.handleAcceptAck(decodedMsg)
		elif decodedMsg["msg"] == "DECISION":
			if (decodedMsg["bal-num"] is not None and decodedMsg["bal-num"].depth == len(self.blockchain)):
				self.proposer.curSeqNum = 0
				self.handleDecision(decodedMsg)
				x = {}
				x["msg"] = "RETRY"
				self.handlePaxos(x)
			elif decodedMsg["bal-num"] and ((len(self.blockchain) == 0 and decodedMsg["bal-num"].depth > 0) or (len(self.blockchain) < decodedMsg["bal-num"].depth)):
				self.askResync()

	def askResync(self):
		print("Sending RESYNC request to all servers.")
		encMsg = pickle.dumps(createResyncRequest(self))
		self.broadcast(encMsg)

	def broadcast(self, m):
		threads = []
		for s in servers:
			if (s in PARTITION[self.config["name"]]):
				t = threading.Thread(target=Server.randDelayMsg, args=(self, m, self.globalConfig[s]),)
				threads.append(t)
			else:
				print("NW partition - cannot speak to ", s)
		for t in threads:
			t.start()
		for t in threads:
			t.join()

	def randDelayMsg(self, m, proc):
		b = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		try:
			b.connect((proc["ip-addr"], proc["port"]))
			time.sleep(randDelay())
			b.sendall(m)
			b.close()
		except socket.error as sock_err:
			if(sock_err.errno == socket.errno.ECONNREFUSED):
				pass


	def checkProposeReady(self):
		block = self.transactionCheck()
		if block is not None:
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
		balance = self.calcBalance()
		
		return (balance[t1["sender"]] - t1["amount"] - t2["amount"] >= 0)

	def calcPrevHash(self, b):
		s = str(b.tx1 + b.tx2 + b.nonce)
		shaHash = hashlib.sha256(s.encode())
		digest = shaHash.hexdigest()
		return digest

	def mineBlock(self, t1, t2):
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
		self.acceptor.recvDecision(dMsg)
		if (dMsg["bal-num"].depth == len(self.blockchain)):
			self.inPaxos = False
			if (len(self.set) >= 2 and dMsg["val"].tx1 == msgFormatTrans(self.set[0]) and dMsg["val"].tx2 == msgFormatTrans(self.set[1])):
				tempMsg = createServerRes(self.config, dMsg, msgFormatTrans(self.set[0]) + " and " + msgFormatTrans(self.set[1]) + " have been committed to the blockchain.", "TRANSFER-ACK")
				encMsg = pickle.dumps(tempMsg)
				self.client_sock.sendall(encMsg)
				self.set.pop(0) # pop first 2 items because committed successfully
				self.set.pop(0)
				print("Popped transactions from set.")
			self.blockchain.append(dMsg["val"])
			with open(self.config["name"] + "_blockchain.txt", "wb") as f:
				print("Saving current blockchain to disk.")  
				pickle.dump(self.blockchain, f)
				f.close()
			print("New blockchain length: ", len(self.blockchain))
			print("Commiting block to blockchain. Block: \n", dMsg["val"])
			if (len(self.set) >= 2):
				x = {}
				x["msg"] = "RETRY"
				self.handlePaxos(x)

		else:
			print("Not commiting block. Block Depth < Current Blockchain Depth")
	def printBlockchain(self, dMsg, conn):
		msg = createServerRes(self.config, dMsg, self.blockchain, "BLOCKCHAIN-ACK")
		encMsg = pickle.dumps(msg)
		conn.sendall(encMsg)
	def printBalance(self, dMsg, conn):
		balance = self.calcBalance()
		msg = createServerRes(self.config, dMsg, balance, "BALANCE-ACK")
		encMsg = pickle.dumps(msg)
		conn.sendall(encMsg)
	def printSet(self, dMsg, conn):
		#print("printing set")
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
		print("Format should be 'python server.py < A | B | C | D | E >")