#!/usr/bin/env python3
import ast
import pickle
import json
import socket
import sys
import threading
import time
from messages import createServerRes, randDelay
from paxos import Ballot, Proposer, Acceptor
from blockchain import Block

class Server:
	def __init__(self, config, globalConfig, client_sock=None):
		self.config = config
		self.init_balance = 100
		self.set = [];
		self.blockchain = [];
		self.proposer = Proposer(self.config, globalConfig)
		self.acceptor = Acceptor()
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
		msg = conn.recv(1024)
		if (msg):
			decodedMsg = pickle.loads(msg)
			t1 = threading.Thread(target=Server.handleClientMsg, args=(self, decodedMsg, conn,))
			t1.start()
			
	def handleClientMsg(self, decodedMsg, conn):
		print(decodedMsg)
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
		else:
			self.handlePaxos(decodedMsg, conn)
		# 	t1 = threading.Thread(target=Server.handlePaxos, args=(self, decodedMsg, conn,))
		# 	t1.start()
	def handlePaxos(self, decodedMsg, conn):
		if decodedMsg["msg"] == "TRANSFER":
			#form block here
			self.createBallotThread(decodedMsg, conn,)
		elif decodedMsg["msg"] == "PREPARE":
			#print("in here")
			self.acceptor.recvPrepare(decodedMsg, conn)
		elif decodedMsg["msg"] == "PREP-ACK":
			print("in here")
			self.proposer.handlePrepAck(decodedMsg)
		elif decodedMsg["msg"] == "ACCEPT":
			self.acceptor.recvAccept(decodedMsg, conn)
		elif decodedMsg["msg"] == "ACCEPT-ACK":
			self.proposer.handleAcceptAck()
		elif decodedMsg["msg"] == "DECISION":
			self.handleDecision(decodedMsg)
			#encRes = pickle.dumps(res)
			#print("Sending ", res)
			#time.sleep(randDelay())
			#conn.sendall(encRes)
			#print("client socket closed")
			#conn.close()
		#print("outside if")
	def createBallotThread(self, decodedMsg, conn):
		self.proposer.createBallot(decodedMsg["amount"], len(self.blockchain))

	def handleDecision(self, dMsg):
		# ADD TO BLOCKCHAIN
		print("Commiting block to blockchain. Msg: ", dMsg)
	def printBlockchain(self, dMsg, conn):
		data = "Test Blockchain."
		msg = createServerRes(self.config, dMsg, data, "BLOCKCHAIN")
		encMsg = pickle.dumps(msg)
		conn.sendall(encMsg)
	def printBalance(self, dMsg, conn):
		data = "Balance: Test."
		msg = createServerRes(self.config, dMsg, data, "BALANCE")
		encMsg = pickle.dumps(msg)
		conn.sendall(encMsg)
	def printSet(self, dMsg, conn):
		data = "Test Set."
		msg = createServerRes(self.config, dMsg, data, "SET")
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