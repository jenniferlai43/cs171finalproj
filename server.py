#!/usr/bin/env python3
import ast
import pickle
import json
import socket
import sys
import threading
import time
from messages import createServerRes, randDelay

class Server:
	def __init__(self, config, client_sock=None):
		self.config = config
		self.init_balance = 100
		self.set = [];
		self.blockchain = [];
		#self.client_sock = client_sock #client socket for client server is connected to
	def run(self):
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.bind((self.config["ip-addr"], self.config["port"]))
		sock.listen()
		print("Server is listening...")
		while True:
			print("iter while loop")
			conn, addr = sock.accept()
			print("Start thread")
			t1 = threading.Thread(target=Server.handleReq, args=(self, conn,))
			t1.start()
		print("server socket closed.")
		sock.close()
	def handleReq(self, conn):
		print("handling")
		msg = conn.recv(1024) #will this take in one pickle or multiple
		if (msg):
			decodedMsg = pickle.loads(msg)
			print("decodedMsg: ", decodedMsg)
			res = {}
			if decodedMsg["msg"] == "TRANSFER":
				res = Server.handleTransfer(self, decodedMsg, conn)
			elif decodedMsg["msg"] == "PRINTBLOCKCHAIN":
				res = Server.printBlockchain(self, decodedMsg, conn)
			elif decodedMsg["msg"] == "PRINTBALANCE":
				res = Server.printBalance(self, decodedMsg, conn)
			elif decodedMsg["msg"] == "PRINTSET":
				res = Server.printSet(self, decodedMsg, conn)
			encRes = pickle.dumps(res)
			print("Sending ", res)
			time.sleep(randDelay())
			conn.sendall(encRes)
			#print("client socket closed")
			conn.close()
		print("outside if")
	def broadcast(self, msg):
		# for all active / connected nodes
		data = "Test Broadcase."
	def handleTransfer(self, dMsg, conn):
		data = "Transfer Success."
		msg = createServerRes(self.config, dMsg, data, "SUCCESS")
		return msg
	def printBlockchain(self, dMsg, conn):
		data = "Test Blockchain."
		msg = createServerRes(self.config, dMsg, data, "BLOCKCHAIN")
		return msg
	def printBalance(self, dMsg, conn):
		data = "Balance: Test."
		msg = createServerRes(self.config, dMsg, data, "BALANCE")
		return msg
	def printSet(self, dMsg, conn):
		data = "Test Set."
		msg = createServerRes(self.config, dMsg, data, "SET")
		return msg

if __name__ == "__main__":
	with open('config.json') as f:
		config = json.load(f)

	if (len(sys.argv) == 2):
		server_owner = sys.argv[1]

		server_info = config[server_owner]

		s = Server(server_info)
		s.run()

	else:
		print("Format should be 'python server.py < A | B | C >'")