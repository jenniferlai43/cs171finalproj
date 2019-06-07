#!/usr/bin/env python3

import ast
import json
import pickle
import socket
import sys
import threading
from random import random
import time
from messages import createUserReq, randDelay, strSplitComma
import errno
from socket import error as socket_error

clients = {'A', 'B', 'C', 'D', 'E'}

class Client:
	def __init__(self, config):
		self.config = config;
	### Run Client
	def run(self):
		while True:
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			server_ip = self.config["ip-addr"]
			server_port = self.config["port"]	
			command = input("Enter a command: ")
			try:
				s.connect((server_ip, server_port))
				if (Client.validateCommand(self,command)):
					#if (self.checkTransfer(command) == True):
					t1 = threading.Thread(target=Client.sendMsg, args=(self,command,s))
					if (command == "crash" or command == "printSet" or command == "printBlockchain" or command == "printBalance"):
						t1.start()
						t1.join()
					else:
						t1.start()
				else:
					print('Invalid command. Commands available: "moneyTransfer(<amt>, <c1>, <c2>)", "printBlockchain", "printBalance", "printSet", "crash"')
			except socket.error as sock_err:
			    if(sock_err.errno == socket.errno.ECONNREFUSED):
			        print("Server " + self.config["name"] + " unreachable. Please wait until server is back up.")
	### Send message to server
	def sendMsg(self, command, s):
		#print("calling sendmsg")
		if (command[:13] == "moneyTransfer"):
			msgSend = createUserReq(command, self.config)
		elif(command[:7] == "network"):
			nodes = command.split(' ')
			nodes.pop(0)
			msgSend = createUserReq(command, self.config, nodes)
		else:
			msgSend = createUserReq(command, self.config)
		#print("trying to send ", msgSend)
		encodedMsg = pickle.dumps(msgSend)
		if (command != "crash" and command != "printSet" and command != "printBlockchain" and command != "printBalance"):
			time.sleep(randDelay())
		s.sendall(encodedMsg)
		#print("Command sent to server.")
		if msgSend["msg"] == "TRANSFER":
			try:
				while True:
					res = s.recv(4096)
					if (res):
						msgRecvd = pickle.loads(res)
						if (msgRecvd["msg"] == 'TRANSFER-ACK'):
							print(msgRecvd["body"])
			except socket.timeout:
				print("timed out")
		elif msgSend["msg"] == "CRASH":
			res = s.recv(4096)
			if (res):
				msgRecvd = pickle.loads(res)
				if (msgRecvd["msg"] == 'CRASH-ACK'):
					s.shutdown(socket.SHUT_RDWR)
					s.close()
		else:
			res = s.recv(4096)
			if (res):
				msgRecvd = pickle.loads(res)
				if (msgRecvd["msg"]  == 'BLOCKCHAIN-ACK'):
					for b in msgRecvd["body"]:
						print(str(b)) 
				elif (msgRecvd["msg"] == 'BALANCE-ACK'):
					print('Balance:')
					# for key in msgRecvd["body"]:
					# 	print("{}: ${}".format(key, msgRecvd["body"][key])) 
					print("{}: ${}".format(self.config["name"], msgRecvd["body"][self.config["name"]])) 

				elif(msgRecvd["msg"] == 'SET-ACK'):
					print('Queued Transactions:')
					for t in msgRecvd["body"]:
						print(t) 

	def validateCommand(self, s):
		if (s[:13] == "moneyTransfer"):
			if (s.find("(") > -1 and s.find(")") > -1):
				vals = strSplitComma(s)
				#print(vals)
				if (len(vals) == 3 and vals[0].isdigit() and vals[1] == self.config["name"] and vals[2] in clients):
					return True
				else:
					return False
		# elif(s[:7] == "network"):
		# 	vals = s.split(' ')
		# 	vals.pop(0)
		# 	containsSelf = False
		# 	for v in vals:
		# 		if v not in clients:
		# 			return False
		# 		if v == self.config["name"]:
		# 			containsSelf = True
		# 	return containsSelf
		else: 
			return (s == "printBlockchain" or s == "printBalance" or s == "printSet" or s == "crash")

if __name__ == "__main__":
	with open('config.json') as f:
		config = json.load(f)

	if (len(sys.argv) == 2):
		client_name = sys.argv[1]
		client_info = config[client_name]
		c = Client(client_info)
		c.run()
	else:
		print("Format should be 'python client.py < A | B | C | D | E >")
