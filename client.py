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

clients = {'A', 'B', 'C', 'D', 'E'}

class Client:
	def __init__(self, config):
		self.config = config;
	def run(self):
		while True:
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			server_ip = self.config["ip-addr"]
			server_port = self.config["port"]
			s.connect((server_ip, server_port))
			command = input("Enter a command: ")
			if (Client.validateCommand(self,command)):
				print("Started new thread")
				t1 = threading.Thread(target=Client.sendMsg, args=(self,command,s))
				t1.start()
				#Client.sendMsg(self, command, s)
			else:
				print('Invalid command. Commands available: "moneyTransfer(<amt>, <c1>, <c2>)", "printBlockchain", "printBalance", "printSet"')
			#s.close()
	def sendMsg(self, command, s):
		#print("calling sendmsg")
		if (command[:13] == "moneyTransfer"):
			msgSend = createUserReq(command, self.config)
		else:
			msgSend = createUserReq(command, self.config)
		#print("trying to send ", msgSend)
		encodedMsg = pickle.dumps(msgSend)
		time.sleep(randDelay())
		s.sendall(encodedMsg)
		print("Command sent to server.")
		res = None
		while res is None:
			res = s.recv(1024)
			if (res):
				msgRecvd = pickle.loads(res)
				print(msgRecvd["body"])
			# if (msgRecvd["res"] == 'SUCCESS'): #successfully added to blockchain
			# 	print("Tranfer success.")
			# 	break;
			# elif(msgRecvd["res"] == 'FAIL'): #invalid OR other block/transaction got majority
			# 	print("Transfer failed.")
			# 	break;
			# elif (msgRecvd["res"]  == 'BLOCKCHAIN'):
			# 	print(msgRecvd["body"]) # some variation of this
			# 	break;
			# elif (msgRecvd["res"] == 'BALANCE'):
			# 	print(msgRecvd["body"]) # some variation of this
			# 	break;
			# elif(msgRecvd["res"] == 'SET')
			# 	print(msgRecvd["body"]) # some variation of this
			# 	break;
		#print("out loop")

	def validateCommand(self, s):
		if (s[:13] == "moneyTransfer"):
			if (s.find("(") > -1 and s.find(")") > -1):
				vals = strSplitComma(s)
				print(vals)
				if (len(vals) == 3 and vals[0].isdigit() and vals[1] == self.config["name"] and vals[2] in clients):
					return True
				else:
					return False
		else: 
			return (s == "printBlockchain" or s == "printBalance" or s == "printSet")

if __name__ == "__main__":
	with open('config.json') as f:
		config = json.load(f)

	if (len(sys.argv) == 2):
		client_name = sys.argv[1]
		client_info = config[client_name]
		c = Client(client_info)
		c.run()
	else:
		print("Format should be 'python client.py < A | B | C | D | E >'")
