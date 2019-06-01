import random

def createUserReq(ui, cmeta, vals=None):
	msg = {}
	msg["src-ip"] = cmeta["ip-addr"]
	msg["src-port"] = cmeta["port"]
	#print(ui[:13])
	if (ui[:13] == "moneyTransfer"):

		msg["msg"] = "TRANSFER"
		#print(cmeta["name"])
		vals = strSplitComma(ui)
		msg["amount"] = int(vals[0])
		msg["sender"] = vals[1]
		msg["receiver"] = vals[2]
	if (ui == "printBlockchain"):
		msg["msg"] = "PRINTBLOCKCHAIN"

	if (ui == "printBalance"):
		msg["msg"] = "PRINTBALANCE"

	if (ui == "printSet"):
		msg["msg"] = "PRINTSET"
	return msg

def createServerRes(smeta, cMsg, sData, res): #sData holds what server whats to send in message (like for printing)
	msg = {}
	msg["res"] = res
	msg["req"] = cMsg # to keep track of what transaction it was doing
	msg["src-ip"] = smeta["ip-addr"]
	msg["src-port"] = smeta["port"]
	msg["body"] = sData
	return msg

def randDelay():
	return random.uniform(1,4)

def strSplitComma(s):
		text = s[s.find("(")+1:s.find(")")]
		vals = [x.strip() for x in text.split(',')]
		return vals