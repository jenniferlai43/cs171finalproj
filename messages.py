import random

def createUserReq(ui, cmeta, vals=None):
	msg = {}
	msg["src-name"] = cmeta["name"]
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

	if(ui == "crash"):
		msg["msg"] = "CRASH"
	return msg

def createServerRes(smeta, cMsg, sData, res): #sData holds what server whats to send in message (like for printing)
	msg = {}
	msg["msg"] = res
	msg["req"] = cMsg # to keep track of what transaction it was doing
	msg["src-ip"] = smeta["ip-addr"]
	msg["src-port"] = smeta["port"]
	msg["body"] = sData
	return msg

def createSyncReqMessage(smeta):
	msg["msg"] = "SYNC"
	msg["src-name"] = smeta["name"]
	return msg

def createSyncResMessage(smeta, blockchain):
	msg["msg"] = "SYNC-ACK"
	msg["src-name"] = smeta["name"]
	msg["block-chain"] = blockchain
	return msg

def randDelay():
	return random.uniform(1,4)

def strSplitComma(s):
		text = s[s.find("(")+1:s.find(")")]
		vals = [x.strip() for x in text.split(',')]
		return vals

def createPrepareMsg(proposer):
	msg = {}
	msg["msg"] = "PREPARE"
	msg["src-name"] = proposer.sMeta["name"]
	msg["bal-num"] = proposer.balNum
	return msg

def createAcceptMsg(proposer):
	msg = {}
	msg["msg"] = "ACCEPT"
	msg["src-name"] = proposer.sMeta["name"]
	msg["bal-num"] = proposer.balNum
	msg["val"] = proposer.val
	return msg

def createDecisionMsg(proposer):
	msg = {}
	msg["msg"] = "DECISION"
	msg["src-name"] = proposer.sMeta["name"]
	msg["bal-num"] = proposer.balNum
	msg["val"] = proposer.val
	return msg

def createPrepareAck(acceptor):
	msg = {}
	msg["msg"] = "PREP-ACK"
	#msg["bal-num"] = acceptor.minBal
	msg["src-name"] = acceptor.sMeta["name"]
	msg["accept-num"] = acceptor.acceptNum 
	msg["accept-val"] = acceptor.acceptVal
	return msg

def createAcceptAck(acceptor):
	msg = {}
	msg["msg"] = "ACCEPT-ACK"
	msg["src-name"] = acceptor.sMeta["name"]
	msg["accept-num"] = acceptor.acceptNum
	msg["accept-val"] = acceptor.acceptVal
	return msg

def createResyncRequest(server):
	msg = {}
	msg["msg"] = "RESYNC"
	msg["src-name"] = server.config["name"]
	if (len(server.blockchain) == 0):
		msg["cur-depth"] = None
	else:
		msg["cur-depth"] = len(server.blockchain)-1 # other server should send blocks with depth >= cur-depth + 1
	return msg

def createResyncAck(server, relevantBlockchain):
	msg = {}
	msg["msg"] = "RESYNC-ACK"
	msg["src-name"] = server.config["name"]
	msg["blockchain"] = relevantBlockchain
	return msg
