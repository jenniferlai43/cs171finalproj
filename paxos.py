class Ballot:
	def __init__(self, seqNum, procId, depth):
		self.seqNum = 

class Proposer:
	def __init__(self, pid, proposedVal, depth = 0, seqNum = 0):
		self.ballot = Ballot(seqNum, pid, depth)
		self.val = proposedVal
	

