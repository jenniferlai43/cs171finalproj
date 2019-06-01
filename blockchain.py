class Block:
	def __init__(self, depth=0):
		self.depth = depth
		self.tx1 = None
		self.tx2 = None
		self.nonce = None