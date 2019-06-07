import hashlib
import random
import string

class Block:
	def __init__(self, t1, t2, prevHash = None, depth=0, nonce = None):
		self.depth = depth
		self.prevHash = prevHash
		self.nonce = nonce
		self.tx1 = t1
		self.tx2 = t2
	def mine(self):
		digest = None
		tempNonce = self.randNonce(10)
		while True:
			s = str(self.tx1 + self.tx2 + tempNonce)
			shaHash = hashlib.sha256(s.encode())
			digest = shaHash.hexdigest()
			if (digest[len(digest)-1] == '0' or digest[len(digest)-1] == '1'):
				self.nonce = tempNonce
				break;
			tempNonce = self.randNonce(10)
		print("Mined block with hash ", digest)
		return digest
	def randNonce(self, nonceLen):
		alphanumerics = string.digits + string.ascii_lowercase
		return ''.join(random.choice(alphanumerics) for i in range(nonceLen))
	def __eq__(self, other): 
		return (self.depth == other.depth and self.prevHash == other.prevHash and self.nonce == other.nonce)
	def __str__(self):
		return "---\ndepth:{}\nprevHash:{}\nnonce:{}\ntx1:{}\ntx2:{}\n---".format(self.depth, self.prevHash, self.nonce, self.tx1, self.tx2)