### How to run:
1. Edit ```config.json``` file to the ip addresses and ports you want to run on.
2. For network partitioning, ```network.py``` contains dictionary of servers along with a set of servers that can communicate with. You can edit the partitions.
	- Each list must contain AT LEAST it's own node.
	- If B exist's in A's list, then A must exist in B's list.
	- This cannot be changed in the middle of the program. Must restart servers after editing network.py to see effects.
3. Run clients with command ```python client.py < A | B | C | D | E>```
4. Run servers with command ```python server.py < A | B | C | D | E>```
5. Commands on the client include:
	- moneyTransfer(<amt>, <p1>, <p2>)
	- printSet
	- printBalance
	- printBlockchain
	- crash
6. Once blocks start to get committed, files with name like *A_blockchain.txt* appear. This is the serialized format of the server's current blockchain.