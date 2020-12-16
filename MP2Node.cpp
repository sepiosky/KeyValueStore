/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	vector<Node> curMemList;
	bool change = false;

	curMemList = getMembershipList();

	sort(curMemList.begin(), curMemList.end());

	bool ringChanged = false;
	if(ring.size() != curMemList.size()) {
		ringChanged = true;
	} else {
		for(int i=0;i<curMemList.size();i++) {
			ringChanged = ringChanged || (curMemList[i].getHashCode() != ring[i].getHashCode());
		}
	}

	if( ringChanged ) {
		this->ring = curMemList;
		this->stabilizationProtocol();
	}
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

Message MP2Node::dispatchMessage(MessageType msgType, string key, string value) {
	int created_at = this->par->getcurrtime();
	int trans_id = this->transactions.size();
	Transaction* t = new Transaction(trans_id, msgType, key, value, created_at);
	this->transactions.push_back(t);
	if(msgType == MessageType::CREATE || msgType == MessageType::UPDATE){
		Message msg = Message(trans_id, this->memberNode->addr, msgType, key, value);
		return msg;
	} else {
		Message msg = Message(trans_id, this->memberNode->addr, msgType, key);
		return msg;
	}
}


/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	vector<Node> replicas = findNodes(key);
	string msg = (dispatchMessage(MessageType::CREATE, key, value)).toString();
	for(int i=0;i<replicas.size();i++) {
		this->emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), msg);
	}
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	vector<Node> replicas = findNodes(key);
	string msg = (dispatchMessage(MessageType::READ, key)).toString();
	for(int i=0;i<replicas.size();i++) {
		this->emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), msg);
	}
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	vector<Node> replicas = findNodes(key);
	string msg = (dispatchMessage(MessageType::UPDATE, key, value)).toString();
	for(int i=0;i<replicas.size();i++) {
		this->emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), msg);
	}
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	vector<Node> replicas = findNodes(key);
	string msg = (dispatchMessage(MessageType::DELETE, key)).toString();
	for(int i=0;i<replicas.size();i++) {
		this->emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), msg);
	}
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica, int transID, MessageType msgType) {
	if( msgType == MessageType::STABILIZATION ) {
		if( this->ht->read(key) != "" ) {
			this->ht->deleteKey(key);
		}
		return this->ht->create(key, value);
	} else {
		bool result = this->ht->create(key, value);
		if( result ) {
			this->log->logCreateSuccess(&memberNode->addr, false, transID, key, value);
		} else {
			this->log->logCreateFail(&memberNode->addr, false, transID, key, value);
		}
		return result;
	}
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key, int transID) {
	string value = this->ht->read(key);
	if(value != "") {
		this->log->logReadSuccess(&memberNode->addr, false, transID, key, value);
	} else {
		this->log->logReadFail(&memberNode->addr, false, transID, key);
	}
	return value;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica, int transID) {
	bool result = this->ht->update(key,value);
	if (result) {
		this->log->logUpdateSuccess(&memberNode->addr, false, transID, key, value);
	} else {
		this->log->logUpdateFail(&memberNode->addr, false, transID, key, value);
	}
	return result;
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key, int transID) {
	bool result = this->ht->deleteKey(key);
	if ( transID == -1 ) {
		return result;
	}
	if (result) {
		this->log->logDeleteSuccess(&memberNode->addr, false, transID, key);
	} else {
		this->log->logDeleteFail(&memberNode->addr, false, transID, key);
	}
	return result;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	char * data;
	int size;

	while ( !memberNode->mp2q.empty() ) {
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);
		Message msg = Message(message);

		switch( msg.type ) {
			case MessageType::STABILIZATION:
			case MessageType::CREATE:
			case MessageType::DELETE:
			case MessageType::READ:
			case MessageType::UPDATE: {
				this->createTransaction(msg);
				break;
			}

			case MessageType::REPLY:
			case MessageType::READREPLY: {
				this->updateTransaction(msg);
				break;
			}

		}
	}


	for(int i=0;i < this->transactions.size();i++) {
		if(this->transactions[i] != NULL && this->transactions[i]->isFinished == false && this->par->getcurrtime() - this->transactions[i]->created_at > 15) {
			bool success = false;
			if(this->transactions[i]->successReply >= 2) {
				success = true;
			}
			logTransaction(this->transactions[i], success);
		}
	}
}

void MP2Node::createTransaction(Message msg) {
	Message reply = Message(msg);
	bool result;
	switch( msg.type ) {
		case MessageType::READ: {
			string value = this->readKey(msg.key, msg.transID);
			result = (value != "");
			reply = Message(msg.transID, this->memberNode->addr, value);
			break;
		}
		case MessageType::STABILIZATION:
		case MessageType::CREATE: {
			result = createKeyValue(msg.key, msg.value, msg.replica, msg.transID, msg.type);
			reply = Message(msg.transID, this->memberNode->addr, MessageType::REPLY, result);
			break;
		}
		case MessageType::DELETE: {
			result = deletekey(msg.key, msg.transID);
			reply = Message(msg.transID, this->memberNode->addr, MessageType::REPLY, result);
			break;
		}
		case MessageType::UPDATE: {
			result = updateKeyValue(msg.key, msg.value, msg.replica, msg.transID);
			reply = Message(msg.transID, this->memberNode->addr, MessageType::REPLY, result);
			break;
		}
	}
	this->emulNet->ENsend(&memberNode->addr, &msg.fromAddr, reply.toString());

}

void MP2Node::updateTransaction(Message msg) {
	if( msg.transID == -1 ) {
		return ;
	}
	Transaction* t = this->transactions[msg.transID];
	if( t == NULL || t->isFinished == true ) {
		return;
	}
	t->allReply++;
	if( msg.type == MessageType::READREPLY ) {
		t->value = msg.value;
	}
	if( (msg.type == MessageType::READREPLY && msg.value != "") || (MessageType::REPLY && msg.success) ) {
		t->successReply++;
	}

	if( t->allReply == 3 ) {
		if( t->successReply >= 2 ) {
			this->logTransaction(t, true);
		} else {
			this->logTransaction(t, false);
		}
	}
}

void MP2Node::logTransaction(Transaction* t, bool success) {
	t->isFinished = true;
	switch (t->type) {
		case CREATE: {
			if (success) {
				log->logCreateSuccess(&memberNode->addr, true, t->id, t->key, t->value);
			} else {
				log->logCreateFail(&memberNode->addr, true, t->id, t->key, t->value);
			}
			break;
		}

		case UPDATE: {
			if (success) {
				log->logUpdateSuccess(&memberNode->addr, true, t->id, t->key, t->value);
			} else {
				log->logUpdateFail(&memberNode->addr, true, t->id, t->key, t->value);
			}
			break;
		}

		case READ: {
			if (success) {
				log->logReadSuccess(&memberNode->addr, true, t->id, t->key, t->value);
			} else {
				log->logReadFail(&memberNode->addr, true, t->id, t->key);
			}
			break;
		}

		case DELETE: {
			if (success) {
				log->logDeleteSuccess(&memberNode->addr, true, t->id, t->key);
			} else {
				log->logDeleteFail(&memberNode->addr, true, t->id, t->key);
			}
			break;
		}
	}
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	map<string, string>::iterator it;
	for(it = this->ht->hashTable.begin(); it != this->ht->hashTable.end(); it++) {
		string key = it->first;
		string value = it->second;
		Message stabilizationDeleteMsg = Message(-1, this->memberNode->addr, MessageType::DELETE, key);
		for(int i=0;i<haveReplicasOf.size();i++) {
			emulNet->ENsend(&memberNode->addr, haveReplicasOf[i].getAddress(), stabilizationDeleteMsg.toString());
		}

		vector<Node> replicas = findNodes(key);
		Message stabilizationCreateMsg = Message(-1, this->memberNode->addr, MessageType::CREATE, key, value);
		for (int i = 0; i < replicas.size(); i++) {
			emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), stabilizationCreateMsg.toString());
		}
	}
}
