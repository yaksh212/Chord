import socket
import sys
from thread import *
import datetime
import json
import errno
import hashlib
import urllib
import urllib2
import time
import select
import os
import glob

m = 6				#chord ring parameter
confFileList = []	#list containing info about all nodes
port = 12420

def findSuccessorFromList(startID,confFileList):
	"""
	Helper function used to build the fingerTable. Finds
	the successor to the startID from the confFileList

	Args:
		startID (int): ID on the chord ring
		confFileList: list containing ID and IP of all nodes
	Returns:
		tuple of ID (int) and IP (str) for the successor of the startID
	"""
	lower = 0
	upper = len(confFileList)
	if((startID <= confFileList[lower][0]) or (startID > confFileList[upper-1][0])):
		return confFileList[lower]
	while lower < upper:
		pos = lower + ((upper - lower) / 2)
		if pos != 0 and startID <= confFileList[pos][0] and startID > confFileList[pos-1][0]:
			return confFileList[pos]
		elif pos != len(confFileList) - 1 and startID > confFileList[pos][0] and startID <= confFileList[pos+1][0]:
			return confFileList[pos+1]
		elif startID > confFileList[pos][0]:
			lower = pos + 1
		elif startID < confFileList[pos][0]:
			upper = pos - 1

	return -1

def generateID(value):
	"""
	Generates md5 hash of input value

	Args:
		value: value to generate md5 Hash from
	Returns:
		md5 Hash (int) for the input value mod the number
		of points on the chord ring.
	"""	
	return int(hashlib.md5(str(value)).hexdigest(),16) % pow(2,m)

def getChordRingValue():
	return m

def getConfFileList():
	return confFileList

def removeFromConfFileList(nodeID):
	"""
	Removes the node with nodeID from the confFileList

	Args:
		nodeID: ID of the node to remove
	"""	
	global confFileList	
	for pos in range(0,len(confFileList)):
		if confFileList[pos][0] == nodeID:
			del confFileList[pos]
			break
	return

def removeFromConfFileListByIP(nodeIP):
	"""
	Removes the node with input IP from the confFileList

	Args:
		nodeIP: IP address of the node to remove
	"""	
	global confFileList	
	for pos in range(0,len(confFileList)):
		if confFileList[pos][1].strip('\n') == nodeIP:
			del confFileList[pos]
			break
	return

def generateConfFileList():
	"""
	Generates the confFileList containing information
	about all nodes in the system.
	"""	
	global confFileList
	with open('testNodeIP.conf') as fp:
		for line in fp:
			if line:
				tempID = line.split(' ')[0]
				tempIP = line.split(' ')[1]
				pair = (int(tempID),str(tempIP).strip('\n'))
				confFileList.append(pair)

def generateFingerTable(nodeID):
	"""
	Generate the fingerTable for node with ID: nodeID

	Args:
		nodeID: ID on the chord ring for the given node
	Returns:
		fingerTable: list of tuples (nodeID(int),nodeIP(str))
	"""	
	fingerTable = []
	for i in range(1,m+1):
		startRange = (nodeID + pow(2,i-1)) % pow(2,m)
		nodeIDIPTuple = findSuccessorFromList(startRange,confFileList)
		fingerTable.append(nodeIDIPTuple)
	return fingerTable

def getSuccessor(nodeFingerTable):
	#return the ID of the successor
	return nodeFingerTable[0][0]

def getSuccessorIP(nodeFingerTable):
	#return the IP address of the successor
	return nodeFingerTable[0][1]

def getPredecessor(nodeID):
	#return the ID of the predecessor
	if((nodeID == confFileList[0][0])):
		return confFileList[len(confFileList)-1][0]
	pos = 1
	while confFileList[pos][0] != nodeID and pos < len(confFileList):
		pos +=1
	return confFileList[pos-1][0]	

def getPredecessorIP(nodeID):
	#return the IP address of the predecessor
	if((nodeID == confFileList[0][0])):
		return confFileList[len(confFileList)-1][1]
	pos = 1
	while confFileList[pos][0] != nodeID and pos < len(confFileList):
		pos += 1
	return confFileList[pos-1][1]	

def isResponsibleForKeyID(keyID,nodeID,predecessorID,successorID):
	"""
	Checks to see if the current node is responsible for storing the Key

	Args:
		keyID (int): ID of key on chord ring
		nodeID (int): ID of current node on chord ring
		predecessorID (int): ID of current nodes predecessor
		successorID (int): ID of current nodes successor
	Returns:
		ret (bool): True if responsible , false otherwise
	"""	
	ret = False
	print keyID,nodeID,predecessorID
	if(keyID > predecessorID and keyID <= nodeID) or ((keyID > predecessorID or keyID <= nodeID) and (nodeID < predecessorID and nodeID < successorID) or len(confFileList) == 1):
		ret = True
	return ret
	
def getClosestNodeIP(keyID,nodeID,nodeSuccessor,nodeFingerTable):
	"""
	Returns the IP address of the node just before keyID on the chord ring

	Args:
		keyID (int): ID of key on chord ring
		nodeID (int): ID of the current node on the chord ring
		nodeSuccessor (str): IP address of the current nodes successor
		nodeFingerTable: current nodes fingerTable
	Returns:
		IP address of the node just before the keyID if found in fingerTable, 
		otherwise defaults to the successors IP address
	"""	
	for pos in range(len(nodeFingerTable) - 1, -1,-1):
		if nodeFingerTable[pos][0] <= keyID and nodeFingerTable[pos][0] > nodeID:
			return nodeFingerTable[pos][1].strip('\n')

	return nodeSuccessor
	pass

def getFromDisk(keyID,myID):
	"""
	returns the value associated with the key. Disk operation

	Args:
		keyID (int): ID of key on chord ring
		myID (int): ID of current node
	Returns:
		value associated with the key if found, error message otherwise
	Raises:
		OSError: if file path incorrect
	"""		
	print "GFD Entered"
	print myID
	try:
		dirPath = "data_" + str(myID)
		if not os.path.exists(dirPath):
			os.makedirs(dirPath)
		path=dirPath+"/"+str(keyID)

		f = open(path, 'r')
		keyValue = f.read()
		f.close()
	except Exception,e: 
		print str(e)
		print "Failed"
		return "No Such Key"

	return keyValue

def writeToDisk(keyID,keyValue,myID):
	"""
	writes the value associated with the key. Disk operation

	Args:
		keyID (int): ID of key on chord ring
		keyValue (str): value associated with the key
		myID (int): ID of current node
	Returns:
		failure message if write not successful, nothing otherwise
	Raises:
		OSError: if file path incorrect
	"""		
	print "WTD entered"
	print myID
	print keyID,keyValue
	try:
		dirPath = "data_" + str(myID)
		if not os.path.exists(dirPath):
			os.makedirs(dirPath)
		path=dirPath+"/"+str(keyID)
		target = open(path, 'w+')
	except Exception,e: 
		print str(e)
		print "Failed"
		return
	print "Target Acquired",keyID
	print target,keyID,keyValue
	try:
		target.write(keyValue)
		target.write(os.linesep)
		target.close()
	except Exception,e:
		print str(e)
	return

#leaves cluster
def leaveCluster(myID, mySuccessor, myPredecessor,nodeIP):
	"""
	Initiates the LEAVE operation

	Args:
		myID (int): ID of current node
		mySuccessor (str): IP address of current nodes successor
		myPredecessor (str): IP address of current nodes predecessor
		nodeIP (str): IP address of the current node
	"""		
	global port
	print 'Leaving Cluster'
	#remove yourself from successor's and predecessor's conf lists
	jsond = createJson('REMOVE', myID, None)
	if mySuccessor == myPredecessor:
		print 'Informing predecessor & successor:', myPredecessor
		s0 = connectToServer(mySuccessor,port,nodeIP)
		if s0 != False:
			sendData(jsond, mySuccessor, port,s0)
			closeConnection(s0)
	else:
		print 'Informing predecessor:',myPredecessor,' & successor:', mySuccessor

		s1 = connectToServer(mySuccessor,port,nodeIP)
		if s1 != False:
			sendData(jsond, mySuccessor, port,s1)
			closeConnection(s1)

		s2 = connectToServer(myPredecessor,port,nodeIP)
		if s2 != False:
			sendData(jsond, myPredecessor, port,s2)
			closeConnection(s2)

	s3 = connectToServer(mySuccessor,port,nodeIP)
	if s3 != False:
		transferFiles(mySuccessor,myID,s3)
		closeConnection(s3)

	return
	pass

def transferFiles(mySuccessor,myID,s):
	"""
	transfers all files to the next node

	Args:
		mySuccessor (str): IP address of current nodes successor
		myID (int): current nodes ID
		s: socket connection
	"""
	#if folder exists, transfer all data in directory
	global port
	print 'Initiating Transfering Files'
	pathToSource = "data_" + str(myID)
	if os.path.exists(pathToSource):
		for f in os.listdir(pathToSource):
			print 'Transfering f:', f
			value = getFromDisk(str(f),myID)
			jsond = createJson('PUTn', str(f), value)
			result = sendData(jsond, mySuccessor, port,s)
			if not result:
				print "Could not send ", str(f)
			else:
				print "Sent ", str(f)
	return
	pass

def createJson(method, key, value):
	"""
	create a JSON object with the given method,key,value

	Args:
		method: method field value
		key: key field value
		value: value field value (whatte tongue twister)
	Returns:
		the created JSON Object
	"""	
	data = {}
	data['METHOD'] = method
	data['KEY'] = key
	data['VALUE'] = value
	jsond = json.dumps(data)
	print 'Json Created'
	return jsond

def connectToServer(host,port,nodeIP):
	"""
	establish a connection with host

	Args:
		host (str): hosts IP address
		port (int): hosts port number
		nodeIP (int): current nodes ID
	Returns:
		The socket connection param if connection 
		successful, false otherwise
	Raises:
		SocketError: if connection fails
	"""	
	try:
		print 'Connecting to Server:',host,type(host)
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.bind((nodeIP, 0))
		s.connect((host, port))
		print 'Connected to Server:',host	
		return s
	except:
		e = sys.exc_info()[0]
		print "Closing Connection due to Exception"
		s.close()
		print e
		return False

def closeConnection(s):
	s.close()
	return

def sendData(data, host, port,s):
	"""
	Send data over connection s

	Args:
		data: serialized JSON object
		host (str): IP address of the host (not used, only there to maintain api throughout:Yes I'm lazy)
		port (int): port number for the connection (not used, lazzzyyy)
		s: socket connection 
	Returns:
		true is send successful,false otherwise
	"""	
	try:
		reply=(s.recv(1024))
		print "Message from server: ", reply
		s.send(data)
		reply=json.loads(s.recv(1024))
		print"Reply from Server:"
		print reply
		return True

	except:
		e = sys.exc_info()[0]
		print "Closing Connection due to Exception"
		s.close()
		print e
		return False

