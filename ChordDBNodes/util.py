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

m = 6
confFileList = []
port = 12420

def findSuccessorFromList(startID,confFileList):
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
	return int(hashlib.md5(str(value)).hexdigest(),16) % pow(2,m)

def getChordRingValue():
	return m

def getConfFileList():
	return confFileList

def removeFromConfFileList(nodeID):
	global confFileList	
	for pos in range(0,len(confFileList)):
		if confFileList[pos][0] == nodeID:
			del confFileList[pos]
			break
	return

def generateConfFileList():
	global confFileList
	with open('testNodeIP.conf') as fp:
		for line in fp:
			if line:
				tempID = line.split(' ')[0]
				tempIP = line.split(' ')[1]
				pair = (int(tempID),str(tempIP))
				confFileList.append(pair)

def generateFingerTable(nodeID):
	fingerTable = []
	for i in range(1,m+1):
		startRange = (nodeID + pow(2,i-1)) % pow(2,m)
		nodeIDIPTuple = findSuccessorFromList(startRange,confFileList)
		fingerTable.append(nodeIDIPTuple)
	return fingerTable

def getSuccessor(nodeFingerTable):
	return nodeFingerTable[0][0]

def getSuccessorIP(nodeFingerTable):
	return nodeFingerTable[0][1]

def getPredecessor(nodeID):
	if((nodeID == confFileList[0][0])):
		return confFileList[len(confFileList)-1][0]
	pos = 1
	while confFileList[pos][0] != nodeID and pos < len(confFileList):
		pos +=1
	return confFileList[pos-1][0]	

def getPredecessorIP(nodeID):
	if((nodeID == confFileList[0][0])):
		return confFileList[len(confFileList)-1][1]
	pos = 1
	while confFileList[pos][0] != nodeID and pos < len(confFileList):
		pos += 1
	return confFileList[pos-1][1]	

def isResponsibleForKeyID(keyID,nodeID,predecessorID,successorID):
	ret = False
	print keyID,nodeID,predecessorID
	if(keyID > predecessorID and keyID <= nodeID) or ((keyID > predecessorID or keyID <= nodeID) and (nodeID < predecessorID and nodeID < successorID) or len(confFileList) == 1):
		ret = True
	return ret
	
def getClosestNodeIP(keyID,nodeFingerTable):
	pass

def getFromDisk(keyID,myID):
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
def leaveCluster(myID, mySuccessor, myPredecessor):
	global port
	print 'Leaving Cluster'
	#remove yourself from successor's and predecessor's conf lists
	jsond = createJson('REMOVE', myID, None)
	if mySuccessor == myPredecessor:
		print 'Informing predecessor & successor:', myPredecessor
		sendData(jsond, mySuccessor, port)
	else:
		print 'Informing predecessor:',myPredecessor,' & successor:', mySuccessor
		sendData(jsond, mySuccessor, port)
		sendData(jsond, myPredecessor, port)
	
	transferFiles(mySuccessor,myID)
	return
	pass

#transfers all files to the next node
def transferFiles(mySuccessor,myID):
	#if folder exists, transfer all data in directory
	global port
	print 'Initiating Transfering Files'
	pathToSource = "data_" + str(myID)
	if os.path.exists(pathToSource):
		for f in os.listdir(pathToSource):
			print 'Transfering f:', f
			value = getFromDisk(str(f),myID)
			jsond = createJson('PUTn', str(f), value)
			result = sendData(jsond, mySuccessor, port)
			if not result:
				print "Could not send ", str(f)
			else:
				print "Sent ", str(f)
	return
	pass

def createJson(method, key, value):
	data = {}
	data['METHOD'] = method
	data['KEY'] = key
	data['VALUE'] = value
	jsond = json.dumps(data)
	print 'Json Created'
	return jsond

def sendData(data, host, port):
	try:
		print 'Connecting to Successor:',host,type(host)
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect((host, port))
		print 'Connected to Successor:',host
		reply=(s.recv(1024))
		print "Message from server: ", reply
		s.send(data)
		reply=json.loads(s.recv(1024))
		print"Reply from Server:"
		print reply
		s.close()
		return True

	except:
		e = sys.exc_info()[0]
		print "Closing Connection due to Exception"
		s.close()
		print e
		return False

