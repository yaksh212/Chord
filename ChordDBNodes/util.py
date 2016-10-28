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

m = 6
confFileList = []

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
	return int(hashlib.md5(value).hexdigest(),16) % pow(2,m)

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
				pair = (int(tempID),tempIP)
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

def getPredecessor(nodeID):
	if((nodeID == confFileList[0][0])):
		return confFileList[len(confFileList)-1][0]
	pos = 1
	while confFileList[pos][0] != nodeID and pos < len(confFileList):
		pos +=1
	return confFileList[pos-1][0]	

def isResponsibleForKeyID(keyID,nodeID,predecessorID):
	ret = False
	if(keyID > predecessorID and keyID <= nodeID):
		ret = True
	return ret
	
def getClosestNodeIP(keyID,nodeFingerTable):
	pass

def getFromDisk(keyID,myID):
	print "GTD Entered"
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
	print "Target Acquired",keyID
	print target,keyID,keyValue
	try:
		target.write(keyValue)
		target.write(os.linesep)
		target.close()
	except Exception,e:
		print str(e)
	return

