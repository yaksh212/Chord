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

m = 6

def generateID(value):
	return int(hashlib.md5(value).hexdigest(),16) % pow(2,m)

def getChordRingValue():
	return m

def generateFingerTable(nodeID):
	pass

def getSuccessor(nodeID):
	pass

def getPredecessor(nodeID):
	pass

def isResponsibleForKeyID(keyID,nodeID,predecessorID):
	ret = False
	if(keyID > predecessorID and keyID <= nodeID):
		ret = True
	return ret
	
def getClosestNodeIP(keyID,nodeFingerTable):
	pass

def getFromDisk(keyID):
	pass

def writeToDisk(keyID,keyValue):
	pass