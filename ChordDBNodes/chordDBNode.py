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
import netifaces as ni
import util
import os
from collections import Counter 
from itertools import chain
from threading import Thread, Lock

myID = 0
myIP = None
myFingerTable = []
mySuccessor = -1
myPredecessor = -1
mySuccessorIP = None
myPredecessorIP = None
mutex = Lock() 					#mutex for global variables
readWriteMutex = Lock()			#mutex for reading and writing to disk
seenBefore = {}
port = 12420 					#all nodes run on this port

def checkStatus():
	"""
	Responsible for figuring out node Failures.
	Checks the Status of other nodes present in this nodes
	fingerTable. If a connection fails, that node is removed 
	from the fingerTable and all necessary parameters are 
	recomputed.

	Raises:
		SocketError: if connection fails for any reason
	"""
	try:
		time.sleep(20)
		print "Entered checkStatus"
		global myFingerTable,myPredecessor,mySuccessor,myID,myPredecessorIP,mySuccessorIP,myIP,seenBefore,port
		while True:
			print 'ok'
			mutex.acquire()
			confList = util.getConfFileList()[:]
			for i in range(0,len(confList)):
				host = confList[i][1]
				if host == myIP:
					#It'd be stupid to remove yourself from your FingerTable.
					continue
				try:
					ns = util.connectToServer(host,port,myIP)
				except:
					#Failed to connect to host
					ns = False

				#if ns is false, remove host from fingerTable and recompute params
				if ns == False:
					util.removeFromConfFileListByIP(host)
					myFingerTable = util.generateFingerTable(myID)
					mySuccessor = util.getSuccessor(myFingerTable)
					myPredecessor = util.getPredecessor(myID)
					mySuccessorIP = (util.getSuccessorIP(myFingerTable))
					myPredecessorIP = (util.getPredecessorIP(myID))
					print 'removed:',host,myFingerTable,util.getConfFileList()	
				else:
					print 'host alive:',host
					ns.close()

			mutex.release()
			time.sleep(3)	
	except:		
		e = sys.exc_info()[0]
		print e
		if mutex.locked():
			mutex.release()
		print "status thread dead"
		start_new_thread(checkStatus,())  		#restart statusThread, essential for figuring out node failures.

def clientThreadStart(conn):
	"""
	Start function for the ChordDBNode threads. Handles the
	GET,PUT,REMOVE and LEAVE requests. Responsible for initiating
	ReadFrom or WriteTo Disk.

	Args:
		conn: the socket connection initiated with the client
	Returns:
		reply: either Key not found, Value for key in GET, node failure
		       reason or echoed data from client on PUT
	Raises:
		SocketError: if connection fails at any time or sequence of send/recv
			     not followed
		TypeError: if data not correctly serialized or desearialized (JSON)
	"""
	try:		
		while 1:
			conn.send('Welcome to the ChordDB server 1.') #send only takes string
			dataFromClient = json.loads(conn.recv(1024)) #read what the client sent
			keyID = util.generateID(dataFromClient['KEY'])
			print "keyID:",keyID
			print "data from client:"
			print dataFromClient
			global myFingerTable,myPredecessor,mySuccessor,myID,myPredecessorIP,mySuccessorIP,myIP,seenBefore
			dataFromClientMethod = dataFromClient['METHOD']
			dataFromClientValue = dataFromClient['VALUE']
			dataFromClientKey = dataFromClient['KEY']
			reply = dataFromClient

			mutex.acquire()
			if util.isResponsibleForKeyID(keyID,myID,myPredecessor,mySuccessor) or dataFromClientMethod == 'LEAVE' or dataFromClientMethod == 'REMOVE' or dataFromClientMethod == 'PUTn':  #Forced True just to enter if condition (remove True after writing util.isResponsibleForKeyID method)
				print 'Responsible for Key ID: ',keyID
				mutex.release()
				#check if key value pair present on disk and return
				if dataFromClientMethod == 'GET':
					readWriteMutex.acquire()
					reply = util.getFromDisk(dataFromClientKey, myID)
					readWriteMutex.release()
					pass
				#write or update key value pair on disk
				elif dataFromClientMethod == 'PUT':
					readWriteMutex.acquire()
					try:
						util.writeToDisk(dataFromClientKey,dataFromClientValue,myID)
					except Exception,e:
						print str(e)
					readWriteMutex.release()
					pass
				#auxiliary method used to transfer keys during voluntary node leave
				elif dataFromClientMethod == 'PUTn':
					readWriteMutex.acquire()
					util.writeToDisk(dataFromClientKey,dataFromClientValue,myID)
					reply = 'OK'
					readWriteMutex.release()
				#initiate node leave 
				elif dataFromClientMethod == 'LEAVE':
					readWriteMutex.acquire()
					util.leaveCluster(myID, mySuccessorIP, myPredecessorIP,myIP)
					readWriteMutex.release()
					print 'LEAVE Successful'
					conn.send(json.dumps('LEAVE Successful'))
					conn.close()
					os._exit(1)
					pass
				#remove the node whose id is in dataFromClientKey
				elif dataFromClientMethod == 'REMOVE':		
					print 'Removing Node from Cluster'
					mutex.acquire()			
					util.removeFromConfFileList(dataFromClientKey)
					myFingerTable = util.generateFingerTable(myID)
					mySuccessor = util.getSuccessor(myFingerTable)
					myPredecessor = util.getPredecessor(myID)
					mySuccessorIP = (util.getSuccessorIP(myFingerTable))
					myPredecessorIP = (util.getPredecessorIP(myID))		
					mutex.release()
					print myFingerTable,mySuccessor,myPredecessor,mySuccessorIP,myPredecessorIP									
				else:
					reply['METHOD'] = 'Invalid METHOD'

			#node not responsible for the key, initiate connection with next closest node in fingerTable
			else:
				try:
					print 'Not Responsible for Key ID: ',keyID
					print 'myFingerTable:',myFingerTable
					nextClosestNodeToKeyIP = util.getClosestNodeIP(keyID,myID,mySuccessorIP,myFingerTable)
					print 'nextClosestNodeToKeyIP:', nextClosestNodeToKeyIP
					mutex.release()
					nextClosestNodeToKeyPort = 12420
					newSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
					newSocket.bind((myIP, 0))
					newSocket.connect((nextClosestNodeToKeyIP,nextClosestNodeToKeyPort))
					welcomeMsg = newSocket.recv(1024) 	#just to maintain sequence of send/recv
					jDump = json.dumps(dataFromClient)
					newSocket.send(jDump)
					reply = json.loads(newSocket.recv(1024))
					newSocket.close()
				except:
					reply = 'Node Down, Try again in a few seconds'
					print "Closing newSocket because of Exception"
					if mutex.locked():
						mutex.release()
					newSocket.close()					

			conn.send(json.dumps(reply))
	except:
		e = sys.exc_info()[0]
		if readWriteMutex.locked():
			readWriteMutex.release()
		if mutex.locked():
			mutex.release()
		conn.close()
		print "Closing Connection\n"

def main():
	ni.ifaddresses('eth0')
	HOST = str(ni.ifaddresses('eth0')[2][0]['addr'])
	PORT = 12420
	
	try:
		global myID,mySuccessor,myPredecessor,myFingerTable,myPredecessorIP,mySuccessorIP,myIP
		mutex.acquire()
		myIP = HOST
		myID = util.generateID(HOST)
		print 'myip:',HOST,myID
		util.generateConfFileList()
		myFingerTable = util.generateFingerTable(myID)
		mySuccessor = util.getSuccessor(myFingerTable)
		myPredecessor = util.getPredecessor(myID)
		mySuccessorIP = (util.getSuccessorIP(myFingerTable))
		myPredecessorIP = (util.getPredecessorIP(myID))
		mutex.release()
		print "Server ID:",myID
		print myFingerTable,mySuccessor,myPredecessor
	except:
		print sys.exc_info()[0]
		print "Initialization Failed, Node Shutting down.."
		if mutex.locked():
			mutex.release()
		sys.exit()

	start_new_thread(checkStatus,())

	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	print 'Socket created'
	 
	#Bind socket to local host and port
	try:
	    s.bind((HOST, PORT))
	except socket.error as msg:
	    print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
	    sys.exit()
	print 'Socket bind complete'
	 
	#Number of connections the OS will keep in queue while serving the current one
	s.listen(5)
	print 'Socket now listening\n'

	try:
		#Continuously accept Client Connections
		while 1:
		    conn, addr = s.accept()	#wait to accept a connection - blocking call
		    print 'Connected with ' + addr[0] + ':' + str(addr[1])
		    start_new_thread(clientThreadStart ,(conn,))	#start a new thread for each client connection

	except KeyboardInterrupt:
		print "Forced Stop"
		s.close()

if __name__ == "__main__": 
	main()
