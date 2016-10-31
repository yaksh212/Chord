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
mutex = Lock()
readWriteMutex = Lock()
seenBefore = {}
port = 12420 

def checkStatus():
	try:
		time.sleep(10)
		print "Entered checkStatus"
		global myFingerTable,myPredecessor,mySuccessor,myID,myPredecessorIP,mySuccessorIP,myIP,seenBefore,port
		while True:
			print 'ok'
			mutex.acquire()
			confList = util.getConfFileList()[:]
			for i in range(0,len(confList)):
				host = confList[i][1]
				if host == myIP:
					continue
				try:
					ns = util.connectToServer(host,port,myIP)
				except:
					ns = False
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
		start_new_thread(checkStatus,())

def clientThreadStart(conn):

	try:		
		while 1:
			conn.send('Welcome to the ChordDB server 1.') #send only takes string
			dataFromClient = json.loads(conn.recv(1024))
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
				if dataFromClientMethod == 'GET':
					readWriteMutex.acquire()
					reply = util.getFromDisk(dataFromClientKey, myID)
					readWriteMutex.release()
					# reply = dataFromClient   #not required, just there to have some reply value (Remove this line after writing util.getFromDisk method)
					pass
				elif dataFromClientMethod == 'PUT':
					readWriteMutex.acquire()
					try:
						util.writeToDisk(dataFromClientKey,dataFromClientValue,myID)
					except Exception,e:
						print str(e)
					readWriteMutex.release()
					pass
				elif dataFromClientMethod == 'PUTn':
					readWriteMutex.acquire()
					util.writeToDisk(dataFromClientKey,dataFromClientValue,myID)
					reply = 'OK'
					readWriteMutex.release()

				elif dataFromClientMethod == 'LEAVE':
					readWriteMutex.acquire()
					util.leaveCluster(myID, mySuccessorIP, myPredecessorIP,myIP)
					readWriteMutex.release()
					print 'LEAVE Successful'
					conn.send(json.dumps('LEAVE Successful'))
					conn.close()
					os._exit(1)
					pass
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
			else:
				try:
					print 'Not Responsible for Key ID: ',keyID
					print 'myFingerTable:',myFingerTable
					nextClosestNodeToKeyIP = util.getClosestNodeIP(keyID,myID,mySuccessorIP,myFingerTable)
					print 'nextClosestNodeToKeyIP:', nextClosestNodeToKeyIP
					mutex.release()
					nextClosestNodeToKeyPort = 12420
					newSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
					newSocket.bind(('127.0.0.1', 0))
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
	HOST = '127.0.0.1'
	PORT = 12420
	
	try:
		global myID,mySuccessor,myPredecessor,myFingerTable,myPredecessorIP,mySuccessorIP,myIP
		mutex.acquire()
		myIP = HOST
		myID = util.generateID(HOST)
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
	
