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
from collections import Counter 
from itertools import chain
from threading import Thread, Lock

myID = 0
myFingerTable = []
mySuccessor = -1
myPredecessor = -1
mutex = Lock()
readWriteMutex = Lock()

def clientThreadStart(conn):

	try:
		conn.send('Welcome to the ChordDB server 1.') #send only takes string
		while 1:
			dataFromClient = json.loads(conn.recv(1024))
			keyID = util.generateID(dataFromClient['KEY'])
			print "keyID:",keyID
			print "data from client:"
			print dataFromClient

			dataFromClientMethod = dataFromClient['METHOD']
			dataFromClientValue = dataFromClient['VALUE']
			dataFromClientKey = dataFromClient['KEY']
			reply = dataFromClient

			mutex.acquire()
			if util.isResponsibleForKeyID(keyID,myID,myPredecessor) or True:  #Forced True just to enter if condition (remove True after writing util.isResponsibleForKeyID method)
				mutex.release()
				if dataFromClientMethod == 'GET':
					readWriteMutex.acquire()
					reply = util.getFromDisk(dataFromClientKey, myID)
					readWriteMutex.release()
					reply = dataFromClient   #not required, just there to have some reply value (Remove this line after writing util.getFromDisk method)
					pass
				elif dataFromClientMethod == 'PUT':
					readWriteMutex.acquire()
					try:
						util.writeToDisk(dataFromClientKey,dataFromClientValue)
					except Exception,e:
						print str(e)
					
					readWriteMutex.release()
					pass
				elif dataFromClientMethod == 'LEAVE':
					readWriteMutex.acquire()
					util.leaveCluster(myID, mySuccessor)
					readWriteMutex.release()
					pass
				elif dataFromClientMethod == 'REMOVE':
					util.removeFromConfFileList(dataFromClientKey)
					util.generateFingerTable(myID)
				else:
					reply['METHOD'] = 'Invalid METHOD'
			else:
				try:
					nextClosestNodeToKeyIP = util.getClosestNodeIP(keyID,myFingerTable)
					mutex.release()
					nextClosestNodeToKeyPort = 12415
					newSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
					newSocket.bind(('127.0.0.1', 0))
					newSocket.connect((nextClosestNodeToKeyIP,nextClosestNodeToKeyPort))
					welcomeMsg = newSocket.recv(1024) 	#just to maintain sequence of send/recv
					jDump = json.dumps(dataFromClient)
					newSocket.send(jDump)
					reply = json.loads(newSocket.recv(1024))
					newSocket.close()
				except:
					# This should never happen, but just being safe
					if mutex.locked():
						mutex.release()
					newSocket.close()
					print "Closing newSocket because of Exception"

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
<<<<<<< c018fb498e53429c113a078f150a518fcfde6b21
	PORT = 12420
=======
	PORT = 12417
>>>>>>> Added logic for node leaving
	
	try:
		global myID,mySuccessor,myPredecessor,myFingerTable
		mutex.acquire()
		myID = util.generateID(HOST)
		util.generateConfFileList()
		myFingerTable = util.generateFingerTable(myID)
		mySuccessor = util.getSuccessor(myFingerTable)
		myPredecessor = util.getPredecessor(myID)
		mutex.release()
		print "Server ID:",myID
	except:
		print "Initialization Failed, Node Shutting down.."
		if mutex.locked():
			mutex.release()
		sys.exit()

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
	
