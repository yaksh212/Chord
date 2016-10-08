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
from collections import Counter 
from itertools import chain
from threading import Thread, Lock

def clientThreadStart(conn):

	try:
		conn.send('Welcome to the ChordDB server. \n') #send only takes string
		while 1:
			dataFromClient = json.loads(conn.recv(1024))
			print "data from client:"
			print dataFromClient
			dataFromClient['METHOD'] = 'Modified by Server'
			conn.send(json.dumps(dataFromClient))
	except:
		e = sys.exc_info()[0]
		conn.close()
		print "Closing Connection\n"

def main():
	HOST = '127.0.0.1'
	PORT = 12415
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