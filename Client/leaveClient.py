#!/usr/bin/python           # This is client.py file
import time
import socket               # Import socket module
import json
import argparse
import random
import datetime
import sys
import hashlib

if len(sys.argv) < 2:
	print "Incorrect Usage"
	print "python client.py 1 [METHOD:GET/PUT] [KEY] [VALUE]"
	print "OR"
	print "python client.py 0 ---> for default values"
	sys.exit()

cmd = sys.argv[1] #toggle to use cmd or static key/value
method = 'LEAVE'
key = 'ya'
value = 'hello'
timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

if cmd == '1':
	method = sys.argv[2]
	key = sys.argv[3]
	value = sys.argv[4]

print "\nClient Method,Key,Value:"
print method,key,value
print

s = socket.socket()         # Create a socket object
host = '172.16.100.17' 			# Get local machine name
port = 12420                # Reserve a port for your service.

data = {}
data['METHOD'] = method
data['KEY'] = key
data['VALUE'] = value
print 'keyID:', int(hashlib.md5(key).hexdigest(),16) % pow(2,6)
try:
	j_dump=json.dumps(data)
	print "Connecting to Server.."
	s.connect((host, port))
	print "Connection Established\n"
	reply=(s.recv(1024))
	# time.sleep(5)
	print "Message from Server:"
	print reply
	s.send(j_dump)
	reply=json.loads(s.recv(1024))
	print"Reply from Server:"
	print reply

except:
	e = sys.exc_info()[0]
	print "Closing Connection due to Exception"
	s.close()
	print e
	sys.exit()

print "\nClosing Connection"
s.close()                    # Close the socket
