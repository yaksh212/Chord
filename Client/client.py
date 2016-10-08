#!/usr/bin/python           # This is client.py file
import time
import socket               # Import socket module
import json
import argparse
import datetime
import sys

if len(sys.argv) < 2:
	print "Incorrect Usage"
	print "python client.py 1 [METHOD:GET/PUT] [KEY] [VALUE]"
	print "OR"
	print "python client.py 0 ---> for default values"
	sys.exit()

cmd = sys.argv[1] #toggle to use cmd or static key/value
method = 'GET'
key = 'dummy_key'
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
host = '127.0.0.1' 			# Get local machine name
port = 12415                # Reserve a port for your service.

data = {}
data['METHOD'] = method
data['KEY'] = key
data['VALUE'] = value

try:
	j_dump=json.dumps(data)
	print "Connecting to Server.."
	s.connect((host, port))
	print "Connection Established\n"
	reply=(s.recv(1024))
	print "Message from Server:",reply
	s.send(j_dump)
	reply=json.loads(s.recv(1024))
	print"Reply from Server:"
	print reply

except:
	e = sys.exc_info()[0]
	print e

print "\nClosing Connection"
s.close()                    # Close the socket