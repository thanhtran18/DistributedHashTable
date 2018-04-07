#!usr/bin/python

import socket
import select
import random
import json
import datetime
import time
import sys
from os import environ


key_size = 16


class Node:
    def __init__(self):
        self.port = 15086
        self.host = socket.gethostname()
        self.predecessorId = 0
        self.predecessorHostname = 'silicon.cs.umanitoba.ca'
        self.predecessorPort = 15000
        self.successor = {"hostname": '', "port": -1, "ID": 0}
        self.id = random.randint(1, 2 ** key_size - 2)
        self.bootstrapPort = 15000
        self.bootstrapHost = 'silicon.cs.umanitoba.ca'
        self.bootstrapId = 2 ** key_size - 1
        self.bootstrapAddr = (self.bootstrapHost, self.bootstrapPort)
        self.lastKnownResponse = {}
        self.inRing = False
        self.query = -1
        self.hops = 0

    def setSuccessor(self, newSucc):
        self.successor = newSucc

    def setId(self, newId):
        self.id = newId

    def setInRing(self, newBool):
        self.inRing = newBool

    def savePredId(self, predId):
        self.predecessorId = predId

    def savePredHost(self, predHost):
        self.predecessorHostname = predHost

    def savePredPort(self, predPort):
        self.predecessorPort = predPort

    def isInRing(self):
        return self.inRing

    def setQuery(self, newQuery):
        self.query = newQuery

    def getQuery(self):
        return self.query


# This method builds up a message to be sent and returns a string (jsonObject is a dictionary)
def createMessage(jsonObject, command, port, hostname, id):
    jsonObject['cmd'] = command
    jsonObject['port'] = port
    jsonObject['hostname'] = hostname
    jsonObject['ID'] = id
    return json.dumps(jsonObject)


# This method builds up a message to be sent and returns a string (jsonObject is a dictionary)
def createMessageWithQuery(jsonObject, command, port, hostname, id, query, hops):
    jsonObject['cmd'] = command
    jsonObject['port'] = port
    jsonObject['hostname'] = hostname
    jsonObject['ID'] = id
    jsonObject['query'] = query
    jsonObject['hops'] = hops
    return json.dumps(jsonObject)


# this method sends "pred?" message to his successor and tries to receive the response in 2 seconds.returns a dictionary
def checkSuccessor(succHostname, succPort):
    succAddr = (succHostname, int(succPort))

    predId = 0
    recvData = ''
    receivedMsg = {}  # this is a json object
    mySocket.settimeout(2)
    mySocket.sendto(jsonString, succAddr)
    print(1)

    try:
        (recvData, addr) = mySocket.recvfrom(4096)
        if recvData:
            print("Received message from: {0} (in checkSuccessor): {1}".format(str(addr), recvData))
            receivedMsg = json.loads(recvData)  # receivedMsg is now a json object
            if 'me' not in receivedMsg and 'thePred' not in receivedMsg:
                if receivedMsg['port'] == currNode.port and receivedMsg['hostname'] == currNode.host:
                    newMessageStr = createMessage(jsonContent, 'setPred', currNode.port, currNode.host, currNode.id)
                    print("Oops, got a message from an old version of myself. About to rejoin!")
                    print("Sent 'setPred' message to last known node: [{0}, {1}]".format(
                        currNode.lastKnownResponse['me']['hostname'], currNode.lastKnownResponse['me']['port']))
                    mySocket.sendto(newMessageStr, (currNode.lastKnownResponse['me']['hostname'],
                                                    int(currNode.lastKnownResponse['me']['port'])))
                    print(2)
                    return {}
                replyToRequest(recvData)
                return currNode.lastKnownResponse
            predId = receivedMsg['thePred']['ID']
            currNode.lastKnownResponse = receivedMsg

    except socket.timeout as toe:
        # step 1 of pred?
        print("Didn't get the respond (check successor)...timeout: {0}".format(toe))
        print('Timestamp error: {:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()))
        print("Attempting to set the last known node as my successor to join the ring...")

        newMessageStr = createMessage(jsonContent, 'setPred', currNode.port, currNode.host, currNode.id)
        if 'me' not in currNode.lastKnownResponse and 'thePred' not in currNode.lastKnownResponse:
            print("*** ERROR: Didn't get a response from the bootstrap!!! ***\n")
        print("Sent 'setPred' message to last known node: [{0}, {1}]".format(
            currNode.lastKnownResponse['me']['hostname'], currNode.lastKnownResponse['me']['port']))
        mySocket.sendto(newMessageStr, (currNode.lastKnownResponse['me']['hostname'],
                                        int(currNode.lastKnownResponse['me']['port'])))
        print(3)
        currNode.setSuccessor(currNode.lastKnownResponse['me'])
        currNode.setInRing(True)
        receivedMsg = {}
    except ValueError as ve:
        print("Received invalid message: {0}, error: {1}".format(recvData, ve))
    return receivedMsg


# this method tries to join the current node to the ring. accepts hostname and port number as parameters
def joinTheRing(hostname, port):
    print("-------------------------------------------------------------------------------------------")
    print("My id: ", currNode.id)

    wholeResponse = checkSuccessor(hostname, port)  # send "pred?" message to the successor, with this is a json object
    if wholeResponse == {}:
        return

    if wholeResponse['thePred']['port'] == currNode.port and wholeResponse['thePred']['ID'] == currNode.id:
        print("I'm in the correct position already")
        currNode.setInRing(True)
        currNode.setSuccessor(wholeResponse['me'])
        return

    if wholeResponse['thePred']['ID'] < currNode.id or wholeResponse['thePred']['ID'] == 0:
        newMessageStr = createMessage(jsonContent, "setPred", currNode.port, currNode.host, currNode.id)

        mySocket.sendto(newMessageStr, (wholeResponse['me']['hostname'], wholeResponse['me']['port']))
        print(4)
        print("My message with 'setPred' cmd (join the ring): {0}".format(newMessageStr))
        currNode.setSuccessor(wholeResponse['me'])
        # logging
        print("My current predecessor: [{0}, {1}, {2}]".format(currNode.successor['hostname'],
                                                               currNode.successor['port'], currNode.successor['ID']))
        print()
        print("==> Joined the ring!")
        currNode.setInRing(True)
    # recurse on the predecessor of the successor
    else:
        joinTheRing(wholeResponse['thePred']['hostname'], int(wholeResponse['thePred']['port']))


# this method tries to stabilize the current node with the rest of ring
def stabilize():
    print("-------------------------------------------------------------------------------------------")
    newMessage = createMessage(jsonContent, "pred?", currNode.port, currNode.host, currNode.id)
    succAddr = (currNode.successor['hostname'], int(currNode.successor['port']))
    mySocket.settimeout(2)

    try:
        mySocket.sendto(newMessage, succAddr)
        print(5)
        print("Sent 'pred?' to my successor at: {0}".format(str(succAddr)))
        (recvData, addr) = mySocket.recvfrom(4096)
        print("Received successor's message (stabilize): {0}".format(recvData))
        receivedMsg = json.loads(recvData)  # receivedMsg is now a json object

        if 'me' not in receivedMsg and 'thePred' not in receivedMsg:
            updatePredMsg = replyToRequest(recvData)
            mySocket.sendto(updatePredMsg, addr)
            print(6)
            return
        elif 'cmd' in receivedMsg and 'thePred' in receivedMsg and receivedMsg['cmd'] == 'pred?':
            return
        predId = receivedMsg['thePred']['ID']

        if predId > currNode.id:  # if successor's predecessor is greater (after) you...
            print("Trying to stabilize - Sending 'setPred' to 'thePred' and set my successor to be 'thePred'...")
            updatePredMsg = createMessage(jsonContent, "setPred", currNode.port, currNode.host, currNode.id)
            newSuccAddr = (receivedMsg['thePred']['hostname'], int(receivedMsg['thePred']['port']))
            mySocket.sendto(updatePredMsg, newSuccAddr)
            print(7)
            currNode.setSuccessor(receivedMsg['thePred'])

        elif predId < currNode.id:  # if successor's predecessor is smaller (before) you...
            print("Trying to stabilize - Sending 'setPred' to 'me'...")
            updatePredMsg = createMessage(jsonContent, "setPred", currNode.port, currNode.host, currNode.id)
            newSuccAddr = (receivedMsg['me']['hostname'], receivedMsg['me']['port'])
            mySocket.sendto(updatePredMsg, newSuccAddr)
            print(8)

        else:
            print("\nThis is me in stabilization (in the correct position)...\n")
    # rejoin the ring if there is no response
    except socket.timeout as toe:
        currNode.setInRing(False)
        print("Didn't get the respond (stabilize)...timeout: {0}".format(toe))
        print('Timestamp error: {:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()))
        print("Attempting to rejoin the ring...")
        joinTheRing(currNode.bootstrapHost, currNode.bootstrapPort)
    except ValueError as ve:
        print("Received invalid message: {0}, error: {1}".format(recvData, ve))


# this method handles the message received from other sockets, with "pred?" and "setPred" command, returns a string
# input message doesn't contain 'me' or 'thePred' as keys
def replyToRequest(inputMessage):  # inputMessage is a string
    responseObject = {"me": {"hostname": currNode.host, "port": currNode.port, "ID": currNode.id}, "cmd": "myPred",
                      "thePred": {"hostname": currNode.bootstrapHost, "port": currNode.bootstrapPort, "ID": 0}}
    responseMsg = ''
    try:
        inputObject = json.loads(inputMessage)
        requestCmd = inputObject['cmd']
        if requestCmd == 'pred?':
            responseObject['thePred']['hostname'] = currNode.predecessorHostname
            responseObject['thePred']['port'] = currNode.predecessorPort
            responseObject['thePred']['ID'] = currNode.predecessorId
            responseMsg = json.dumps(responseObject)
            print("My response: {0}".format(responseMsg))

        elif requestCmd == 'setPred':
            # save 'me' (the sender) as my pred now
            print("My predecessor has changed - Old predecessor: [{0}, {1}, {2}]".format(
                currNode.predecessorHostname, currNode.predecessorPort, currNode.predecessorId))
            currNode.savePredHost(inputObject['hostname'])
            currNode.savePredId(inputObject['ID'])
            currNode.savePredPort(inputObject['port'])
            print("New predecessor: [{0}, {1}, {2}]".format(currNode.predecessorHostname, currNode.predecessorPort,
                                                            currNode.id))
    except ValueError as ve:
        print("Received invalid message: {0}, error: {1}".format(inputMessage, ve))
    return responseMsg


# MAIN
currNode = Node()
address = (currNode.host, currNode.port)

jsonFile = '{"cmd": "", "port": 0, "ID": 0, "hostname": "", "query": "", "hops": 0}'
jsonContent = json.loads(jsonFile)  # jsonContent is now a dictionary

jsonContent["cmd"] = "pred?"
jsonContent["port"] = currNode.port
jsonContent["hostname"] = currNode.host
jsonContent["ID"] = currNode.id

jsonString = json.dumps(jsonContent)
mySocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
mySocket.bind(address)

nexttime = time.time()
timer = 0

# handle different inputs

keepRunning = True
while keepRunning:
    socketFD = mySocket.fileno()  # file descriptor (an int)
    (readFD, writeFD, errorFD) = select.select([socketFD, sys.stdin], [], [], 0.0)

    if not currNode.isInRing():  # join the ring if the node is not in the ring yet
        print("\nI'm not in the ring yet...")
        joinTheRing(currNode.bootstrapHost, int(currNode.bootstrapPort))

    for desc in readFD:  # choose from different types of input
        # if input is standard input
        if desc == sys.stdin:
            stabilize()
            print("Getting stdin...")
            userInput = sys.stdin.readline()

            if userInput == '\n':  # stop when enter key was pressed
                print("*** Terminated ***")
                mySocket.close()
                keepRunning = False
                break

            else:  # handling the query key
                try:
                    queryKey = int(userInput)
                    if queryKey > currNode.bootstrapId:
                        queryKey = random.randint(currNode.id + 1, 2 ** key_size - 1)
                except ValueError:  # generates a random number if user entered something else other than an int
                    print("Wrong input format. It's supposed to be an int")
                    queryKey = random.randint(currNode.id + 1, 2 ** key_size - 1)
                    print("Generated this new query: {0}".format(queryKey))
                finally:
                    while queryKey <= currNode.id:
                        queryKey = random.randint(currNode.id + 1, 2 ** key_size - 1)
                        print("The query is too small. Generated this new query: {0}".format(queryKey))
                    jsonFile = '{"cmd": "", "port": 0, "ID": 0, "hostname": "", "query": "", "hops": 0}'
                    jsonContent = json.loads(jsonFile)
                    findMessage = createMessageWithQuery(jsonContent, "find", currNode.port, currNode.host, currNode.id,
                                                         int(queryKey), currNode.hops)
                    mySocket.sendto(findMessage, (currNode.successor['hostname'], int(currNode.successor['port'])))
                    print('Sent "find" message to: [{0}, {1}]'.format(currNode.successor["hostname"],
                                                                      currNode.successor["port"]))
                    print("Find message: {0}".format(findMessage))
                    print("")
                    print(9)
        # if input is a socket message
        elif desc == socketFD:
            print("-------------------------------------------------------------------------------------------")
            print("Getting socket message...")
            mySocket.settimeout(10)
            try:
                requestMsg, senderAddr = mySocket.recvfrom(4096)

                if requestMsg == '':
                    print("The received message is empty!")
                    continue
                print("Sender: {0}".format(str(senderAddr)))
                print("Received message: {0}".format(requestMsg))

                # handle different types of socket messages
                requestObject = json.loads(requestMsg)
                if "cmd" in requestObject and 'me' in requestObject:
                    if requestObject["cmd"] == "setPred":
                        print("'setPred' message is in the wrong format but still able to set the sender as my pred!")
                        replyToRequest(requestMsg)
                        continue
                    if requestObject['cmd'] == 'pred?':
                        # print("The received message is in the wrong format!")
                        replyToRequest(requestMsg)
                        continue
                    if 'port' in requestObject['thePred'] and int(requestObject['thePred']['port']) == currNode.port:
                        # if you meet your old version
                        if requestObject['thePred']['ID'] != currNode.id:
                            joinTheRing(requestObject['me']['hostname'], requestObject['me']['port'])
                        else:
                            break

                if "cmd" in requestObject and 'me' not in requestObject:
                    requestCmd = requestObject['cmd']

                    if requestCmd == 'pred?' or requestCmd == 'setPred':
                        myResponse = replyToRequest(requestMsg)
                        mySocket.sendto(myResponse, senderAddr)
                        print(10)

                    elif requestCmd == 'find':
                        stabilize()
                        print("Got a find message!")
                        # pass the message to my successor if the query is not my id
                        if 'query' in requestObject and 'ID' in requestObject and 'port' in requestObject and \
                                'hops' in requestObject and 'hostname' in requestObject \
                                and requestObject['query'] != currNode.id:
                            myResponse = createMessageWithQuery(jsonContent, requestObject['cmd'],
                                                                requestObject['port'], requestObject['hostname'],
                                                                requestObject['ID'], requestObject['query'],
                                                                requestObject['hops'] + 1)

                            mySocket.sendto(myResponse, (currNode.successor['hostname'], int(currNode.successor['port'])))
                            print("Passed it to my successor!")
                            print(11)
                        # return the owner message if it is my key
                        else:
                            myResponse = createMessageWithQuery(jsonContent, "owner", currNode.port, currNode.host,
                                                                currNode.id, requestObject['query'],
                                                                requestObject['hops'] + 1)
                            mySocket.sendto(myResponse, (requestObject['hostname'], int(requestObject['port'])))
                            print("Got a 'find' message with query is my id. Sent 'owner' message back to [{0}, {1}]!".
                                  format(requestObject['hostname'], requestObject['port']))
                            print(12)

                    elif requestCmd == 'owner':
                        print("Owner of the query: [{0}, {1}]".format(requestObject['hostname'], requestObject['port']))
                        print("Required: {0} hops to get this query: {1}".format(str(requestObject['hops']),
                                                                                 str(requestObject['query'])))

                    else:  # wrong type of message
                        print("The received message is in the wrong format (we don't support this given cmd!")

                else:  # in the case there is no "cmd": "myPred" in the response
                    continue

            except socket.timeout as toe:
                print("There is no request message! {0}".format(toe))
            except ValueError as ve:
                print("Received invalid message: {0}, error: {1}".format(requestMsg, ve))

