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


# todo: handle the input in the while loop with 'cmd' and 'me'
class Node:
    def __init__(self):
        self.port = 15086
        self.host = socket.gethostname()
        self.predecessorId = 0
        self.predecessorHostname = ''
        self.predecessorPort = -1
        self.successor = {"hostname": '', "port": -1, "ID": 0}
        self.id = random.randint(1, 2 ** key_size - 2)
        # self.id = 65533
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


# this returns a string
def createMessage(jsonObject, command, port, hostname, id):
    jsonObject['cmd'] = command
    jsonObject['port'] = port
    jsonObject['hostname'] = hostname
    jsonObject['ID'] = id
    return json.dumps(jsonObject)


def createMessageWithQuery(jsonObject, command, port, hostname, id, query, hops):
    jsonObject['cmd'] = command
    jsonObject['port'] = port
    jsonObject['hostname'] = hostname
    jsonObject['ID'] = id
    jsonObject['query'] = query
    jsonObject['hops'] = hops
    return json.dumps(jsonObject)


# this returns a json object
def checkSuccessor(succHostname, succPort):
    succAddr = (succHostname, succPort)

    predId = 0
    recvData = ''
    receivedMsg = {}  # this is a json object
    mySocket.settimeout(2)
    mySocket.sendto(jsonString, succAddr)
    print(1)

    try:
        (recvData, addr) = mySocket.recvfrom(4096)
        if recvData:
            # print("Receiced message from: ", str(addr), " (check successor): ", recvData)
            print("Received message from: {0} (in checkSuccessor): {1}".format(str(addr), recvData))
            receivedMsg = json.loads(recvData)  # receivedMsg is now a json object
            if 'me' not in receivedMsg and 'thePred' not in receivedMsg:
                if receivedMsg['port'] == currNode.port and receivedMsg['hostname'] == currNode.host:
                    newMessageStr = createMessage(jsonContent, 'setPred', currNode.port, currNode.host, currNode.id)
                    print("Oops, got a message from an old version of myself. About to rejoin!")
                    print("Sent 'setPred' message to last known node: [{0}, {1}]".format(
                        currNode.lastKnownResponse['me']['hostname'], currNode.lastKnownResponse['me']['port']))
                    mySocket.sendto(newMessageStr, (currNode.lastKnownResponse['me']['hostname'],
                                                    currNode.lastKnownResponse['me']['port']))
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
        receivedMsg = currNode.lastKnownResponse
        newMessageStr = createMessage(jsonContent, 'setPred', currNode.port, currNode.host, currNode.id)
        # print("Sent 'setPred' message to last known node: ", (currNode.lastKnownResponse['me']['hostname'],
        #                                            currNode.lastKnownResponse['me']['port']))
        print("Sent 'setPred' message to last known node: [{0}, {1}]".format(
            currNode.lastKnownResponse['me']['hostname'], currNode.lastKnownResponse['me']['port']))
        mySocket.sendto(newMessageStr, (currNode.lastKnownResponse['me']['hostname'],
                                        currNode.lastKnownResponse['me']['port']))
        print(3)
        currNode.setSuccessor(currNode.lastKnownResponse['me'])
        currNode.setInRing(True)
        receivedMsg = {}
    return receivedMsg


# hostname of port of the successor
def joinTheRing(hostname, port):
    print("-------------------------------------------------------------------------------------------")
    print("My id: ", currNode.id)

    wholeResponse = checkSuccessor(hostname, port)  # check with the bootstrap now, this is a json object
    if wholeResponse == {}:
        return
    nextTime = time.time()
    if wholeResponse['thePred']['port'] == currNode.port and wholeResponse['thePred']['ID'] == currNode.id:
        print("I'm in the correct position already")
        currNode.setInRing(True)
        currNode.setSuccessor(wholeResponse['me'])
        return

    if wholeResponse['thePred']['ID'] < currNode.id or wholeResponse['thePred']['ID'] == 0:
        # newMessage = wholeResponse['thePred']
        newMessageStr = createMessage(jsonContent, "setPred", currNode.port, currNode.host, currNode.id)
        # newMessageStr = json.dumps(newMessage)

        mySocket.sendto(newMessageStr, (wholeResponse['me']['hostname'], wholeResponse['me']['port']))
        print(4)
        print("My message with 'setPred' cmd (join the ring): {0}".format(newMessageStr))
        currNode.setSuccessor(wholeResponse['me'])
        # logging
        # print("My current successor: [", currNode.successor['hostname'], ", ", currNode.successor['port'], ", ",
        #       currNode.successor['ID'], "]")
        print("My current predecessor: [{0}, {1}, {2}]".format(currNode.successor['hostname'],
                                                               currNode.successor['port'], currNode.successor['ID']))
        print()
        print("==> Joined the ring!")
        currNode.setInRing(True)

    else:
        joinTheRing(wholeResponse['thePred']['hostname'], wholeResponse['thePred']['port'])


def stabilize():
    print("-------------------------------------------------------------------------------------------")
    newMessage = createMessage(jsonContent, "pred?", currNode.port, currNode.host, currNode.id)
    succAddr = (currNode.successor['hostname'], currNode.successor['port'])
    predId = 0
    recvData = ''
    receivedMsg = ''
    mySocket.settimeout(2)

    try:
        mySocket.sendto(newMessage, succAddr)
        print(5)
        print("Sent 'pred?' to my successor at: {0}".format(str(succAddr)))
        (recvData, addr) = mySocket.recvfrom(4096)
        # print("Sender's address: ", address)
        print("Received successor's message (stabilize): {0}".format(recvData))
        receivedMsg = json.loads(recvData)  # receivedMsg is now a json object

        if 'me' not in receivedMsg and 'thePred' not in receivedMsg:
            updatePredMsg = replyToRequest(recvData)
            mySocket.sendto(updatePredMsg, addr)
            print(6)
            return

        predId = receivedMsg['thePred']['ID']

        if predId > currNode.id:  # if successor's predecessor is greater (after) you...
            print("Trying to stabilize - Sending 'setPred' to 'thePred' and set my successor to be 'thePred'...")
            updatePredMsg = createMessage(jsonContent, "setPred", currNode.port, currNode.host, currNode.id)
            newSuccAddr = (receivedMsg['thePred']['hostname'], receivedMsg['thePred']['port'])
            mySocket.sendto(updatePredMsg, newSuccAddr)
            print(7)
            currNode.setSuccessor(receivedMsg['thePred'])

        elif predId < currNode.id:  # if successor's predecessor is greater (after) you...
            print("Trying to stabilize - Sending 'setPred' to 'me'...")
            updatePredMsg = createMessage(jsonContent, "setPred", currNode.port, currNode.host, currNode.id)
            newSuccAddr = (receivedMsg['me']['hostname'], receivedMsg['me']['port'])
            mySocket.sendto(updatePredMsg, newSuccAddr)
            print(8)

        else:
            print("\nThis is me in stabilization (in the correct position)...\n")

    except socket.timeout as toe:
        currNode.setInRing(False)
        print("Didn't get the respond (stabilize)...timeout: {0}".format(toe))
        print('Timestamp error: {:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()))
        print("Attempting to rejoin the ring...")
        joinTheRing(currNode.bootstrapHost, currNode.bootstrapPort)


# this returns a string
# input doesn't contain 'me' or 'thePred'
def replyToRequest(inputMessage):  # inputMessage is a string
    responseObject = {"me": {"hostname": currNode.host, "port": currNode.port, "ID": currNode.id}, "cmd": "myPred",
                      "thePred": {"hostname": currNode.bootstrapHost, "port": currNode.bootstrapPort, "ID": 0}}
    responseMsg = ''
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
        # print("My predecessor has changed - Old predecessor: [", currNode.predecessorHostname, ", ",
        #       currNode.predecessorPort, ", ", currNode.id, "]")
        print("My predecessor has changed - Old predecessor: [{0}, {1}, {2}]".format(
            currNode.predecessorHostname, currNode.predecessorPort, currNode.id))
        currNode.savePredHost(inputObject['hostname'])
        currNode.savePredId(inputObject['ID'])
        currNode.savePredPort(inputObject['port'])
        # print("New predecessor: [", currNode.predecessorHostname, ", ", currNode.predecessorPort, ", ",currNode.id, "]")
        print("New predecessor: [{0}, {1}, {2}]".format(currNode.predecessorHostname, currNode.predecessorPort,
                                                        currNode.id))

    return responseMsg


# MAIN
currNode = Node()
address = (currNode.host, currNode.port)

# jsonFile = open("object.json", "r+")
jsonFile = '{"cmd": "", "port": 0, "ID": 0, "hostname": "", "query": "", "hops": 0}'
jsonContent = json.loads(jsonFile)  # jsonContent is now a json object

jsonContent["cmd"] = "pred?"
jsonContent["port"] = currNode.port
jsonContent["hostname"] = currNode.host
jsonContent["ID"] = currNode.id

jsonString = json.dumps(jsonContent)
mySocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
mySocket.bind(address)

nexttime = time.time()
timer = 0

# handle input
# todo: should be able to stop immediately after pressing enter

keepRunning = True
while keepRunning:
    socketFD = mySocket.fileno()  # file descriptor (an int)
    (readFD, writeFD, errorFD) = select.select([socketFD, sys.stdin], [], [], 0.0)

    if not currNode.isInRing():
        print("\nI'm not in the ring yet...")
        joinTheRing(currNode.bootstrapHost, currNode.bootstrapPort)
    else:
        time.sleep(0.5)
        timer += 0.5
        if 15 <= timer <= 16:
            stabilize()
            timer = 0

    for desc in readFD:
        if desc == sys.stdin:
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
                except ValueError:
                    print("Wrong input format. It's supposed to be an int")
                    queryKey = random.randint(currNode.id + 1, 2 ** key_size - 1)
                    print("Generated this new query: {0}".format(queryKey))
                finally:
                    while queryKey <= currNode.id:
                        queryKey = random.randint(currNode.id + 1, 2 ** key_size - 1)
                    jsonFile = '{"cmd": "", "port": 0, "ID": 0, "hostname": "", "query": "", "hops": 0}'
                    jsonContent = json.loads(jsonFile)
                    findMessage = createMessageWithQuery(jsonContent, "find", currNode.port, currNode.host, currNode.id,
                                                         int(queryKey), currNode.hops)
                    # print('Sent "find" message to: [', currNode.successor["hostname"], ', ', currNode.successor["port"],
                    #       '] with query: ', int(queryKey))
                    print('Sent "find" message to: [{0}, {1}]'.format(currNode.successor["hostname"],
                                                                      currNode.successor["port"]))
                    print("")
                    mySocket.sendto(findMessage, (currNode.successor['hostname'], currNode.successor['port']))
                    print(9)

        elif desc == socketFD:
            print("-------------------------------------------------------------------------")
            print("Getting socket message...")
            mySocket.settimeout(10)
            try:
                requestMsg, senderAddr = mySocket.recvfrom(4096)

                if requestMsg == '':
                    print("The received message is empty!")
                    continue
                print("Sender: {0}".format(str(senderAddr)))
                print("Received message: {0}".format(requestMsg))

                requestObject = json.loads(requestMsg)
                if "cmd" in requestObject and 'me' in requestObject:
                    if requestObject['thePred']['port'] == currNode.port:
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
                        # pass the message to my successor if the query is not my id
                        if requestObject['query'] != currNode.id:
                            myResponse = createMessageWithQuery(requestObject, requestObject['cmd'],
                                                                requestObject['port'], requestObject['hostname'],
                                                                requestObject['ID'], requestObject['query'],
                                                                requestObject['hops'] + 1)
                            # myResponse = json.dumps(requestObject)

                            mySocket.sendto(myResponse, (currNode.successor['hostname'], currNode.successor['port']))
                            print(11)
                        else:
                            myResponse = createMessageWithQuery(jsonContent, "owner", currNode.port, currNode.host,
                                                                currNode.id, requestObject['query'],
                                                                requestObject['hops'] + 1)
                            mySocket.sendto(myResponse, (requestObject['hostname'], requestObject['port']))
                            print(12)

                    elif requestCmd == 'owner':
                        # print("Owner of the query: [", (requestObject['hostname'], requestObject['port']), "]")
                        # print("Required: ", requestObject['hops'], "hops to get this query: ", requestObject['query'])
                        print("Owner of the query: [{0}, {1}]".format(requestObject['hostname'], requestObject['port']))
                        print("Required: {0} hops to get this query: {1}".format(str(requestObject['hops']),
                                                                                 str(requestObject['query'])))

                    else:  # wrong type of message
                        print("The received message is in the wrong format (we don't support this given cmd!")

                else:  # in the case there is no "cmd": "myPred" in the response
                    continue

            except socket.timeout as toe:
                print("There is no request message! {0}".format(toe))

