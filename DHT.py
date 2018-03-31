#!usr/bin/python

import socket
import select
import random
import json
import datetime
import time
import sys

from os import environ

# HOST = socket.getfqdn()
# PORT = 15086
# address = (HOST, PORT)
# mySocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

key_size = 16


class Node:
    def __init__(self):
        self.port = 15086
        self.host = socket.gethostname()
        self.predecessorId = 0
        self.predecessorHost = ''
        self.predecessorPort = 0
        self.successor = {}
        self.id = random.randint(1, 2 ** key_size - 2)
        # self.id = 65533
        self.bootstrapPort = 15000
        self.bootstrapHost = 'silicon.cs.umanitoba.ca'
        self.bootstrapId = 2 ** key_size - 1
        self.bootstrapAddr = (self.bootstrapHost, self.bootstrapPort)
        self.lastKnownResponse = {}
        self.inRing = False

    def setSuccessor(self, newSucc):
        self.successor = newSucc

    def setId(self, newId):
        self.id = newId

    def setInRing(self, newBool):
        self.inRing = newBool

    def savePredId(self, predId):
        self.predecessorId = predId

    def savePredHost(self, predHost):
        self.predecessorHost = predHost

    def savePredPort(self, predPort):
        self.predecessorPort = predPort

    def isInRing(self):
        return self.inRing


# currNode = Node()
# address = (currNode.host, currNode.port)

# this returns a string
def createMessage(jsonObject, command, port, hostname, id):
    jsonObject['cmd'] = command
    jsonObject['port'] = port
    jsonObject['hostname'] = hostname
    jsonObject['ID'] = id
    return json.dumps(jsonObject)


def checkSuccessor(succHostname, succPort):
    # mySocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    succAddr = (succHostname, succPort)

    predId = 0
    recvData = ''
    receivedMsg = ''
    mySocket.settimeout(2)
    mySocket.sendto(jsonString, succAddr)

    try:
        (recvData, addr) = mySocket.recvfrom(4096)
        if recvData:
            print("Receiced message from ", addr, " (check successor): ", recvData)
            receivedMsg = json.loads(recvData)  # receivedMsg is now a json object
            if 'me' not in receivedMsg and 'thePred' not in receivedMsg:
                replyToRequest(recvData)
                return currNode.lastKnownResponse
            predId = receivedMsg['thePred']['ID']
            currNode.lastKnownResponse = receivedMsg

    except socket.timeout as toe:
        # step 1 of pred?
        print("Didn't get the respond (check successor)...timeout: ", toe)
        print('Timestamp error: {:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()))
        receivedMsg = currNode.lastKnownResponse
        # joinTheRing(succHostname, succPort)
        # print("DSFds")
        print("last known: ", currNode.lastKnownResponse)
        newMessageStr = createMessage(jsonContent, 'setPred', currNode.port, currNode.host, currNode.id)
        print("Sent this 'setPred' message to ,", (currNode.lastKnownResponse['me']['hostname'], currNode.lastKnownResponse['me']['port']), ": ", newMessageStr)
        mySocket.sendto(newMessageStr, (currNode.lastKnownResponse['me']['hostname'], currNode.lastKnownResponse['me']['port']))
        currNode.setSuccessor(currNode.lastKnownResponse['me'])
    # print("resoponse: ", recvData)
    print("pred id: ", predId)
    # print(addr)

    # clientSocket.close()
    return receivedMsg


# hostname of port of the successor
def joinTheRing(hostname, port):
    print("-------------------------------------------------------------------------------------------")
    print("My id: ", currNode.id)

    wholeResponse = checkSuccessor(hostname, port)  # check with the bootstrap now, this is a json object

    nextTime = time.time()
    if wholeResponse['thePred']['port'] == currNode.port:
        print("======== I'm in the CORRECT position ========")
        # checkSuccessor(hostname, port)
        # nextTime += 30
        # sleepTime = nextTime - time.time()
        # if sleepTime > 0:
        #     time.sleep(sleepTime)
        return

    if wholeResponse['thePred']['ID'] < currNode.id:
        # newMessage = wholeResponse['thePred']
        newMessageStr = createMessage(jsonContent, "setPred", currNode.port, currNode.host, currNode.id)
        # newMessageStr = json.dumps(newMessage)

        mySocket.sendto(newMessageStr, (wholeResponse['me']['hostname'], wholeResponse['me']['port']))
        print("My repsonse with 'setPred' (join the ring): ", newMessageStr)
        currNode.setSuccessor(wholeResponse['me'])
        print("joined the ring!")
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
    # todo: check every 30 seconds
    try:
        mySocket.sendto(newMessage, succAddr)
        print("Sent 'pred?' to successor at: ", succAddr)
        (recvData, addr) = mySocket.recvfrom(4096)
        print("Sender's address: ", address)
        print("Received successor's message (stabilize): ", recvData)
        receivedMsg = json.loads(recvData)  # receivedMsg is now a json object
        if 'me' not in receivedMsg and 'thePred' not in receivedMsg:
            replyToRequest(recvData)
            return
        predId = receivedMsg['thePred']['ID']
        if predId > currNode.id:  # if successor's predecessor is greater (after) you...
            print("Sending 'setPred' to 'thePred' and set my successor to be 'thePred'...")
            updatePredMsg = createMessage(jsonContent, "setPred", currNode.port, currNode.host, currNode.id)
            newSuccAddr = (receivedMsg['thePred']['hostname'], receivedMsg['thePred']['port'])
            mySocket.sendto(updatePredMsg, newSuccAddr)
            currNode.setSuccessor(receivedMsg['thePred'])
        elif predId < currNode.id:  # if successor's predecessor is greater (after) you...
            print("Sending 'setPred' to 'me'...")
            updatePredMsg = createMessage(jsonContent, "setPred", currNode.port, currNode.host, currNode.id)
            newSuccAddr = (receivedMsg['me']['hostname'], receivedMsg['me']['port'])
            mySocket.sendto(updatePredMsg, newSuccAddr)
        else:
            print("\nThis is me in stabilization...\n")

    except socket.timeout as toe:
        currNode.setInRing(False)
        print("Didn't get the respond (stabilize)...timeout: ", toe)
        print('Timestamp error: {:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()))
        joinTheRing(currNode.bootstrapHost, currNode.bootstrapPort)


# this returns a string
def replyToRequest(inputMessage):  # inputMessage is a string
    responseObject = {"me": {"hostname": currNode.host, "port": currNode.port, "ID": currNode.id}, "cmd": "myPred", "thePred": {"hostname": currNode.bootstrapHost, "port": currNode.bootstrapPort, "ID": 0}}
    responseMsg = ''
    inputObject = json.loads(inputMessage)
    requestCmd = inputObject['cmd']
    if requestCmd == 'pred?':
        responseObject['thePred']['hostname'] = currNode.predecessorHost
        responseObject['thePred']['port'] = currNode.predecessorPort
        responseObject['thePred']['ID'] = currNode.predecessorId
        responseMsg = json.dumps(responseObject)
        print("This is my response: ", responseMsg)

    elif requestCmd == 'setPred':
        # save 'me' (the sender) as my pred now
        currNode.savePredHost(inputObject['hostname'])
        currNode.savePredId(inputObject['ID'])
        currNode.savePredPort(inputObject['port'])
    return responseMsg


# MAIN
currNode = Node()
address = (currNode.host, currNode.port)

jsonFile = open("object.json", "r+")
jsonContent = json.load(jsonFile)  # jsonContent is now a json object
jsonFile.close()

jsonContent["cmd"] = "pred?"
jsonContent["port"] = currNode.port
jsonContent["hostname"] = currNode.host
jsonContent["ID"] = currNode.id

jsonString = json.dumps(jsonContent)
mySocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
mySocket.bind(address)

nexttime = time.time()

# handle input

print("===READY 1===")

print("===READY 2===")
keepRunning = True
while keepRunning:
    socketFD = mySocket.fileno()  # file descriptor (an int)
    (readFD, writeFD, errorFD) = select.select([socketFD, sys.stdin], [], [], 0.0)

    if not currNode.isInRing():
        print("\nI'm not in the ring yet...")
        joinTheRing(currNode.bootstrapHost, currNode.bootstrapPort)
        nexttime += 10
        sleeptime = nexttime - time.time()
        if sleeptime > 0:
            time.sleep(sleeptime)
    else:
        print("\nI'm in the ring...")
        stabilize()
        nexttime += 10
        sleeptime = nexttime - time.time()
        if sleeptime > 0:
            time.sleep(sleeptime)

    for desc in readFD:
        if desc == sys.stdin:
            print("Getting stdin...")
            userInput = sys.stdin.readline()
            print(userInput)
            if userInput == '\n':  # stop when enter key was pressed
                print("stopped")
                mySocket.close()
                keepRunning = False
                break

        elif desc == socketFD:
            # if not currNode.isInRing():
            #     print("\nI'm not in the ring yet...")
            #     joinTheRing(currNode.bootstrapHost, currNode.bootstrapPort)
            #     nexttime += 10
            #     sleeptime = nexttime - time.time()
            #     if sleeptime > 0:
            #         time.sleep(sleeptime)
            # else:
            print("Getting socket message...")
            mySocket.settimeout(10)
            try:
                requestMsg, senderAddr = mySocket.recvfrom(4096)
                print("The received message is: ", requestMsg)
                requestObject = json.loads(requestMsg)
                if "cmd" in requestObject:
                    requestCmd = requestObject['cmd']
                    if requestCmd == 'pred?' or requestCmd == 'setPred':
                        myResponse = replyToRequest(requestMsg)
                        mySocket.sendto(myResponse, senderAddr)
                else:  # in the case there is no "cmd": "myPred" in the response
                    continue
            except socket.timeout as toe:
                print("There is no request message! ", toe)
            # print("\nI'm in the ring...")
            # stabilize()
            # nexttime += 10
            # sleeptime = nexttime - time.time()
            # if sleeptime > 0:
            #     time.sleep(sleeptime)
