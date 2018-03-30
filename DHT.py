#!usr/bin/python

import socket
import select
import random
import json
import datetime
import time

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
        self.predecessor = 0
        self.successor = {}
        self.id = random.randint(1, 2 ** key_size - 2)
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


currNode = Node()
address = (currNode.host, currNode.port)


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
            print("&&&&&&& ", recvData)
            receivedMsg = json.loads(recvData)  # receivedMsg is now a json object
            predId = receivedMsg['thePred']['ID']
            currNode.lastKnownResponse = receivedMsg
        # print('Timestamp: {:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()))

    except socket.timeout as toe:
        # step 1 of pred?
        print("Didn't get the respond (check successor)...timeout: ", toe)
        print('Timestamp error: {:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()))
        receivedMsg = currNode.lastKnownResponse
        joinTheRing(succHostname, succPort)

        # newMessageStr = createMessage(jsonContent, 'setPred', currNode.lastKnownResponse['me']['port'], currNode.lastKnownResponse['me']['hostname'], currNode.lastKnownResponse['me']['ID'])
        # mySocket.sendto(newMessageStr, (currNode.lastKnownResponse['me']['hostname'], currNode.lastKnownResponse['me']['port']))
    # print("resoponse: ", recvData)
    print("pred id: ", predId)
    # print(addr)

    # clientSocket.close()
    return receivedMsg


# hostname of port of the successor
def joinTheRing(hostname, port):
    print("-------------------------------------------------------------------------------------------")
    print("curr node id: ", currNode.id)
    wholeResponse = ''
    responseStorage = ''
    wholeResponse = checkSuccessor(hostname, port)  # check with the bootstrap now, this is a json object
    # if wholeResponse != '':
    #     responseStorage = wholeResponse
    # else:
    #     wholeResponse = responseStorage
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
        newMessage = wholeResponse['thePred']
        newMessage['cmd'] = 'setPred'
        newMessage['query'] = ''
        newMessage['hops'] = 0
        newMessageStr = json.dumps(newMessage)

        mySocket.sendto(newMessageStr, (wholeResponse['me']['hostname'], wholeResponse['me']['port']))

        print("join the ring: ")
        currNode.setSuccessor(wholeResponse['me'])
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
        (recvData, addr) = mySocket.recvfrom(4096)
        print("Successor's message (stabilize): ", recvData)
        receivedMsg = json.loads(recvData)  # receivedMsg is now a json object
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
        print("Didn't get the respond (stabilize)...timeout: ", toe)
        print('Timestamp error: {:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()))
        joinTheRing(currNode.bootstrapHost, currNode.bootstrapPort)


# def replyToRequest(requestCmd,)

# MAIN


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
while True:
    if not currNode.inRing:
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

# joinTheRing(currNode.bootstrapHost, currNode.bootstrapPort)
