#!usr/bin/python

import socket
import select
import random
import json

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
        self.successor = -1
        self.id = random.randint(1, 2 ** key_size - 2)
        self.bootstrapPort = 15000
        self.bootstrapHost = 'silicon.cs.umanitoba.ca'
        self.bootstrapId = 2 ** key_size - 1
        self.bootstrapAddr = (self.bootstrapHost, self.bootstrapPort)
        self.lastKnownResponse = {}

    def setSuccessor(self, newSucc):
        self.successor = newSucc

    def setId(self, newId):
        self.id = newId


currNode = Node()
address = (currNode.host, currNode.port)


def createMessage(jsonObject, command, port, hostname, id):
    jsonObject['cmd'] = command
    jsonObject['port'] = port
    jsonObject['hostname'] = hostname
    jsonObject['ID'] = id
    return json.dumps(jsonObject)


def checkSuccessor(succHostname, succPort):
    mySocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    succAddr = (succHostname, succPort)

    # jsonFile = open("object.json", "r+")
    # jsonContent = json.load(jsonFile)
    # jsonFile.close()
    #
    # jsonContent["cmd"] = "pred?"
    # jsonContent["port"] = currNode.port
    # jsonContent["hostname"] = currNode.host
    # jsonContent["ID"] = currNode.id
    #
    # # jsonFile = open("object.json", "w+")
    # # jsonFile.write(json.dumps(jsonContent))
    # # jsonFile.close()
    #
    # jsonString = json.dumps(jsonContent)
    # print(jsonString)
    # clientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # clientSocket.bind(address)

    predId = 0
    recvData = ''
    receivedMsg = ''
    clientSocket.settimeout(2)

    try:
        clientSocket.sendto(jsonString, succAddr)
        (recvData, addr) = clientSocket.recvfrom(4096)
        receivedMsg = json.loads(recvData)  # receivedMsg is now a json object
        predId = receivedMsg['thePred']['ID']
        currNode.lastKnownResponse = receivedMsg

    except socket.timeout as toe:
        print("Didn't get the respond (check successor)...timeout: ", toe)
        receivedMsg = currNode.lastKnownResponse
        newMessageStr = createMessage(jsonContent, 'setPred', currNode.lastKnownResponse['me']['port'], currNode.lastKnownResponse['me']['hostname'], currNode.lastKnownResponse['me']['ID'])
        clientSocket.sendto(newMessageStr, (currNode.lastKnownResponse['me']['hostname'], currNode.lastKnownResponse['me']['port']))
    print("resoponse: ", recvData)
    print("pred id: ", predId)
    # print(addr)

    # clientSocket.close()
    return receivedMsg


# compare the predecessor id to the node id
def joinTheRing(hostName, port):
    print("******* curr node id: ", currNode.id)
    wholeResponse = ''
    responseStorage = ''
    wholeResponse = checkSuccessor(hostName, port)  # check with the bootstrap now, this is a json object
    if wholeResponse != '':
        responseStorage = wholeResponse
    else:
        wholeResponse = responseStorage

    if wholeResponse['thePred']['ID'] < currNode.id:
        newMessage = wholeResponse['thePred']
        newMessage['cmd'] = 'setPred'
        newMessage['query'] = ''
        newMessage['hops'] = 0
        newMessageStr = json.dumps(newMessage)
        # clientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # clientSocket.bind(address)
        clientSocket.sendto(newMessageStr, (wholeResponse['me']['hostname'], wholeResponse['me']['port']))

        # (recvData, addr) = clientSocket.recvfrom(4096)
        # receivedMsg = json.loads(recvData)
        print("join the ring: ")
        currNode.setSuccessor(wholeResponse['me']['ID'])

    else:
        joinTheRing(wholeResponse['thePred']['hostname'], wholeResponse['thePred']['port'])


jsonFile = open("object.json", "r+")
jsonContent = json.load(jsonFile)
jsonFile.close()

jsonContent["cmd"] = "pred?"
jsonContent["port"] = currNode.port
jsonContent["hostname"] = currNode.host
jsonContent["ID"] = currNode.id

jsonString = json.dumps(jsonContent)
clientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
clientSocket.bind(address)

joinTheRing(currNode.bootstrapHost, currNode.bootstrapPort)
