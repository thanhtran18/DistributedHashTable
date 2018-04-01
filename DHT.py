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


# currNode = Node()
# address = (currNode.host, currNode.port)

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

    try:
        (recvData, addr) = mySocket.recvfrom(4096)
        if recvData:
            print("Receiced message from ", addr, " (check successor): ", recvData)
            receivedMsg = json.loads(recvData)  # receivedMsg is now a json object
            if 'me' not in receivedMsg and 'thePred' not in receivedMsg:
                if receivedMsg['port'] == currNode.port and receivedMsg['hostname'] == currNode.host:
                    newMessageStr = createMessage(jsonContent, 'setPred', currNode.port, currNode.host, currNode.id)
                    print("Oops, got a message from an old version of myself. About to rejoin!")
                    print("Sent this 'setPred' message to last known node: ",
                          (currNode.lastKnownResponse['me']['hostname'],
                           currNode.lastKnownResponse['me']['port']), ": ", newMessageStr)
                    mySocket.sendto(newMessageStr, (currNode.lastKnownResponse['me']['hostname'],
                                                    currNode.lastKnownResponse['me']['port']))
                    return {}
                replyToRequest(recvData)
                return currNode.lastKnownResponse
            predId = receivedMsg['thePred']['ID']
            currNode.lastKnownResponse = receivedMsg

    except socket.timeout as toe:
        # step 1 of pred?
        print("Didn't get the respond (check successor)...timeout: ", toe)
        print('Timestamp error: {:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()))
        print("Attempting to set the last known node as my successor to join the ring...")
        receivedMsg = currNode.lastKnownResponse
        # print("last known: ", currNode.lastKnownResponse)
        # jsonContent['query'] = 65535
        newMessageStr = createMessage(jsonContent, 'setPred', currNode.port, currNode.host, currNode.id)
        print("Sent this 'setPred' message to last known node: ", (currNode.lastKnownResponse['me']['hostname'],
                                                   currNode.lastKnownResponse['me']['port']), ": ", newMessageStr)
        mySocket.sendto(newMessageStr, (currNode.lastKnownResponse['me']['hostname'],
                                        currNode.lastKnownResponse['me']['port']))
        currNode.setSuccessor(currNode.lastKnownResponse['me'])
        currNode.setInRing(True)
        receivedMsg = {}
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
    if wholeResponse == {}:
        return
    nextTime = time.time()
    if wholeResponse['thePred']['port'] == currNode.port and wholeResponse['thePred']['ID'] == currNode.id:
        print("======== I'm in the CORRECT position already ========")
        currNode.setInRing(True)
        currNode.setSuccessor(wholeResponse['me'])
        # checkSuccessor(hostname, port)
        # nextTime += 30
        # sleepTime = nextTime - time.time()
        # if sleepTime > 0:
        #     time.sleep(sleepTime)
        return

    if wholeResponse['thePred']['ID'] < currNode.id or wholeResponse['thePred']['ID'] == 0:
        # newMessage = wholeResponse['thePred']
        newMessageStr = createMessage(jsonContent, "setPred", currNode.port, currNode.host, currNode.id)
        # newMessageStr = json.dumps(newMessage)

        mySocket.sendto(newMessageStr, (wholeResponse['me']['hostname'], wholeResponse['me']['port']))
        print("My message with 'setPred' cmd (join the ring): ", newMessageStr)
        currNode.setSuccessor(wholeResponse['me'])
        # logging
        print("My current successor: ", currNode.successor)
        print("My current predecessor: ", (currNode.predecessorHostname, currNode.predecessorPort))
        print("joined the ring!")
        currNode.setInRing(True)

    else:
        joinTheRing(wholeResponse['thePred']['hostname'], wholeResponse['thePred']['port'])


def stabilize():
    print("-------------------------------------------------------------------------------------------")
    # jsonContent['query'] = 65535
    newMessage = createMessage(jsonContent, "pred?", currNode.port, currNode.host, currNode.id)
    succAddr = (currNode.successor['hostname'], currNode.successor['port'])
    predId = 0
    recvData = ''
    receivedMsg = ''
    mySocket.settimeout(2)
    # todo: check every 30 seconds
    try:
        mySocket.sendto(newMessage, succAddr)
        print("Sent 'pred?' to my successor at: ", succAddr)
        (recvData, addr) = mySocket.recvfrom(4096)
        # print("Sender's address: ", address)
        print("Received successor's message (stabilize): ", recvData)
        receivedMsg = json.loads(recvData)  # receivedMsg is now a json object
        if 'me' not in receivedMsg and 'thePred' not in receivedMsg:
            # if requestMsg['port'] == currNode.port and requestMsg['hostname'] == currNode.host:
            #
            updatePredMsg = replyToRequest(recvData)
            mySocket.sendto(updatePredMsg, addr)
            return
        predId = receivedMsg['thePred']['ID']
        if predId > currNode.id:  # if successor's predecessor is greater (after) you...
            print("Trying to stabilize - Sending 'setPred' to 'thePred' and set my successor to be 'thePred'...")
            updatePredMsg = createMessage(jsonContent, "setPred", currNode.port, currNode.host, currNode.id)
            newSuccAddr = (receivedMsg['thePred']['hostname'], receivedMsg['thePred']['port'])
            mySocket.sendto(updatePredMsg, newSuccAddr)
            currNode.setSuccessor(receivedMsg['thePred'])
        elif predId < currNode.id:  # if successor's predecessor is greater (after) you...
            print("Tryin to stabilize - Sending 'setPred' to 'me'...")
            updatePredMsg = createMessage(jsonContent, "setPred", currNode.port, currNode.host, currNode.id)
            newSuccAddr = (receivedMsg['me']['hostname'], receivedMsg['me']['port'])
            mySocket.sendto(updatePredMsg, newSuccAddr)
        else:
            print("\nThis is me in stabilization (in the correct position)...\n")

    except socket.timeout as toe:
        currNode.setInRing(False)
        print("Didn't get the respond (stabilize)...timeout: ", toe)
        print('Timestamp error: {:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()))
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
        print("This is my response: ", responseMsg)

    elif requestCmd == 'setPred':
        # save 'me' (the sender) as my pred now
        print("My predecessor has changed from: ", (currNode.predecessorHostname, currNode.predecessorPort, "... to ..."))
        currNode.savePredHost(inputObject['hostname'])
        currNode.savePredId(inputObject['ID'])
        currNode.savePredPort(inputObject['port'])
        print((currNode.predecessorHostname, currNode.predecessorPort))

    # elif requestCmd == 'find':

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
timer = 0

# handle input
# todo: should be able to stop immediately after pressing enter
# todo: get the int
# todo: handle the case where you type in something else other than an int, it should generate a number

keepRunning = True
while keepRunning:
    socketFD = mySocket.fileno()  # file descriptor (an int)
    (readFD, writeFD, errorFD) = select.select([socketFD, sys.stdin], [], [], 0.0)

    if not currNode.isInRing():
        print("\nI'm not in the ring yet...")
        joinTheRing(currNode.bootstrapHost, currNode.bootstrapPort)
    else:
        # print("\nI'm in the ring now...")
        time.sleep(0.5)
        timer += 0.5
        # print("timer: ", timer)
        if 10 <= timer <= 11:
            stabilize()
            timer = 0
        # nexttime += 10
        # sleeptime = nexttime - time.time()
        # if sleeptime > 0:
        #     time.sleep(sleeptime)
    # else:
    #     print("\nI'm in the ring...")
    #     stabilize()
        # nexttime += 10
        # sleeptime = nexttime - time.time()
        # print("sleeo time: ", sleeptime)
        # # if sleeptime == 10:
        # #     stabilize()
        # if sleeptime > 0:
        #     time.sleep(sleeptime)

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
            else:  # handling the query key
                try:
                    queryKey = int(userInput)
                except ValueError:
                    print("Wrong input format. It's supposed to be an int")
                    print("Generating a new query...")
                    queryKey = random.randint(currNode.id + 1, 2 ** key_size - 1)
                finally:
                    while queryKey <= currNode.id:
                        queryKey = random.randint(currNode.id + 1, 2 ** key_size - 1)
                    findMessage = createMessageWithQuery(jsonContent, "find", currNode.port, currNode.host, currNode.id,
                                                         int(userInput), currNode.hops)
                    print("Sent this 'find' message to ", (currNode.successor['hostname'], currNode.successor['port']),
                          ': ', findMessage)
                    mySocket.sendto(findMessage, (currNode.successor['hostname'], currNode.successor['port']))

        elif desc == socketFD:
            # if not currNode.isInRing():
            #     print("\nI'm not in the ring yet...")
            #     joinTheRing(currNode.bootstrapHost, currNode.bootstrapPort)
            #     nexttime += 10
            #     sleeptime = nexttime - time.time()
            #     if sleeptime > 0:
            #         time.sleep(sleeptime)
            # else:
            print("-------------------------------------------------------------------------\nGetting socket message...")
            mySocket.settimeout(10)
            try:
                requestMsg, senderAddr = mySocket.recvfrom(4096)
                if requestMsg == '':
                    print("The received message is empty!")
                    continue
                print("Sender: ", senderAddr)
                print("The received message is: ", requestMsg)
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
                    elif requestCmd == 'find':
                        # pass the message to my successor if the query is not my id
                        if requestObject['query'] != currNode.id:
                            myResponse = createMessageWithQuery(requestObject, requestObject['cmd'],
                                                                requestObject['port'], requestObject['hostname'],
                                                                requestObject['id'], requestObject['query'],
                                                                requestObject['hops'] + 1)
                            myResponse = json.dumps(requestObject)
                            mySocket.sendto(myResponse, (currNode.successor['hostname'], currNode.successor['port']))
                        else:
                            myResponse = createMessageWithQuery(jsonContent, "owner", currNode.port, currNode.host,
                                                                currNode.id, requestObject['query'], 0)
                            # todo: handle the case where the find message's query is a not number
                            mySocket.sendto(myResponse, (requestObject['hostname'], requestObject['port']))
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
    # time.sleep(0.1)
    # timer += 0.1
    # if timer == 10:
    #     stabilize()
    #     timer = 0

