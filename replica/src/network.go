package src

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"paxos_raft/common"
	"paxos_raft/proto"
	"strconv"
)

/*
	given an int32 id, connect to that node
	nodeType should be one of client and replica
*/

func (rp *Replica) ConnectToNode(id int32, address string, nodeType string) {

	//rp.debug("Connecting to "+strconv.Itoa(int(id)), 0)

	var b [4]byte
	bs := b[:4]

	for true {
		conn, err := net.Dial("tcp", address)
		if err == nil {

			if nodeType == "client" {
				rp.outgoingClientWriters[id] = bufio.NewWriter(conn)
				binary.LittleEndian.PutUint16(bs, uint16(rp.name))
				_, err := conn.Write(bs)
				if err != nil {
					panic("Error while connecting to client " + strconv.Itoa(int(id)))
				}
			} else if nodeType == "replica" {
				rp.outgoingReplicaWriters[id] = bufio.NewWriter(conn)
				binary.LittleEndian.PutUint16(bs, uint16(rp.name))
				_, err := conn.Write(bs)
				if err != nil {
					panic("Error while connecting to replica " + strconv.Itoa(int(id)))
				}
			} else {
				panic("Unknown node id")
			}
			//rp.debug("Established outgoing connection to "+strconv.Itoa(int(id)), 0)
			break
		}
	}
}

/*
	Connect to all replicas on bootstrap
*/

func (rp *Replica) ConnectBootStrap() {

	for name, address := range rp.replicaAddrList {
		rp.ConnectToNode(name, address, "replica")
	}
}

/*
	listen to a given connection reader. Upon receiving any message, put it into the central incoming buffer
*/

func (rp *Replica) connectionListener(reader *bufio.Reader, id int32) {

	var msgType uint8
	var err error = nil

	for true {

		if msgType, err = reader.ReadByte(); err != nil {
			//rp.debug("Error while reading message code: connection broken from "+strconv.Itoa(int(id))+fmt.Sprintf(" %v", err), 0)
			return
		}

		if rpair, present := rp.rpcTable[msgType]; present {
			obj := rpair.Obj.New()
			if err = obj.Unmarshal(reader); err != nil {
				//rp.debug("Error while unmarshalling from "+strconv.Itoa(int(id))+fmt.Sprintf(" %v", err), 0)
				return
			}
			rp.incomingChan <- &common.RPCPair{
				Code: msgType,
				Obj:  obj,
			}
			//rp.debug("Pushed a message from "+strconv.Itoa(int(id)), 0)

		} else {
			//rp.debug("Error received unknown message type from "+strconv.Itoa(int(id)), 0)
			return
		}
	}
}

/*
	listen on the replica port for new connections from clients, and all replicas
*/

func (rp *Replica) WaitForConnections() {
	go func() {
		var b [4]byte
		bs := b[:4]
		Listener, _ := net.Listen("tcp", rp.listenAddress)
		//rp.debug("Listening to incoming connections in "+rp.listenAddress, 0)

		for true {
			conn, err := Listener.Accept()
			if err != nil {
				panic(err.Error() + fmt.Sprintf("%v", err))
			}
			if _, err := io.ReadFull(conn, bs); err != nil {
				panic(err.Error() + fmt.Sprintf("%v", err))
			}
			id := int32(binary.LittleEndian.Uint16(bs))
			//rp.debug("Received incoming connection from "+strconv.Itoa(int(id)), 0)
			nodeType := rp.getNodeType(id)
			if nodeType == "client" {
				rp.incomingClientReaders[id] = bufio.NewReader(conn)
				go rp.connectionListener(rp.incomingClientReaders[id], id)
				//rp.debug("Started listening to client "+strconv.Itoa(int(id)), 0)
				rp.ConnectToNode(id, rp.clientAddrList[id], "client")

			} else if nodeType == "replica" {
				rp.incomingReplicaReaders[id] = bufio.NewReader(conn)
				go rp.connectionListener(rp.incomingReplicaReaders[id], id)
				//rp.debug("Started listening to replica "+strconv.Itoa(int(id)), 0)
			} else {
				panic("should not happen")
			}
		}
	}()
}

/*
	this is the main execution thread that listens to all the incoming messages
	It listens to incoming messages from the incomingChan, and invokes the appropriate handler depending on the message type
*/

func (rp *Replica) Run() {

	for true {
		select {
		case replicaMessage := <-rp.incomingChan:

			//rp.debug("Received replica message", 0)

			switch replicaMessage.Code {

			case rp.messageCodes.StatusRPC:
				statusMessage := replicaMessage.Obj.(*proto.Status)
				//rp.debug("Status message from "+fmt.Sprintf("%#v", statusMessage.Sender), 0)
				rp.handleStatus(statusMessage)
				break

			case rp.messageCodes.ClientBatchRpc:
				clientBatch := replicaMessage.Obj.(*proto.ClientBatch)
				//rp.debug("Client batch message from "+fmt.Sprintf("%#v", clientBatch.Sender), 0)
				rp.handleClientBatch(clientBatch)
				break

			case rp.messageCodes.PaxosConsensus:
				paxosConsensusMessage := replicaMessage.Obj.(*proto.PaxosConsensus)
				//rp.debug("Paxos consensus message from "+fmt.Sprintf("%#v", paxosConsensusMessage.Sender), 0)
				rp.handlePaxosConsensus(paxosConsensusMessage)
				break

			}
			break
		case clientRespBatches := <-rp.requestsOut:
			rp.sendClientResponses(clientRespBatches)
			break
		}

	}
}

/*
	Write a message to the wire, first the message type is written and then the actual message
*/

func (rp *Replica) internalSendMessage(peer int32, rpcPair *common.RPCPair) {
	peerType := rp.getNodeType(peer)
	if peerType == "replica" {
		w := rp.outgoingReplicaWriters[peer]
		if w == nil {
			panic("replica not found")
		}
		rp.outgoingReplicaWriterMutexs[peer].Lock()
		err := w.WriteByte(rpcPair.Code)
		if err != nil {
			//rp.debug("Error writing message code byte:"+err.Error(), 0)
			rp.outgoingReplicaWriterMutexs[peer].Unlock()
			return
		}
		err = rpcPair.Obj.Marshal(w)
		if err != nil {
			//rp.debug("Error while marshalling:"+err.Error(), 0)
			rp.outgoingReplicaWriterMutexs[peer].Unlock()
			return
		}
		err = w.Flush()
		if err != nil {
			//rp.debug("Error while flushing:"+err.Error(), 0)
			rp.outgoingReplicaWriterMutexs[peer].Unlock()
			return
		}
		rp.outgoingReplicaWriterMutexs[peer].Unlock()
		//rp.debug("Internal sent message to "+strconv.Itoa(int(peer)), 0)
	} else if peerType == "client" {
		w := rp.outgoingClientWriters[peer]
		if w == nil {
			panic("client not found")
		}
		rp.outgoingClientWriterMutexs[peer].Lock()
		err := w.WriteByte(rpcPair.Code)
		if err != nil {
			//rp.debug("Error writing message code byte:"+err.Error(), 0)
			rp.outgoingClientWriterMutexs[peer].Unlock()
			return
		}
		err = rpcPair.Obj.Marshal(w)
		if err != nil {
			//rp.debug("Error while marshalling:"+err.Error(), 0)
			rp.outgoingClientWriterMutexs[peer].Unlock()
			return
		}
		err = w.Flush()
		if err != nil {
			//rp.debug("Error while flushing:"+err.Error(), 0)
			rp.outgoingClientWriterMutexs[peer].Unlock()
			return
		}
		rp.outgoingClientWriterMutexs[peer].Unlock()
		//rp.debug("Internal sent message to "+strconv.Itoa(int(peer)), 0)
	} else {
		panic("Unknown id from node name " + strconv.Itoa(int(peer)))
	}
}

/*
	A set of threads that manages outgoing messages
*/

func (rp *Replica) StartOutgoingLinks() {
	for i := 0; i < numOutgoingThreads; i++ {
		go func() {
			for true {
				outgoingMessage := <-rp.outgoingMessageChan
				rp.internalSendMessage(outgoingMessage.Peer, outgoingMessage.RpcPair)
				//rp.debug("Invoked internal sent to "+strconv.Itoa(int(outgoingMessage.Peer)), 0)
			}
		}()
	}
}

/*
	Add a new out-going message to the outgoing channel
*/

func (rp *Replica) sendMessage(peer int32, rpcPair common.RPCPair) {
	rp.outgoingMessageChan <- &common.OutgoingRPC{
		RpcPair: &rpcPair,
		Peer:    peer,
	}
	//rp.debug("Added RPC pair to outgoing channel to peer "+strconv.Itoa(int(peer)), 0)
}
