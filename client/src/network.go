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
	each client sends connection requests to all replicas
*/

func (cl *Client) ConnectToReplicas() {

	//cl.debug("connecting to replicas", 0)

	var b [4]byte
	bs := b[:4]

	//connect to replicas
	for name, address := range cl.replicaAddrList {
		for true {
			conn, err := net.Dial("tcp", address)
			if err == nil {
				cl.outgoingReplicaWriters[name] = bufio.NewWriter(conn)
				binary.LittleEndian.PutUint16(bs, uint16(cl.clientName))
				_, err := conn.Write(bs)
				if err != nil {
					panic("error while connecting to replica " + strconv.Itoa(int(name)))
				}
				//cl.debug("Established outgoing connection to "+strconv.Itoa(int(name)), 0)
				break
			}
		}
	}
	//cl.debug("established outgoing connections to all replicas", 0)
}

/*
	listen on the client port for new connections from replicas
*/

func (cl *Client) WaitForConnections() {
	go func() {
		var b [4]byte
		bs := b[:4]
		Listener, err := net.Listen("tcp", cl.clientListenAddress)
		if err != nil {
			panic("should not happen " + fmt.Sprintf("%v", err))
		}
		//cl.debug("Listening to incoming connection from "+cl.clientListenAddress, 0)

		for true {
			conn, err := Listener.Accept()
			if err != nil {
				panic("Socket accept error" + fmt.Sprintf("%v", err))
			}
			if _, err := io.ReadFull(conn, bs); err != nil {
				panic("connection read error when establishing incoming connections" + fmt.Sprintf("%v", err))
			}
			id := int32(binary.LittleEndian.Uint16(bs))
			//cl.debug("Received incoming connection from "+strconv.Itoa(int(id)), 0)

			cl.incomingReplicaReaders[id] = bufio.NewReader(conn)
			go cl.connectionListener(cl.incomingReplicaReaders[id], id)
			//cl.debug("Started listening to "+strconv.Itoa(int(id)), 0)

		}
	}()
}

/*
	listen to a given connection. Upon receiving any message, put it into the central buffer
*/

func (cl *Client) connectionListener(reader *bufio.Reader, id int32) {

	var msgType uint8
	var err error = nil

	for true {

		if msgType, err = reader.ReadByte(); err != nil {
			//cl.debug("error while reading message code: connection broken from "+strconv.Itoa(int(id)), 0)
			return
		}

		if rpair, present := cl.rpcTable[msgType]; present {
			obj := rpair.Obj.New()
			if err = obj.Unmarshal(reader); err != nil {
				//cl.debug("error while unmarshalling from "+strconv.Itoa(int(id)), 0)
				return
			}
			cl.incomingChan <- &common.RPCPair{
				Code: msgType,
				Obj:  obj,
			}
			//cl.debug("Pushed a message from "+strconv.Itoa(int(id)), 0)

		} else {
			//cl.debug("error received unknown message type from "+strconv.Itoa(int(id)), 0)
			return
		}
	}
}

/*
	this is the main execution thread that listens to all the incoming messages
	It listens to incoming messages from the incomingChan, and invokes the appropriate handler depending on the message type
*/

func (cl *Client) Run() {
	go func() {
		for true {

			//cl.debug("Checking channel..", 0)
			replicaMessage := <-cl.incomingChan
			//cl.debug("Received message", 0)

			switch replicaMessage.Code {
			case cl.messageCodes.ClientBatchRpc:
				clientResponseBatch := replicaMessage.Obj.(*proto.ClientBatch)
				//cl.debug("Client response batch from "+strconv.Itoa(int(clientResponseBatch.Sender)), 0)
				cl.handleClientResponseBatch(clientResponseBatch)
				break

			case cl.messageCodes.StatusRPC:
				clientStatusResponse := replicaMessage.Obj.(*proto.Status)
				//cl.debug("Client status "+fmt.Sprintf("%#v", clientStatusResponse), 0)
				cl.handleClientStatusResponse(clientStatusResponse)
				break
			}
		}
	}()
}

/*
	write a message to the wire, first the message type is written and then the actual message
*/

func (cl *Client) internalSendMessage(peer int32, rpcPair *common.RPCPair) {
	w := cl.outgoingReplicaWriters[peer]
	cl.outgoingReplicaWriterMutexs[peer].Lock()
	err := w.WriteByte(rpcPair.Code)
	if err != nil {
		//cl.debug("Error writing message code byte:"+err.Error(), 0)
		cl.outgoingReplicaWriterMutexs[peer].Unlock()
		return
	}
	err = rpcPair.Obj.Marshal(w)
	if err != nil {
		//cl.debug("error while marshalling:"+err.Error(), 0)
		cl.outgoingReplicaWriterMutexs[peer].Unlock()
		return
	}
	err = w.Flush()
	if err != nil {
		//cl.debug("error while flushing:"+err.Error(), 0)
		cl.outgoingReplicaWriterMutexs[peer].Unlock()
		return
	}
	cl.outgoingReplicaWriterMutexs[peer].Unlock()
	//cl.debug("Internal sent message to "+strconv.Itoa(int(peer)), 0)
}

/*
	A set of threads that manages outgoing messages
*/

func (cl *Client) StartOutgoingLinks() {
	for i := 0; i < numOutgoingThreads; i++ {
		go func() {
			for true {
				outgoingMessage := <-cl.outgoingMessageChan
				cl.internalSendMessage(outgoingMessage.Peer, outgoingMessage.RpcPair)
				//cl.debug("Invoked internal sent to replica "+strconv.Itoa(int(outgoingMessage.Peer)), 0)
			}
		}()
	}
}

/*
	Add a new out-going message to the outgoing channel
*/

func (cl *Client) sendMessage(peer int32, rpcPair common.RPCPair) {
	cl.outgoingMessageChan <- &common.OutgoingRPC{
		RpcPair: &rpcPair,
		Peer:    peer,
	}
	//cl.debug("Added RPC pair to outgoing channel to peer "+strconv.Itoa(int(peer)), 0)
}
