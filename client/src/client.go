package src

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"paxos_raft/common"
	"paxos_raft/configuration"
	"paxos_raft/proto"
	"strconv"
	"sync"
	"time"
)

/*
	This file defines the client struct and the new method that is invoked when creating a new client by the main
*/

type Client struct {
	clientName  int32 // unique client identifier as defined in the configuration.yml
	numReplicas int   // number of replicas

	replicaAddrList             map[int32]string        // map with the IP:port address of every replica node
	incomingReplicaReaders      map[int32]*bufio.Reader // socket readers for each replica
	outgoingReplicaWriters      map[int32]*bufio.Writer // socket writer for each replica
	outgoingReplicaWriterMutexs map[int32]*sync.Mutex   // for mutual exclusion for each buffio.writer outgoingReplicaWriters

	rpcTable     map[uint8]*common.RPCPair // map each RPC type (message type) to its unique number
	incomingChan chan *common.RPCPair      // used to collect ClientBatch messages for responses and Status messages for responses

	messageCodes proto.MessageCode
	logFilePath  string // the path to write the requests and responses time, used for sanity checks

	clientBatchSize int // maximum client side batch size
	clientBatchTime int // maximum client side batch time in micro seconds

	outgoingMessageChan chan *common.OutgoingRPC // buffer for messages that are written to the wire

	debugOn    bool // if turned on, the debug messages will be printed on the console
	debugLevel int  // current debug level

	testDuration int // test duration in seconds
	arrivalRate  int // poisson rate of the arrivals (requests per second)

	arrivalTimeChan     chan int64              // channel to which the poisson process adds new request arrival times in nanoseconds w.r.t test start time
	arrivalChan         chan bool               // channel to which the main scheduler adds new request indications, to be consumed by the request generation threads
	RequestType         string                  // [request] for sending a stream of client requests, [status] for sending a status request
	OperationType       int                     // status operation type 1 (bootstrap server), 2: print log, 3: start consensus
	sentRequests        [][]requestBatch        // generator i updates sentRequests[i] :this is to avoid concurrent access to the same array
	receivedResponses   map[string]requestBatch // set of received client response batches from replicas: a map is used for fast lookup
	startTime           time.Time               // test start time
	clientListenAddress string                  // TCP address to which the client listens to new incoming TCP connections
	keyLen              int                     // length of key
	valueLen            int                     // length of value

	finished           bool
	window             int64
	numSentBatches     int64
	numReceivedBatches int64
	receivedNumMutex   *sync.Mutex
}

/*
	requestBatch contains a batch that was written to wire, and the time it was written
*/

type requestBatch struct {
	batch proto.ClientBatch
	time  time.Time
}

const statusTimeout = 5               // time to wait for a status request in seconds
const numOutgoingThreads = 200        // number of wire writers: since the I/O writing is expensive we delegate that task to a thread pool and separate from the critical path
const numRequestGenerationThreads = 1 // number of  threads that generate client requests upon receiving an arrival indication
const incomingBufferSize = 1000000    // the size of the buffer which receives all the incoming messages (client response batch messages and client status response message)
const outgoingBufferSize = 1000000    // size of the buffer that collects messages to be written to the wire
const arrivalBufferSize = 1000000     // size of the buffer that collects new request arrivals

/*
	Instantiate a new Client instance, allocate the buffers
*/

func New(name int32, cfg *configuration.InstanceConfig, logFilePath string, clientBatchSize int, clientBatchTime int, testDuration int, arrivalRate int, requestType string, operationType int, debugOn bool, debugLevel int, keyLen int, valLen int, window int64) *Client {
	cl := Client{
		clientName:                  name,
		numReplicas:                 len(cfg.Peers),
		replicaAddrList:             make(map[int32]string),
		incomingReplicaReaders:      make(map[int32]*bufio.Reader),
		outgoingReplicaWriters:      make(map[int32]*bufio.Writer),
		outgoingReplicaWriterMutexs: make(map[int32]*sync.Mutex),
		rpcTable:                    make(map[uint8]*common.RPCPair),
		incomingChan:                make(chan *common.RPCPair, incomingBufferSize),
		messageCodes:                proto.GetRPCCodes(),
		logFilePath:                 logFilePath,
		clientBatchSize:             clientBatchSize,
		clientBatchTime:             clientBatchTime,
		outgoingMessageChan:         make(chan *common.OutgoingRPC, outgoingBufferSize),

		debugOn:    debugOn,
		debugLevel: debugLevel,

		testDuration:        testDuration,
		arrivalRate:         arrivalRate,
		arrivalTimeChan:     make(chan int64, arrivalBufferSize),
		arrivalChan:         make(chan bool, arrivalBufferSize),
		RequestType:         requestType,
		OperationType:       operationType,
		sentRequests:        make([][]requestBatch, numRequestGenerationThreads),
		receivedResponses:   make(map[string]requestBatch),
		startTime:           time.Time{},
		clientListenAddress: common.GetAddress(cfg.Clients, name),
		keyLen:              keyLen,
		valueLen:            valLen,
		finished:            false,
		window:              window,
		numSentBatches:      0,
		numReceivedBatches:  0,
		receivedNumMutex:    &sync.Mutex{},
	}

	//cl.debug("Created a new client instance", 0)

	// initialize replicaAddrList
	for i := 0; i < len(cfg.Peers); i++ {
		int32Name, _ := strconv.ParseInt(cfg.Peers[i].Name, 10, 32)
		cl.replicaAddrList[int32(int32Name)] = cfg.Peers[i].Address
		cl.outgoingReplicaWriterMutexs[int32(int32Name)] = &sync.Mutex{}
	}

	/*
		Register the rpcs
	*/
	cl.RegisterRPC(new(proto.ClientBatch), cl.messageCodes.ClientBatchRpc)
	cl.RegisterRPC(new(proto.Status), cl.messageCodes.StatusRPC)

	//cl.debug("Registered RPCs in the table", 0)

	// Set random seed
	rand.Seed(time.Now().UTC().UnixNano())

	pid := os.Getpid()
	fmt.Printf("initialized client %v with process id: %v \n", name, pid)

	return &cl
}

/*
	fill the RPC table by assigning a unique id to each message type
*/

func (cl *Client) RegisterRPC(msgObj proto.Serializable, code uint8) {
	cl.rpcTable[code] = &common.RPCPair{Code: code, Obj: msgObj}
}

//func (cl *Client) debug(message string, level int) {
//	if cl.debugOn && level >= cl.debugLevel {
//		fmt.Printf("%v\n", message)
//	}
//}
