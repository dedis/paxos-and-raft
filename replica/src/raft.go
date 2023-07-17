package src

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"math/rand"
	"net"
	"os"
	"paxos_raft/configuration"
	"paxos_raft/proto"
	"strconv"
	"sync"
	"time"
)

// for gRPC backward compatibility

func (in *Raft) mustEmbedUnimplementedConsensusServer() {
	panic("implement me")
}

// a single raft entry content

type entry struct {
	id       int64
	term     int64
	commands proto.ReplicaBatch
	decided  bool
}

type Raft struct {
	id         int32
	cfg        configuration.InstanceConfig
	debugOn    bool
	debugLevel int

	server   *grpc.Server // gRPC server
	listener net.Listener // socket for gRPC connections
	address  string       // address to listen for gRPC connections
	peers    []peer

	centralMutex *sync.Mutex
	numNodes     int64

	currentTerm    int64
	votedFor       map[int32]int32
	log            []entry
	commitIndex    int64
	nextFreeChoice int64

	viewTimeOut int64 //constant

	logFilePath string

	lastSeenTimeLeader time.Time

	state string

	requestsIn  chan []*proto.ClientBatch
	requestsOut chan []*proto.ClientBatch

	proposalCounter int

	replica                *Replica
	startedFailureDetector bool
	cancel                 chan bool
	startTime              time.Time
}

// create a new raft instance

func NewRaft(id int32, cfg configuration.InstanceConfig, debugOn bool, debugLevel int, address string, numNodes int64, viewTimeOut int64, logFilePath string, requestsIn chan []*proto.ClientBatch, requestsOut chan []*proto.ClientBatch, replica *Replica, cancel chan bool) *Raft {

	r := &Raft{
		id:           id,
		cfg:          cfg,
		debugOn:      debugOn,
		debugLevel:   debugLevel,
		server:       nil,
		listener:     nil,
		address:      address,
		peers:        nil,
		centralMutex: &sync.Mutex{},
		numNodes:     numNodes,
		currentTerm:  1,
		votedFor:     make(map[int32]int32),
		log: []entry{{
			id:   0,
			term: 1,
			commands: proto.ReplicaBatch{
				UniqueId: "nil",
				Requests: []*proto.ClientBatch{{
					UniqueId: "nil",
					Requests: make([]*proto.SingleOperation, 0),
					Sender:   -1,
				}},
				Sender: -1,
			},
			decided: true,
		}},
		commitIndex:            0,
		nextFreeChoice:         1,
		viewTimeOut:            viewTimeOut,
		logFilePath:            logFilePath,
		lastSeenTimeLeader:     time.Time{},
		state:                  "F",
		requestsIn:             requestsIn,
		requestsOut:            requestsOut,
		proposalCounter:        1,
		replica:                replica,
		startedFailureDetector: false,
		cancel:                 cancel,
	}

	r.votedFor[1] = 1
	if r.id == 1 {
		r.state = "L"
	}
	return r
}

// a grpc peer

type peer struct {
	name   int32
	client ConsensusClient
}

// start listening to gRPC connection

func (in *Raft) NetworkInit() {
	in.server = grpc.NewServer()
	RegisterConsensusServer(in.server, in)

	// start listener
	listener, err := net.Listen("tcp", in.address)
	if err != nil {
		panic(err)
	}
	in.listener = listener
	go func() {
		err := in.server.Serve(listener)
		if err != nil {
			panic("should not happen")
		}
	}()

	//in.debug("started listening to grpc  ", 7)
}

// setup gRPC clients to all replicas and return the connection pointers

func (in *Raft) SetupgRPC() {
	peers := make([]peer, 0)
	for _, peeri := range in.cfg.Peers {
		conn, err := grpc.Dial(peeri.GAddress, grpc.WithInsecure())
		if err != nil {
			fmt.Fprintf(os.Stderr, "dial: %v", err)
			os.Exit(1)
		}
		intName, _ := strconv.Atoi(peeri.Name)
		newClient := NewConsensusClient(conn)
		peers = append(peers, peer{
			name:   int32(intName),
			client: newClient,
		})
	}
	in.peers = peers
	//in.debug("setup all the grpc clients", 7)
}

///*
//	if turned on, print the message to console
//*/
//
//func (in *Raft) debug(message string, level int) {
//	if in.debugOn && level >= in.debugLevel {
//		fmt.Printf("%s\n", message)
//	}
//}

// listen to the incoming channel, make a replica batch, replicate it and reply back

func (in *Raft) proposeBatch() {

	go func() {
		for true {
			requests := <-in.requestsIn
			clientResponses := in.appendEntries(requests)
			//in.debug("raft proposed batch of client batches of size "+strconv.Itoa(len(requests)), 0)
			if clientResponses != nil {
				//in.debug("sent back 1 batch of client responses", 5)
				in.requestsOut <- clientResponses
			}
		}
	}()
}

// create new entries in the log

func (in *Raft) createNChoices(number int64) {

	for i := 0; int64(i) < number; i++ {

		in.log = append(in.log, entry{
			id:      in.nextFreeChoice,
			term:    in.currentTerm,
			decided: false,
		})

		in.nextFreeChoice++
	}
}

// compare the last log position with that of candidate's values

func (in *Raft) compareLog(lastLogIndex int64, lastLogTerm int64) bool {

	myLastLogIndex := int64(len(in.log) - 1)
	myLastLogTerm := in.log[myLastLogIndex].term

	if myLastLogTerm > lastLogTerm {
		return false
	} else if myLastLogTerm < lastLogTerm {
		return true
	} else if myLastLogTerm == lastLogTerm {
		if myLastLogIndex > lastLogIndex {
			return false
		} else {
			return true
		}
	}
	panic("should not happen")
}

// receiver code for leader request

func (in *Raft) RequestVote(ctx context.Context, req *proto.LeaderRequest) (*proto.LeaderResponse, error) {
	in.centralMutex.Lock()
	if !in.startedFailureDetector {
		in.startedFailureDetector = true
		in.centralMutex.Unlock()
		in.startViewTimeoutChecker(in.cancel)
	} else {
		in.centralMutex.Unlock()
	}
	in.centralMutex.Lock()
	defer in.centralMutex.Unlock()

	var leaderResponse proto.LeaderResponse

	in.lastSeenTimeLeader = time.Now()

	currentTerm := in.currentTerm

	if currentTerm >= req.Term {
		leaderResponse.Term = currentTerm
		leaderResponse.VoteGranted = false
		//in.debug("request vote failed due to older term", 7)
	} else {
		result := in.compareLog(req.LastLogIndex, req.LastLogTerm)
		if result {
			in.currentTerm = req.Term
			in.votedFor[int32(in.currentTerm)] = req.CandidateId
			in.state = "F"
			leaderResponse.Term = req.Term
			leaderResponse.VoteGranted = true
			//in.debug("leader for term "+strconv.Itoa(int(in.currentTerm))+" is "+strconv.Itoa(int(req.CandidateId)), 7)

		} else {
			leaderResponse.Term = currentTerm
			leaderResponse.VoteGranted = false
			//in.debug("request vote failed due to log mismatch", 7)
		}
	}

	return &leaderResponse, nil
}

// receiver side of the append entries RPC

func (in *Raft) AppendEntries(ctx context.Context, req *proto.AppendRequest) (*proto.AppendResponse, error) {
	in.centralMutex.Lock()
	if !in.startedFailureDetector {
		in.startedFailureDetector = true
		in.centralMutex.Unlock()
		in.startViewTimeoutChecker(in.cancel)
	} else {
		in.centralMutex.Unlock()
	}
	in.centralMutex.Lock()
	defer in.centralMutex.Unlock()

	in.lastSeenTimeLeader = time.Now()

	var appendEntryResponse proto.AppendResponse

	currentTerm := in.currentTerm

	if req.Term < currentTerm {

		appendEntryResponse.Term = currentTerm
		appendEntryResponse.Success = false
		//in.debug("append request failed due to older term", 7)
		return &appendEntryResponse, nil
	}

	lenLog := int64(len(in.log))

	if req.PrevLogIndex+1 > lenLog {

		appendEntryResponse.Term = currentTerm
		appendEntryResponse.Success = false
		//in.debug("append entries failed due to stale log", 7)
		return &appendEntryResponse, nil
	} else {
		// PrevLogIndex exists
		prevLogTerm := in.log[req.PrevLogIndex].term
		prevLogValues := in.log[req.PrevLogIndex].commands

		if prevLogTerm != req.PrevLogTerm || prevLogValues.UniqueId != req.PrevLogValue {
			appendEntryResponse.Term = currentTerm
			appendEntryResponse.Success = false
			//in.debug("append entries failed due to previous log mismatch", 7)
			return &appendEntryResponse, nil
		} else {

			//prevLogTerm matching!!!
			lenLeaderLog := req.PrevLogIndex + 1 + int64(len(req.Entries))

			lenMyLog := int64(len(in.log))

			numMissingEntries := lenLeaderLog - lenMyLog

			if numMissingEntries > 0 {
				in.createNChoices(numMissingEntries)
			}

			if req.Term > currentTerm {
				in.currentTerm = req.Term
				in.state = "F"

			} else if req.Term == currentTerm {
				in.state = "F"
			}

			for i := int64(0); i < int64(len(req.Entries)); i++ {

				in.log[req.PrevLogIndex+1+i].term = req.Entries[i].Term
				in.log[req.PrevLogIndex+1+i].commands = *req.Entries[i].Value
			}

			//in.debug("append entry succeeded", 7)

			in.updateRaftSMR(int(req.LeaderCommit))

			appendEntryResponse.Term = req.Term
			appendEntryResponse.Success = true
			return &appendEntryResponse, nil

		}
	}
}

// candidate sending a new leader request

func (in *Raft) requestVote() bool {

	in.lastSeenTimeLeader = time.Now()
	term := in.currentTerm

	type response struct {
		term        int64
		voteGranted bool
	}

	responses := make(chan *response, in.numNodes)
	candidateId := in.id
	lastLogIndex := int64(len(in.log) - 1)
	lastLogTerm := in.log[lastLogIndex].term

	responses <- &response{
		term:        term,
		voteGranted: true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(20)*time.Second)

	wg := sync.WaitGroup{}
	for _, p := range in.peers {
		if p.name == candidateId {
			continue
		}
		wg.Add(1)
		go func(p peer) {
			defer wg.Done()
			resp, err := p.client.RequestVote(ctx, &proto.LeaderRequest{
				Term:         term,
				CandidateId:  candidateId,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})

			if err != nil {
				responses <- &response{term: -1, voteGranted: false}
				//in.debug("request vote failed with"+err.Error(), 0)
				return
			} else {
				responses <- &response{term: resp.Term, voteGranted: resp.VoteGranted}
				//in.debug("request vote received for term "+strconv.Itoa(int(resp.Term)), 0)
				return
			}
		}(p)
	}

	go func() {
		wg.Wait()
		cancel()
		close(responses)
	}()

	// count the votes
	yea, nay := 0, 0
	canceled := false

	foundHigherTerm := false

	for r := range responses {
		if r.voteGranted {
			yea++
		} else {
			nay++

		}

		if r.term > term {
			foundHigherTerm = true
		}

		// stop counting as soon as we have a majority
		if !canceled {
			if int64(yea) == in.numNodes/2+1 || int64(nay) == in.numNodes/2+1 {
				canceled = true
				break
			}
		}
	}

	return (int64(yea) == in.numNodes/2+1) && !foundHigherTerm
}

// leader sending append entries rpc

func (in *Raft) appendEntries(values []*proto.ClientBatch) []*proto.ClientBatch {
	in.centralMutex.Lock()
	if !in.startedFailureDetector {
		in.startedFailureDetector = true
		in.centralMutex.Unlock()
		in.startViewTimeoutChecker(in.cancel)
	} else {
		in.centralMutex.Unlock()
	}
	in.centralMutex.Lock()
	if in.id != in.votedFor[int32(in.currentTerm)] || in.state != "L" {
		in.centralMutex.Unlock()
		return nil
	}

	in.lastSeenTimeLeader = time.Now()

	var lastIndex int64
	var term int64
	var prevLogIndex int64
	var prevLogTerm int64
	var prevLogValue string
	var leaderCommit int64
	var leaderId int32
	var entries []*proto.AppendRequestEntry

	in.createNChoices(1)
	in.proposalCounter++

	lastIndex = int64(len(in.log)) - 1
	term = in.currentTerm
	in.log[lastIndex].commands = proto.ReplicaBatch{
		UniqueId: strconv.Itoa(int(in.id)) + ":" + strconv.Itoa(in.proposalCounter),
		Requests: values,
		Sender:   int64(in.id),
	}
	in.log[lastIndex].term = term
	prevLogIndex = int64(len(in.log)) - 2
	prevLogTerm = in.log[prevLogIndex].term
	prevLogValue = in.log[prevLogIndex].commands.UniqueId
	leaderCommit = in.commitIndex
	leaderId = in.id
	entries = []*proto.AppendRequestEntry{{Value: &in.log[lastIndex].commands, Term: term}}

	type response struct {
		success bool
		term    int64
	}

	responses := make(chan *response, in.numNodes-1)
	//in.debug("invoking raft append entry for index "+strconv.Itoa(int(lastIndex)), 7)
	in.centralMutex.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(10)*time.Second)
	cancelled := false
	wg := sync.WaitGroup{}

	if in.replica.isAsynchronous {

		epoch := time.Now().Sub(in.startTime).Milliseconds() / int64(in.replica.timeEpochSize)

		if in.replica.amIAttacked(int(epoch)) {
			time.Sleep(time.Duration(in.replica.asyncSimulationTimeout) * time.Millisecond)
		}
	}

	for i, p := range in.peers {
		if p.name == in.id {
			continue
		}
		wg.Add(1)

		// send commit requests
		go func(p peer, peerIndex int, termA int64, prevLogIndexA int64, prevLogTermA int64, prevLogValueA string, leaderCommitA int64, leaderIdA int32, entriesA []*proto.AppendRequestEntry) {
			defer wg.Done()

			termLocal := termA
			prevLogIndexLocal := prevLogIndexA
			prevLogTermLocal := prevLogTermA
			prevLogValueLocal := prevLogValueA
			leaderCommitLocal := leaderCommitA
			leaderIdLocal := leaderIdA
			entriesLocal := entriesA

		retry:
			resp, err := p.client.AppendEntries(ctx, &proto.AppendRequest{
				Term:         termLocal,
				LeaderId:     leaderIdLocal,
				PrevLogIndex: prevLogIndexLocal,
				PrevLogTerm:  prevLogTermLocal,
				PrevLogValue: prevLogValueLocal,
				Entries:      entriesLocal,
				LeaderCommit: leaderCommitLocal,
			})

			if err != nil {
				responses <- &response{success: false, term: -1}
				//in.debug("raft append entry failed with "+err.Error(), 0)
				return
			} else {
				respTerm := resp.Term
				respSuccess := resp.Success

				if respSuccess {
					responses <- &response{success: true, term: respTerm}
					//in.debug("append entry rpc succeeded"+fmt.Sprintf("%v", respSuccess), 0)
					return
				} else if respTerm > termLocal {
					responses <- &response{success: false, term: respTerm}
					//in.debug("append entry rpc failed with higher term", 0)
					return
				} else if !respSuccess && respTerm <= termLocal {
					//in.debug("append entry rpc failed with previous index mismatch", 0)
					// retry
					if prevLogIndexLocal >= 1 {
						prevLogIndexLocal--
						in.centralMutex.Lock()
						prevLogTermLocal = in.log[prevLogIndexLocal].term
						prevLogValueLocal = in.log[prevLogIndexLocal].commands.UniqueId
						prevValues := in.log[prevLogIndexLocal+1].commands
						prevTerm := in.log[prevLogIndexLocal+1].term
						in.centralMutex.Unlock()
						entriesLocal = append([]*proto.AppendRequestEntry{{Value: &prevValues, Term: prevTerm}}, entriesLocal...)

					} else {
						responses <- &response{success: false, term: respTerm}
						return
					}
					goto retry
				}
			}

		}(p, i, term, prevLogIndex, prevLogTerm, prevLogValue, leaderCommit, leaderId, entries)
	}

	// close responses channel once all responses have been received, failed, or canceled
	go func() {
		wg.Wait()
		cancel()
		close(responses)
	}()

	var clientResponses []*proto.ClientBatch

	// count the vote
	yea, nay := 1, 0 // we just committed our own data. make it count.
	foundHigherTerm := false
	for r := range responses {
		if r.success {
			yea++
		} else {
			nay++
		}

		if r.term > term {
			foundHigherTerm = true
		}

		if !cancelled {
			if int64(yea) == in.numNodes/2+1 || int64(nay) == in.numNodes/2+1 {
				cancelled = true
				if int64(nay) == in.numNodes/2+1 || foundHigherTerm {
					in.state = "F"
					clientResponses = nil
				} else if int64(yea) == in.numNodes/2+1 {
					// decide
					//in.debug("append entry succeeded for index "+strconv.Itoa(len(in.log)-1), 7)
					in.centralMutex.Lock()
					clientResponses = in.updateRaftSMR((len(in.log)) - 1)
					in.centralMutex.Unlock()
				}
				break
			}
		}
	}

	return clientResponses
}

// check for leader liveness

func (in *Raft) startViewTimeoutChecker(cancel chan bool) {
	go func() {
		rand.Seed(time.Now().UnixNano() + int64(in.id))

		for {
			select {
			case _ = <-cancel:
				return
			default:
				time.Sleep(time.Duration(in.viewTimeOut+int64(rand.Intn(int(in.viewTimeOut)))) * time.Microsecond)
				in.centralMutex.Lock()
				lastSeenTimeLeader := in.lastSeenTimeLeader
				state := in.state
				in.centralMutex.Unlock()
				if time.Now().Sub(lastSeenTimeLeader).Microseconds() > in.viewTimeOut && state != "L" {
					//in.debug("leader timeout!", 7)
					in.centralMutex.Lock()
					in.state = "C"
					in.currentTerm++
					in.votedFor[int32(in.currentTerm)] = in.id
					leaderElected := in.requestVote()
					if leaderElected {
						fmt.Printf("%v became the leader in term %v \n", in.id, in.currentTerm)
						in.state = "L"
						in.lastSeenTimeLeader = time.Now()
					} else {
						//in.debug("leader election failed", 7)
						in.state = "F"
					}
					in.centralMutex.Unlock()
				}
				break
			}
		}
	}()
}

// update smr

func (in *Raft) updateRaftSMR(commitIndex int) []*proto.ClientBatch {
	responses := make([]*proto.ClientBatch, 0)
	for commitIndex > int(in.commitIndex) {
		in.log[in.commitIndex+1].decided = true
		responses = append(responses, in.replica.updateApplicationLogic(in.log[in.commitIndex+1].commands.Requests)...)
		//in.debug("committed index "+fmt.Sprintf("%v", in.commitIndex+1), 7)
		in.commitIndex++
	}
	return responses
}

// print log

func (rp *Replica) printRaftLogConsensus() {
	f, err := os.Create(rp.logFilePath + strconv.Itoa(int(rp.name)) + "-consensus.txt")
	if err != nil {
		panic(err.Error())
	}
	defer f.Close()

	for i := int64(1); i <= rp.raftConsensus.commitIndex; i++ {
		if rp.raftConsensus.log[i].decided == false {
			panic("should not happen")
		}
		for j := 0; j < len(rp.raftConsensus.log[i].commands.Requests); j++ {
			for k := 0; k < len(rp.raftConsensus.log[i].commands.Requests[j].Requests); k++ {
				_, _ = f.WriteString(strconv.Itoa(int(i)) + "-" + strconv.Itoa(int(j)) + "-" + strconv.Itoa(int(k)) + ":" + rp.raftConsensus.log[i].commands.Requests[j].Requests[k].Command + "\n")
			}
		}
	}
}
