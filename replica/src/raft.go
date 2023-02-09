package src

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"
)

func (in *Instance) mustEmbedUnimplementedConsensusServer() {
	panic("implement me")
}

type Instance struct {
	nodeId     string
	debugOn    bool
	debugLevel int
	id         int32
	server     *grpc.Server // gRPC server
	listener   net.Listener // socket for gRPC connections
	address    string       // address to listen for gRPC connections
	peers      []peer
	numNodes   int64
	timeout    time.Duration

	log          []entry
	centralMutex sync.Mutex

	commitIndex int64

	nextFreeChoice int64

	votedFor    string
	currentTerm int64

	leaderTimeOut       int64 //constant
	halfLeaderTimeOut   int64 //constant
	leaderTimeoutMethod int

	logFilePath string

	lastSeenTimeLeader time.Time

	startedTimer bool

	state string

	serviceTime int64

	responseSize   int64
	responseString string

	batchSize   int64
	requestsIn  chan request
	requestsOut chan bool

	startedProposer bool
	batchTime       int64
}

type request struct {
	id    string
	value string
}

type entry struct {
	id       int64
	term     int64
	commands []request

	decided   bool
	decisions []request // can be ommited
}

type peer struct {
	name   string
	client ConsensusClient
}

/*
	if turned on, prints the message to console
*/

func (in *Instance) debug(message string, level int) {
	if in.debugOn && level >= in.debugLevel {
		fmt.Printf("%s\n", message)
	}
}

// start listening to gRPC connection

func (in *Instance) NetworkInit() {
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

	in.debug("started listening to grpc  ", 1)
}

func (in *Instance) createNChoices(number int64) {

	for i := 0; int64(i) < number; i++ {

		in.log = append(in.log, entry{
			id:      in.nextFreeChoice,
			term:    in.currentTerm,
			decided: false,
		})

		in.nextFreeChoice++
	}
}

func (in *Instance) compareLog(lastLogIndex int64, lastLogTerm int64) bool {

	myLastLogIndex := int64(len(in.log) - 1)
	myLastLogTerm := in.log[myLastLogIndex].term

	if myLastLogTerm > lastLogTerm {
		return false
	} else if myLastLogTerm < lastLogTerm {
		return true
	} else {
		if myLastLogIndex > lastLogIndex {
			return false
		} else {
			return true
		}
	}
}

func (in *Instance) RequestVote(ctx context.Context, req *pb.LeaderRequest) (*pb.LeaderResponse, error) {

	in.centralMutex.Lock()
	defer in.centralMutex.Unlock()

	var leaderResponse pb.LeaderResponse

	in.lastSeenTimeLeader = time.Now()

	currentTerm := in.currentTerm

	if currentTerm >= req.Term {
		leaderResponse.Term = currentTerm
		leaderResponse.VoteGranted = false
	} else {
		result := in.compareLog(req.LastLogIndex, req.LastLogTerm)
		if result {
			in.currentTerm = req.Term
			in.votedFor = req.CandidateId
			in.state = "FOLLOWER"
			leaderResponse.Term = req.Term
			leaderResponse.VoteGranted = true

		} else {
			leaderResponse.Term = currentTerm
			leaderResponse.VoteGranted = false
		}
	}

	return &leaderResponse, nil
}

func (in *Instance) identical(values []request, requests []request) bool {
	if len(requests) != len(values) {
		return false
	} else {
		for i := 0; i < len(requests); i++ {
			if requests[i].id != values[i].id {
				return false
			}
		}
		return true
	}

}

func (in *Instance) getRequestArray(protocArray []*pb.AppendRequestLogEntry) []request {
	var returnArray []request
	for i := 0; i < len(protocArray); i++ {
		returnArray = append(returnArray, request{
			id:    protocArray[i].RequestIdentifier,
			value: protocArray[i].RequestValue,
		})
	}

	return returnArray

}

func (in *Instance) AppendEntries(ctx context.Context, req *pb.AppendRequest) (*pb.AppendResponse, error) {

	in.centralMutex.Lock()
	defer in.centralMutex.Unlock()

	if in.startedTimer == false || in.startedProposer == false {
		fmt.Print("First send two status requests to start the timers\n")
		os.Exit(-1)
	}

	in.lastSeenTimeLeader = time.Now()

	var appendEntryResponse pb.AppendResponse

	currentTerm := in.currentTerm

	if req.Term < currentTerm {

		appendEntryResponse.Term = currentTerm
		appendEntryResponse.LastIndex = -1
		appendEntryResponse.Success = false
		return &appendEntryResponse, nil
	}

	lenLog := int64(len(in.log))

	if req.PrevLogIndex+1 > lenLog {

		appendEntryResponse.Term = currentTerm
		appendEntryResponse.Success = false
		appendEntryResponse.LastIndex = lenLog - 1
		return &appendEntryResponse, nil
	} else {
		// PrevLogIndex exists
		prevLogTerm := in.log[req.PrevLogIndex].term
		prevLogValues := in.log[req.PrevLogIndex].commands

		if prevLogTerm != req.PrevLogTerm || !in.identical(prevLogValues, in.getRequestArray(req.PrevLogValues)) {

			appendEntryResponse.Term = currentTerm
			appendEntryResponse.Success = false
			appendEntryResponse.LastIndex = -1
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
				in.state = "FOLLOWER"

			} else if req.Term == currentTerm {
				in.state = "FOLLOWER"
			}

			for i := int64(0); i < int64(len(req.Entries)); i++ {

				in.log[req.PrevLogIndex+1+i].term = req.Entries[i].Term
				in.log[req.PrevLogIndex+1+i].commands = in.getRequestArray(req.Entries[i].Values)
			}

			in.updateLastDecidedEntry(req.LeaderCommit)

			appendEntryResponse.Term = req.Term
			appendEntryResponse.Success = true
			appendEntryResponse.LastIndex = -1
			return &appendEntryResponse, nil

		}
	}
}

func (in *Instance) requestVote() bool {

	in.lastSeenTimeLeader = time.Now()
	term := in.currentTerm

	if in.startedTimer == false || in.startedProposer == false {
		fmt.Print("First start the timer\n")
		os.Exit(-1)
	}

	type response struct {
		term        int64
		voteGranted bool
	}

	responses := make(chan *response, in.numNodes)
	candidateId := in.nodeId
	lastLogIndex := int64(len(in.log) - 1)
	lastLogTerm := in.log[lastLogIndex].term

	responses <- &response{
		term:        term,
		voteGranted: true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), in.timeout)

	wg := sync.WaitGroup{}
	for _, p := range in.peers {
		wg.Add(1)
		go func(p peer) {
			defer wg.Done()
			resp, err := p.client.RequestVote(ctx, &pb.LeaderRequest{
				Term:         term,
				CandidateId:  candidateId,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})

			if err != nil {
				responses <- &response{term: -1, voteGranted: false}
				return
			} else {
				responses <- &response{term: resp.Term, voteGranted: resp.VoteGranted}
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
			// cancel all in-flight proposals if we have reached a majority
			if in.isMajority(int64(yea)) || in.isMajority(int64(nay)) {
				//fmt.Printf("%v received majority promises, returning\n", in.nodeId)
				canceled = true
				break
			}
		}
	}

	return in.isMajority(int64(yea)) && !foundHigherTerm
}

func (in *Instance) getProtocArray(reqArray []request) []*pb.AppendRequestLogEntry {
	var PromiseResponseValues []*pb.AppendRequestLogEntry
	for i := 0; i < len(reqArray); i++ {

		PromiseResponseValues = append(PromiseResponseValues, &pb.AppendRequestLogEntry{
			RequestIdentifier: reqArray[i].id,
			RequestValue:      reqArray[i].value,
		})
	}
	return PromiseResponseValues
}

func (in *Instance) appendEntries(values []request) bool {

	if in.startedTimer == false || in.startedProposer == false {
		fmt.Print("First start the timer\n")
		os.Exit(-1)
	}

	in.lastSeenTimeLeader = time.Now()

	var lastIndex int64
	var term int64
	var prevLogIndex int64
	var prevLogTerm int64
	var prevLogValues []*pb.AppendRequestLogEntry
	var leaderCommit int64
	var leaderId string
	var entries []*pb.AppendRequestEntry

	if len(values) > 0 {

		in.createNChoices(1)

		lastIndex = int64(len(in.log)) - 1
		term = in.currentTerm
		in.log[lastIndex].commands = values
		in.log[lastIndex].term = term
		prevLogIndex = int64(len(in.log)) - 2
		prevLogTerm = in.log[prevLogIndex].term
		prevLogValues = in.getProtocArray(in.log[prevLogIndex].commands)
		leaderCommit = in.commitIndex
		leaderId = in.nodeId
		entries = []*pb.AppendRequestEntry{{Values: in.getProtocArray(values), Term: term}}
	} else {
		term = in.currentTerm
		prevLogIndex = int64(len(in.log)) - 1
		prevLogTerm = in.log[prevLogIndex].term
		prevLogValues = in.getProtocArray(in.log[prevLogIndex].commands)
		leaderCommit = in.commitIndex
		leaderId = in.nodeId
		entries = []*pb.AppendRequestEntry{}
	}

	type response struct {
		success bool
		term    int64
	}

	responses := make(chan *response, in.numNodes-1)

	ctx, cancel := context.WithTimeout(context.Background(), in.timeout)
	cancelled := false
	wg := sync.WaitGroup{}
	for i, p := range in.peers {
		wg.Add(1)

		// send commit requests
		go func(p peer, peerIndex int, termA int64, prevLogIndexA int64, prevLogTermA int64, prevLogValuesA []*pb.AppendRequestLogEntry, leaderCommitA int64,
			leaderIdA string, entriesA []*pb.AppendRequestEntry) {
			defer wg.Done()

			termLocal := termA
			prevLogIndexLocal := prevLogIndexA
			prevLogTermLocal := prevLogTermA
			prevLogValuesLocal := prevLogValuesA
			leaderCommitLocal := leaderCommitA
			leaderIdLocal := leaderIdA
			entriesLocal := entriesA

		retry:
			resp, err := p.client.AppendEntries(ctx, &pb.AppendRequest{
				Term:          termLocal,
				LeaderId:      leaderIdLocal,
				PrevLogIndex:  prevLogIndexLocal,
				PrevLogTerm:   prevLogTermLocal,
				PrevLogValues: prevLogValuesLocal,
				Entries:       entriesLocal,
				LeaderCommit:  leaderCommitLocal,
			})

			if err != nil {
				responses <- &response{success: false, term: -1}
				return
			} else {
				respTerm := resp.Term
				respSuccess := resp.Success

				if respSuccess {
					responses <- &response{success: true, term: respTerm}
					return
				}
				if respTerm > termLocal {
					responses <- &response{success: false, term: respTerm}
					return
				}
				if !respSuccess && respTerm <= termLocal {
					// retry

					if prevLogIndexLocal >= 1 {

						if resp.LastIndex == -1 {

							prevLogIndexLocal = prevLogIndexLocal - 1

							prevLogTermLocal = in.log[prevLogIndexLocal].term
							prevLogValuesLocal = in.getProtocArray(in.log[prevLogIndexLocal].commands)
							prevValues := in.getProtocArray(in.log[prevLogIndexLocal+1].commands)
							prevTerm := in.log[prevLogIndexLocal+1].term

							entriesLocal = append([]*pb.AppendRequestEntry{{Values: prevValues, Term: prevTerm}}, entriesLocal...)

						} else {
							for prevLogIndexLocal != resp.LastIndex {

								prevLogIndexLocal = prevLogIndexLocal - 1

								prevLogTermLocal = in.log[prevLogIndexLocal].term
								prevLogValuesLocal = in.getProtocArray(in.log[prevLogIndexLocal].commands)
								prevValues := in.getProtocArray(in.log[prevLogIndexLocal+1].commands)
								prevTerm := in.log[prevLogIndexLocal+1].term
								entriesLocal = append([]*pb.AppendRequestEntry{{Values: prevValues, Term: prevTerm}}, entriesLocal...)

							}
						}
					} else {
						responses <- &response{success: false, term: respTerm}
						return
					}
					goto retry
				}
			}

		}(p, i, term, prevLogIndex, prevLogTerm, prevLogValues, leaderCommit, leaderId, entries)
	}

	// close responses channel once all responses have been received, failed, or canceled
	go func() {
		wg.Wait()
		cancel()
		close(responses)
	}()

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
			// cancel all in-flight proposals if we have reached a majority
			if in.isMajority(int64(yea)) || in.isMajority(int64(nay)) {
				cancelled = true

				if in.isMajority(int64(nay)) || foundHigherTerm {
					in.state = "FOLLOWER"
				} else if in.isMajority(int64(yea)) && len(values) > 0 {
					// decide
					lastAddedIndex := int64(len(in.log)) - 1
					in.updateLastDecidedEntry(lastAddedIndex)
				}

				break
			}
		}
	}

	return in.isMajority(int64(yea)) && !foundHigherTerm
}

func (in *Instance) startViewTimeoutChecker() {
	fmt.Printf("Starting timeout checker\n")
	go func() {
		for {
			time.Sleep(time.Duration(in.leaderTimeOut+int64(rand.Intn(200))) * time.Millisecond)

			lastSeenTimeLeader := in.lastSeenTimeLeader

			if time.Now().Sub(lastSeenTimeLeader).Milliseconds() > in.leaderTimeOut {
				//fmt.Printf("Leader timeout!\n")
				in.centralMutex.Lock()
				in.state = "CANDIDATE"
				in.currentTerm++
				in.votedFor = in.nodeId
				leaderElected := in.requestVote()
				if leaderElected {
					//fmt.Printf("%v became the leader in %v \n\n", in.nodeId, in.currentTerm)
					in.state = "LEADER"
					in.lastSeenTimeLeader = time.Now()
				} else {
					//fmt.Printf("Leader election failed, Somebody else has become the leader\n")
					in.state = "FOLLOWER"
				}
				if in.leaderTimeoutMethod == 1 {
					in.leaderTimeOut = in.leaderTimeOut * 2
					if in.leaderTimeOut > 10000 {
						in.leaderTimeOut = 10000
					}
				}
				in.centralMutex.Unlock()

			}

		}
	}()

}

func (in *Instance) startLeaderBoosting() {
	//fmt.Printf("Starting leader boosting\n")
	go func() {
		for {

			time.Sleep(time.Duration(in.halfLeaderTimeOut) * time.Millisecond)

			lastSeenTimeLeader := in.lastSeenTimeLeader

			if time.Now().Sub(lastSeenTimeLeader).Milliseconds() > in.halfLeaderTimeOut {
				in.centralMutex.Lock()
				currentLeader := in.votedFor
				state := in.state
				if in.nodeId == currentLeader && state == "LEADER" {
					in.appendEntries([]request{})
					in.lastSeenTimeLeader = time.Now()
				}
				in.centralMutex.Unlock()
			}
		}
	}()
}

func (in *Instance) updateLastDecidedEntry(lastDecidedEntry int64) {

	commitIndex := in.commitIndex

	if lastDecidedEntry > commitIndex {
		for i := commitIndex + 1; i < lastDecidedEntry+1; i++ {

			in.log[i].decided = true
			in.log[i].decisions = in.log[i].commands
			for j := 0; j < len(in.log[i].decisions); j++ {
				in.app.Process(in.log[i].decisions[j].value)
			}
			//this should be the upcall
			in.commitIndex++
		}

	}
}
