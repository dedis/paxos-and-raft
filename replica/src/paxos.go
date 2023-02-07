package src

import (
	"async-consensus/common"
	"async-consensus/proto"
	"fmt"
	"os"
	"strconv"
	"time"
)

/*
	an instance defines the content of a single Paxos consensus instance
*/

type PaxosInstance struct {
	proposedBallot int32
	promisedBallot int32
	acceptedBallot int32

	acceptedValue *proto.ReplicaBatch
	proposedValue *proto.ReplicaBatch
	decided       bool
	decisions     *proto.ReplicaBatch

	proposeResponses int

	highestSeenAcceptedBallot int32
	highestSeenAcceptedValue  *proto.ReplicaBatch
}

/*
	Paxos struct defines the replica wide consensus variables
*/

type Paxos struct {
	name          int32
	view          int32 // current view number
	currentLeader int32 // current leader

	lastPromisedBallot    int32 // last promised ballot number, for each next instance created this should be used as the promised ballot
	lastPreparedBallot    int32 // last prepared ballot as the proposer, all future instances should propose for this ballot number
	lastProposedLogIndex  int32
	lastCommittedLogIndex int32           // the last log position that is committed
	replicatedLog         []PaxosInstance // the replicated log of commands
	viewTimer             *common.TimerWithCancel
	startTime             time.Time                         // time when the consensus was started
	lastCommittedTime     time.Time                         // time when the last consensus instance was committed
	lastProposedTime      time.Time                         // time when last proposed
	nextFreeInstance      int                               // log position that needs to be created next in the replicated log
	state                 string                            // can be A (acceptor), L (leader), C (contestant)
	promiseResponses      map[int32][]*proto.PaxosConsensus // for each view the set of received promise messages
	replica               *Replica
	pipeLineLength        int
	decidedIndexes        []int // indexes of already decided slots to be sent to other replicas
}

/*
	init Paxos Consensus data structs
*/

func InitPaxosConsensus(numReplicas int, name int32, replica *Replica, pipelineLength int) *Paxos {

	replicatedLog := make([]PaxosInstance, 0)
	// create the genesis slot

	replicatedLog = append(replicatedLog, PaxosInstance{
		proposedBallot:            -1,
		promisedBallot:            -1,
		acceptedBallot:            -1,
		acceptedValue:             nil,
		decided:                   true,
		decisions:                 nil,
		proposeResponses:          0,
		highestSeenAcceptedBallot: -1,
		highestSeenAcceptedValue:  nil,
	})

	// create initial slots
	for i := 1; i < 100; i++ {
		replicatedLog = append(replicatedLog, PaxosInstance{
			proposedBallot:            -1,
			promisedBallot:            -1,
			acceptedBallot:            -1,
			acceptedValue:             nil,
			decided:                   false,
			decisions:                 nil,
			proposeResponses:          0,
			highestSeenAcceptedBallot: -1,
			highestSeenAcceptedValue:  nil,
		})
	}

	return &Paxos{
		name:                  name,
		view:                  0,
		currentLeader:         -1,
		lastPromisedBallot:    -1,
		lastPreparedBallot:    -1,
		lastProposedLogIndex:  0,
		lastCommittedLogIndex: 0,
		replicatedLog:         replicatedLog,
		viewTimer:             nil,
		startTime:             time.Time{},
		lastCommittedTime:     time.Time{},
		lastProposedTime:      time.Time{},
		nextFreeInstance:      100,
		state:                 "A",
		promiseResponses:      make(map[int32][]*proto.PaxosConsensus),
		replica:               replica,
		pipeLineLength:        pipelineLength,
		decidedIndexes:        make([]int, 0),
	}
}

// start the initial leader

func (p *Paxos) run() {
	p.startTime = time.Now()
	p.lastCommittedTime = time.Now()
	p.lastProposedTime = time.Now()
	initLeader := int32(2)

	if p.name == initLeader {
		p.replica.sendPrepare()
	}

}

/*
	append N new instances to the log
*/

func (rp *Replica) createNPaxosInstances(number int) {

	for i := 0; i < number; i++ {

		rp.paxosConsensus.replicatedLog = append(rp.paxosConsensus.replicatedLog, PaxosInstance{
			proposedBallot:            -1,
			promisedBallot:            rp.paxosConsensus.lastPromisedBallot,
			acceptedBallot:            -1,
			acceptedValue:             nil,
			decided:                   false,
			decisions:                 nil,
			proposeResponses:          0,
			highestSeenAcceptedBallot: -1,
			highestSeenAcceptedValue:  nil,
		})

		rp.paxosConsensus.nextFreeInstance++
	}
}

/*
	check if the instance number instance is already there, if not create 10 new instances
*/

func (rp *Replica) createPaxosInstanceIfMissing(instanceNum int) {

	numMissingEntries := instanceNum - rp.paxosConsensus.nextFreeInstance + 1

	if numMissingEntries > 0 {
		rp.createNPaxosInstances(numMissingEntries)
	}
}

/*
	handler for generic Paxos messages
*/

func (rp *Replica) handlePaxosConsensus(message *proto.PaxosConsensus) {

	if message.Type == 1 {
		rp.debug("Received a prepare message from "+strconv.Itoa(int(message.Sender))+
			" for view "+strconv.Itoa(int(message.View))+" for prepare ballot "+strconv.Itoa(int(message.Ballot))+" for initial instance "+strconv.Itoa(int(message.InstanceNumber))+" at time "+fmt.Sprintf("%v", time.Now().Sub(rp.paxosConsensus.startTime).Milliseconds()), 0)
		rp.handlePrepare(message)
	}

	if message.Type == 2 {
		rp.debug("Received a promise message from "+strconv.Itoa(int(message.Sender))+
			" for view "+strconv.Itoa(int(message.View))+" for instance "+strconv.Itoa(int(message.InstanceNumber))+" for promise ballot "+strconv.Itoa(int(message.Ballot))+" at time "+fmt.Sprintf("%v", time.Now().Sub(rp.paxosConsensus.startTime).Milliseconds()), 0)
		rp.handlePromise(message)
	}

	if message.Type == 3 {
		rp.debug("Received a propose message from "+strconv.Itoa(int(message.Sender))+
			" for view "+strconv.Itoa(int(message.View))+" for instance "+strconv.Itoa(int(message.InstanceNumber))+" for propose ballot "+strconv.Itoa(int(message.Ballot))+" at time "+fmt.Sprintf("%v", time.Now().Sub(rp.paxosConsensus.startTime).Milliseconds()), 0)
		rp.handlePropose(message)
	}

	if message.Type == 4 {
		rp.debug("Received a accept message from "+strconv.Itoa(int(message.Sender))+
			" for view "+strconv.Itoa(int(message.View))+" for instance "+strconv.Itoa(int(message.InstanceNumber))+" for accept ballot "+strconv.Itoa(int(message.Ballot))+" at time "+fmt.Sprintf("%v", time.Now().Sub(rp.paxosConsensus.startTime).Milliseconds()), 0)
		rp.handleAccept(message)
	}

	if message.Type == 5 {
		rp.debug("Received an internal timeout message from "+strconv.Itoa(int(message.Sender))+
			" for view "+strconv.Itoa(int(message.View))+" for instance "+strconv.Itoa(int(message.InstanceNumber))+" at time "+fmt.Sprintf("%v", time.Now().Sub(rp.paxosConsensus.startTime).Milliseconds()), 0)
		rp.handlePaxosInternalTimeout(message)
	}
}

/*
	Sets a timer, which once timeout will send an internal notification for a prepare message after another random wait to break the ties
*/

func (rp *Replica) setPaxosViewTimer(view int32) {

	rp.paxosConsensus.viewTimer = common.NewTimerWithCancel(time.Duration(rp.viewTimeout) * time.Microsecond)

	rp.paxosConsensus.viewTimer.SetTimeoutFuntion(func() {

		// this function runs in a separate thread, hence we do not send prepare message in this function, instead send a timeout-internal signal
		internalTimeoutNotification := proto.PaxosConsensus{
			Sender:   rp.name,
			Receiver: rp.name,
			Type:     5,
			View:     view,
		}

		rpcPair := common.RPCPair{
			Code: rp.messageCodes.PaxosConsensus,
			Obj:  &internalTimeoutNotification,
		}
		rp.sendMessage(rp.name, rpcPair)
		rp.debug("Sent an internal timeout notification for view "+strconv.Itoa(int(view))+" at time "+fmt.Sprintf("%v", time.Now().Sub(rp.paxosConsensus.startTime).Milliseconds()), 0)

	})
	rp.paxosConsensus.viewTimer.Start()
}

/*
	print the replicated log to check for log consistency
*/

func (rp *Replica) printPaxosLogConsensus() {
	f, err := os.Create(rp.logFilePath + strconv.Itoa(int(rp.name)) + "-consensus.txt")
	if err != nil {
		panic(err.Error())
	}
	defer f.Close()

	for i := int32(1); i <= rp.paxosConsensus.lastCommittedLogIndex; i++ {
		if rp.paxosConsensus.replicatedLog[i].decided == false {
			panic("should not happen")
		}
		for j := 0; j < len(rp.paxosConsensus.replicatedLog[i].decisions.Requests); j++ {
			for k := 0; k < len(rp.paxosConsensus.replicatedLog[i].decisions.Requests[j].Requests); k++ {
				_, _ = f.WriteString(strconv.Itoa(int(i)) + "-" + strconv.Itoa(int(j)) + "-" + strconv.Itoa(int(k)) + "-" + ":" + rp.paxosConsensus.replicatedLog[i].decisions.Requests[j].Requests[k].Command + "\n")

			}
		}
	}
}
