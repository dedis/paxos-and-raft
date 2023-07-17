package src

import (
	"math/rand"
	"os"
	"paxos_raft/common"
	"paxos_raft/proto"
	"strconv"
	"time"
)

/*
	instance defines the content of a single Paxos consensus instance
*/

type PaxosInstance struct {
	proposedBallot int32
	promisedBallot int32
	acceptedBallot int32

	acceptedValue proto.ReplicaBatch
	proposedValue proto.ReplicaBatch
	decidedValue  proto.ReplicaBatch
	decided       bool

	proposeResponses int

	highestSeenAcceptedBallot int32
	highestSeenAcceptedValue  proto.ReplicaBatch
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
	nextFreeInstance      int                               // log position that needs to be created next in the replicated log
	state                 string                            // can be A (acceptor), L (leader), C (contestant)
	promiseResponses      map[int32][]*proto.PaxosConsensus // for each view the set of received promise messages
	replica               *Replica
	pipeLineLength        int
	decidedIndexes        []int // indexes of already decided slots to be sent to other replicas
	isAsync               bool
	asyncTimeout          int
}

/*
	init Paxos Consensus data structs
*/

func InitPaxosConsensus(name int32, replica *Replica, pipelineLength int, isAsync bool, asyncTimeout int) *Paxos {

	replicatedLog := make([]PaxosInstance, 0)
	// create the genesis slot
	replicatedLog = append(replicatedLog, PaxosInstance{
		proposedBallot:            -1,
		promisedBallot:            -1,
		acceptedBallot:            -1,
		acceptedValue:             proto.ReplicaBatch{},
		decidedValue:              proto.ReplicaBatch{},
		decided:                   true,
		proposeResponses:          0,
		highestSeenAcceptedBallot: -1,
		highestSeenAcceptedValue:  proto.ReplicaBatch{},
	})

	// create initial slots
	for i := 1; i < 100; i++ {
		replicatedLog = append(replicatedLog, PaxosInstance{
			proposedBallot:            -1,
			promisedBallot:            -1,
			acceptedBallot:            -1,
			acceptedValue:             proto.ReplicaBatch{},
			decidedValue:              proto.ReplicaBatch{},
			decided:                   false,
			proposeResponses:          0,
			highestSeenAcceptedBallot: -1,
			highestSeenAcceptedValue:  proto.ReplicaBatch{},
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
		nextFreeInstance:      100,
		state:                 "A",
		promiseResponses:      make(map[int32][]*proto.PaxosConsensus),
		replica:               replica,
		pipeLineLength:        pipelineLength,
		decidedIndexes:        make([]int, 0),
		isAsync:               isAsync,
		asyncTimeout:          asyncTimeout,
	}
}

// start the initial leader

func (p *Paxos) run() {
	p.startTime = time.Now()
	initLeader := int32(1)

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
			acceptedValue:             proto.ReplicaBatch{},
			decidedValue:              proto.ReplicaBatch{},
			decided:                   false,
			proposeResponses:          0,
			highestSeenAcceptedBallot: -1,
			highestSeenAcceptedValue:  proto.ReplicaBatch{},
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
		//rp.debug("Received a prepare message from "+strconv.Itoa(int(message.Sender))+
		//	" for view "+strconv.Itoa(int(message.View))+" for prepare ballot "+strconv.Itoa(int(message.Ballot))+" for initial instance "+strconv.Itoa(int(message.InstanceNumber))+" at time "+fmt.Sprintf("%v", time.Now().Sub(rp.paxosConsensus.startTime).Milliseconds()), 0)
		rp.handlePrepare(message)
	}

	if message.Type == 2 {
		//rp.debug("Received a promise message from "+strconv.Itoa(int(message.Sender))+
		//	" for view "+strconv.Itoa(int(message.View))+" for instance "+strconv.Itoa(int(message.InstanceNumber))+" for promise ballot "+strconv.Itoa(int(message.Ballot))+" at time "+fmt.Sprintf("%v", time.Now().Sub(rp.paxosConsensus.startTime).Milliseconds()), 0)
		rp.handlePromise(message)
	}

	if message.Type == 3 {
		//rp.debug("Received a propose message from "+strconv.Itoa(int(message.Sender))+
		//	" for view "+strconv.Itoa(int(message.View))+" for instance "+strconv.Itoa(int(message.InstanceNumber))+" for propose ballot "+strconv.Itoa(int(message.Ballot))+" at time "+fmt.Sprintf("%v", time.Now().Sub(rp.paxosConsensus.startTime).Milliseconds()), 0)
		rp.handlePropose(message)
	}

	if message.Type == 4 {
		//rp.debug("Received a accept message from "+strconv.Itoa(int(message.Sender))+
		//" for view "+strconv.Itoa(int(message.View))+" for instance "+strconv.Itoa(int(message.InstanceNumber))+" for accept ballot "+strconv.Itoa(int(message.Ballot))+" at time "+fmt.Sprintf("%v", time.Now().Sub(rp.paxosConsensus.startTime).Milliseconds()), 0)
		rp.handleAccept(message)
	}

	if message.Type == 5 {
		//rp.debug("Received an internal timeout message from "+strconv.Itoa(int(message.Sender))+
		//	" for view "+strconv.Itoa(int(message.View))+" for instance "+strconv.Itoa(int(message.InstanceNumber))+" at time "+fmt.Sprintf("%v", time.Now().Sub(rp.paxosConsensus.startTime).Milliseconds()), 0)
		rp.handlePaxosInternalTimeout(message)
	}
}

/*
	Sets a timer, which once timeout will send an internal notification for a prepare message after another random wait to break the ties
*/

func (rp *Replica) setPaxosViewTimer(view int32) {

	rp.paxosConsensus.viewTimer = common.NewTimerWithCancel(time.Duration(rp.viewTimeout+rand.Intn(rp.viewTimeout+int(rp.name))) * time.Microsecond)

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
		//rp.debug("Sent an internal timeout notification for view "+strconv.Itoa(int(view)), 0)

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

	for i := int32(0); i < rp.paxosConsensus.lastCommittedLogIndex; i++ {
		if rp.paxosConsensus.replicatedLog[i].decided == false {
			panic("should not happen")
		}
		for j := 0; j < len(rp.paxosConsensus.replicatedLog[i].decidedValue.Requests); j++ {
			for k := 0; k < len(rp.paxosConsensus.replicatedLog[i].decidedValue.Requests[j].Requests); k++ {
				_, _ = f.WriteString(strconv.Itoa(int(i)) + "-" + strconv.Itoa(j) + "-" + strconv.Itoa(k) + ":" + rp.paxosConsensus.replicatedLog[i].decidedValue.Requests[j].Requests[k].Command + "\n")

			}
		}
	}
}

/*
	upon a view change / upon bootstrap send a prepare message for all instances from last committed index +1 to len(log)
*/

func (rp *Replica) sendPrepare() {

	//rp.debug("sending prepare for view "+strconv.Itoa(int(rp.paxosConsensus.view)), 7)

	rp.createPaxosInstanceIfMissing(int(rp.paxosConsensus.lastCommittedLogIndex + 1))

	// reset the promise response map, because all we care is new view change messages
	rp.paxosConsensus.promiseResponses = make(map[int32][]*proto.PaxosConsensus)

	if rp.paxosConsensus.lastPromisedBallot > rp.paxosConsensus.lastPreparedBallot {
		rp.paxosConsensus.lastPreparedBallot = rp.paxosConsensus.lastPromisedBallot
	}
	rp.paxosConsensus.lastPreparedBallot = rp.paxosConsensus.lastPreparedBallot + 100*rp.name + 2

	rp.paxosConsensus.state = "C" // become a contestant
	// increase the view number
	rp.paxosConsensus.view++
	// broadcast a prepare message
	for name, _ := range rp.replicaAddrList {
		prepareMsg := proto.PaxosConsensus{
			Sender:         rp.name,
			Receiver:       name,
			Type:           1,
			InstanceNumber: rp.paxosConsensus.lastCommittedLogIndex + 1,
			Ballot:         rp.paxosConsensus.lastPreparedBallot,
			View:           rp.paxosConsensus.view,
		}

		rpcPair := common.RPCPair{
			Code: rp.messageCodes.PaxosConsensus,
			Obj:  &prepareMsg,
		}

		rp.sendMessage(name, rpcPair)
		//rp.debug("Sent prepare to "+strconv.Itoa(int(name)), 0)
	}

	// cancel the view timer
	if rp.paxosConsensus.viewTimer != nil {
		rp.paxosConsensus.viewTimer.Cancel()
		rp.paxosConsensus.viewTimer = nil
	}
	// set the view timer
	rp.setPaxosViewTimer(rp.paxosConsensus.view)

	// cancel the current leader
	rp.paxosConsensus.currentLeader = -1
}

/*
	Handler for prepare message, check if it is possible to promise for all instances from initial index to len(log)-1, if yes send a response
	if at least one instance does not agree with the prepare ballot, do not send anything
*/

func (rp *Replica) handlePrepare(message *proto.PaxosConsensus) {
	prepared := true

	// the view of prepare should be from a higher view
	if rp.paxosConsensus.view < message.View || (rp.paxosConsensus.view == message.View && message.Sender == rp.name) {

		prepareResponses := make([]*proto.PaxosConsensusInstance, 0)

		for i := message.InstanceNumber; i < int32(len(rp.paxosConsensus.replicatedLog)); i++ {

			prepareResponses = append(prepareResponses, &proto.PaxosConsensusInstance{
				Number: i,
				Ballot: rp.paxosConsensus.replicatedLog[i].acceptedBallot,
				Value:  &rp.paxosConsensus.replicatedLog[i].acceptedValue,
			})

			if rp.paxosConsensus.replicatedLog[i].promisedBallot >= message.Ballot {
				prepared = false
				break
			}
		}

		if prepared == true {

			// cancel the view timer
			if rp.paxosConsensus.viewTimer != nil {
				rp.paxosConsensus.viewTimer.Cancel()
				rp.paxosConsensus.viewTimer = nil
			}

			rp.paxosConsensus.lastPromisedBallot = message.Ballot
			if message.Sender != rp.name {
				// become follower
				rp.paxosConsensus.state = "A"
				rp.paxosConsensus.currentLeader = message.Sender
				rp.paxosConsensus.view = message.View

				//rp.debug("leader for view "+strconv.Itoa(int(rp.paxosConsensus.view))+" is "+strconv.Itoa(int(rp.paxosConsensus.currentLeader)), 7)
			}

			for i := message.InstanceNumber; i < int32(len(rp.paxosConsensus.replicatedLog)); i++ {
				rp.paxosConsensus.replicatedLog[i].promisedBallot = message.Ballot
			}

			// send a promise message to the sender
			promiseMsg := proto.PaxosConsensus{
				Sender:         rp.name,
				Receiver:       message.Sender,
				Type:           2,
				InstanceNumber: message.InstanceNumber,
				Ballot:         message.Ballot,
				View:           message.View,
				PromiseReply:   prepareResponses,
			}

			rpcPair := common.RPCPair{
				Code: rp.messageCodes.PaxosConsensus,
				Obj:  &promiseMsg,
			}

			rp.sendMessage(message.Sender, rpcPair)
			//rp.debug("Sent promise to "+strconv.Itoa(int(message.Sender)), 1)

			// set the view timer
			rp.setPaxosViewTimer(rp.paxosConsensus.view)
		}
	}

}

/*
	Handler for promise messages
*/

func (rp *Replica) handlePromise(message *proto.PaxosConsensus) {
	if message.Ballot == rp.paxosConsensus.lastPreparedBallot && message.View == rp.paxosConsensus.view && rp.paxosConsensus.state == "C" {
		// save the promise message
		_, ok := rp.paxosConsensus.promiseResponses[message.View]
		if ok {
			rp.paxosConsensus.promiseResponses[message.View] = append(rp.paxosConsensus.promiseResponses[message.View], message)
		} else {
			rp.paxosConsensus.promiseResponses[message.View] = make([]*proto.PaxosConsensus, 0)
			rp.paxosConsensus.promiseResponses[message.View] = append(rp.paxosConsensus.promiseResponses[message.View], message)
		}

		if len(rp.paxosConsensus.promiseResponses[message.View]) == rp.numReplicas/2+1 {
			// cancel the view timer
			if rp.paxosConsensus.viewTimer != nil {
				rp.paxosConsensus.viewTimer.Cancel()
				rp.paxosConsensus.viewTimer = nil
			}
			// we have majority promise messages for the same view
			// update the highest accepted ballot and the values
			for i := 0; i < len(rp.paxosConsensus.promiseResponses[message.View]); i++ {
				lastAcceptedEntries := rp.paxosConsensus.promiseResponses[message.View][i].PromiseReply
				for j := 0; j < len(lastAcceptedEntries); j++ {
					instanceNumber := lastAcceptedEntries[j].Number
					rp.createPaxosInstanceIfMissing(int(instanceNumber))
					if lastAcceptedEntries[j].Ballot > rp.paxosConsensus.replicatedLog[instanceNumber].highestSeenAcceptedBallot {
						rp.paxosConsensus.replicatedLog[instanceNumber].highestSeenAcceptedBallot = lastAcceptedEntries[j].Ballot
						rp.paxosConsensus.replicatedLog[instanceNumber].highestSeenAcceptedValue = *lastAcceptedEntries[j].Value
					}
				}
			}
			rp.paxosConsensus.state = "L"
			//rp.debug("Became the leader in view "+strconv.Itoa(int(rp.paxosConsensus.view)), 7)
			rp.paxosConsensus.currentLeader = rp.name
		}
	}

}

/*
	leader invokes this function to replicate a new instance for lastProposedLogIndex +1
*/

func (rp *Replica) sendPropose(requests []*proto.ClientBatch) { // requests can be empty

	if rp.paxosConsensus.lastProposedLogIndex < rp.paxosConsensus.lastCommittedLogIndex {
		rp.paxosConsensus.lastProposedLogIndex = rp.paxosConsensus.lastCommittedLogIndex
	}

	if rp.paxosConsensus.state == "L" &&
		rp.paxosConsensus.lastPreparedBallot >= rp.paxosConsensus.lastPromisedBallot &&
		(rp.paxosConsensus.lastProposedLogIndex-rp.paxosConsensus.lastCommittedLogIndex) < int32(rp.paxosConsensus.pipeLineLength) {

		rp.paxosConsensus.lastProposedLogIndex++
		rp.createPaxosInstanceIfMissing(int(rp.paxosConsensus.lastProposedLogIndex))

		for rp.paxosConsensus.replicatedLog[rp.paxosConsensus.lastProposedLogIndex].decided {
			rp.paxosConsensus.lastProposedLogIndex++
			rp.createPaxosInstanceIfMissing(int(rp.paxosConsensus.lastProposedLogIndex))
		}

		proposeValue := &proto.ReplicaBatch{
			UniqueId: "",
			Requests: requests,
			Sender:   int64(rp.name),
		}
		if rp.paxosConsensus.replicatedLog[rp.paxosConsensus.lastProposedLogIndex].highestSeenAcceptedBallot != -1 {
			proposeValue = &rp.paxosConsensus.replicatedLog[rp.paxosConsensus.lastProposedLogIndex].highestSeenAcceptedValue
			rp.incomingRequests = append(rp.incomingRequests, requests...)
		}

		// set the proposed ballot for this instance
		rp.paxosConsensus.replicatedLog[rp.paxosConsensus.lastProposedLogIndex].proposedBallot = rp.paxosConsensus.lastPreparedBallot
		rp.paxosConsensus.replicatedLog[rp.paxosConsensus.lastProposedLogIndex].proposeResponses = 0
		rp.paxosConsensus.replicatedLog[rp.paxosConsensus.lastProposedLogIndex].proposedValue = *proposeValue

		decided_values := make([]*proto.PaxosConsensusInstance, 0)

		for i := 0; i < len(rp.paxosConsensus.decidedIndexes); i++ {
			decided_values = append(decided_values, &proto.PaxosConsensusInstance{
				Number: int32(rp.paxosConsensus.decidedIndexes[i]),
				Value:  &rp.paxosConsensus.replicatedLog[rp.paxosConsensus.decidedIndexes[i]].decidedValue,
			})
		}
		// reset decided indexes
		rp.paxosConsensus.decidedIndexes = make([]int, 0)

		if rp.isAsynchronous {

			epoch := time.Now().Sub(rp.paxosConsensus.startTime).Milliseconds() / int64(rp.timeEpochSize)

			if rp.amIAttacked(int(epoch)) {
				time.Sleep(time.Duration(rp.asyncSimulationTimeout) * time.Millisecond)
			}
		}

		// send a propose message
		for name, _ := range rp.replicaAddrList {
			proposeMsg := proto.PaxosConsensus{
				Sender:         rp.name,
				Receiver:       name,
				Type:           3,
				InstanceNumber: rp.paxosConsensus.lastProposedLogIndex,
				Ballot:         rp.paxosConsensus.lastPreparedBallot,
				View:           rp.paxosConsensus.view,
				ProposeValue:   proposeValue,
				DecidedValues:  decided_values,
			}

			rpcPair := common.RPCPair{
				Code: rp.messageCodes.PaxosConsensus,
				Obj:  &proposeMsg,
			}

			rp.sendMessage(name, rpcPair)
			//rp.debug("Sent propose to "+strconv.Itoa(int(name)), 1)
		}
		//rp.debug("Sent proposal for index "+strconv.Itoa(int(rp.paxosConsensus.lastProposedLogIndex))+" in view "+strconv.Itoa(int(rp.paxosConsensus.view)), 7)
		if rp.paxosConsensus.viewTimer != nil {
			rp.paxosConsensus.viewTimer.Cancel()
			rp.paxosConsensus.viewTimer = nil
		}
		rp.setPaxosViewTimer(rp.paxosConsensus.view)
	} else if rp.paxosConsensus.state == "L" && rp.paxosConsensus.lastPreparedBallot >= rp.paxosConsensus.lastPromisedBallot {
		rp.incomingRequests = append(rp.incomingRequests, requests...)
		//rp.debug("saving for later proposal due to full pipeline while I am the leader "+" in view "+strconv.Itoa(int(rp.paxosConsensus.view)), 0)
	} else {
		//rp.debug("dropping requests because i am not the leader "+" in view "+strconv.Itoa(int(rp.paxosConsensus.view)), 0)
	}
}

/*
	handler for propose message, If the propose ballot number is greater than or equal to the promised ballot number,
	set the accepted ballot and accepted values, and send
	an accept message, also record the decided message for the previous instance
*/

func (rp *Replica) handlePropose(message *proto.PaxosConsensus) {

	for i := 0; i < len(message.DecidedValues); i++ {
		rp.createPaxosInstanceIfMissing(int(message.DecidedValues[i].Number))
		if !rp.paxosConsensus.replicatedLog[message.DecidedValues[i].Number].decided {
			rp.paxosConsensus.replicatedLog[message.DecidedValues[i].Number].decided = true
			rp.paxosConsensus.replicatedLog[message.DecidedValues[i].Number].decidedValue = *message.DecidedValues[i].Value
			//rp.debug("decided index "+fmt.Sprintf("%v", message.DecidedValues[i].Number), 7)
		}
	}

	rp.createPaxosInstanceIfMissing(int(message.InstanceNumber))

	// if the message is from a future view, become an acceptor and set the new leader
	if message.View > rp.paxosConsensus.view {
		rp.paxosConsensus.view = message.View
		rp.paxosConsensus.currentLeader = message.Sender
		rp.paxosConsensus.state = "A"
	}

	// if this message is for the current view
	if message.Sender == rp.paxosConsensus.currentLeader && message.View == rp.paxosConsensus.view && message.Ballot >= rp.paxosConsensus.replicatedLog[message.InstanceNumber].promisedBallot {

		// cancel the view timer
		if rp.paxosConsensus.viewTimer != nil {
			rp.paxosConsensus.viewTimer.Cancel()
			rp.paxosConsensus.viewTimer = nil
		}

		rp.paxosConsensus.replicatedLog[message.InstanceNumber].acceptedBallot = message.Ballot
		rp.paxosConsensus.replicatedLog[message.InstanceNumber].acceptedValue = *message.ProposeValue

		// send an accept message to the sender
		acceptMsg := proto.PaxosConsensus{
			Sender:         rp.name,
			Receiver:       message.Sender,
			Type:           4,
			InstanceNumber: message.InstanceNumber,
			Ballot:         message.Ballot,
			View:           message.View,
		}

		rpcPair := common.RPCPair{
			Code: rp.messageCodes.PaxosConsensus,
			Obj:  &acceptMsg,
		}

		rp.sendMessage(message.Sender, rpcPair)
		//rp.debug("Sent accept message to "+strconv.Itoa(int(message.Sender)), 1)

		rp.updatePaxosSMR()

		// set the view timer
		rp.setPaxosViewTimer(rp.paxosConsensus.view)
	}
}

/*
	handler for accept messages. Upon collecting n-f accept messages, mark the instance as decided, call SMR and
*/

func (rp *Replica) handleAccept(message *proto.PaxosConsensus) {
	if int32(len(rp.paxosConsensus.replicatedLog)) < message.InstanceNumber+1 {
		panic("Received accept without having an instance")
	}

	if message.View <= rp.paxosConsensus.view && message.Ballot == rp.paxosConsensus.replicatedLog[message.InstanceNumber].proposedBallot && rp.paxosConsensus.state == "L" {

		// add the accept to proposeResponses
		rp.paxosConsensus.replicatedLog[message.InstanceNumber].proposeResponses++

		// if there are n-f accept messages
		if rp.paxosConsensus.replicatedLog[message.InstanceNumber].proposeResponses == rp.numReplicas/2+1 && !rp.paxosConsensus.replicatedLog[message.InstanceNumber].decided {

			if rp.paxosConsensus.viewTimer != nil {
				rp.paxosConsensus.viewTimer.Cancel()
				rp.paxosConsensus.viewTimer = nil
			}

			rp.paxosConsensus.replicatedLog[message.InstanceNumber].decided = true
			rp.paxosConsensus.replicatedLog[message.InstanceNumber].decidedValue = rp.paxosConsensus.replicatedLog[message.InstanceNumber].proposedValue
			rp.paxosConsensus.replicatedLog[message.InstanceNumber].proposedValue = proto.ReplicaBatch{}
			//rp.debug("Decided upon receiving n-f accept message for instance "+strconv.Itoa(int(message.InstanceNumber)), 7)
			rp.paxosConsensus.decidedIndexes = append(rp.paxosConsensus.decidedIndexes, int(message.InstanceNumber))
			rp.updatePaxosSMR()
			rp.setPaxosViewTimer(rp.paxosConsensus.view)
		}
	}
}

/*
	handler for internal timeout messages, send a prepare message
*/

func (rp *Replica) handlePaxosInternalTimeout(message *proto.PaxosConsensus) {
	//rp.debug("Received a timeout for view "+strconv.Itoa(int(message.View))+" while my view is "+strconv.Itoa(int(rp.paxosConsensus.view))+" at time "+fmt.Sprintf("%v", time.Now().Sub(rp.paxosConsensus.startTime).Milliseconds()), 5)
	// check if the view timeout is still valid
	if rp.paxosConsensus.view == message.View && rp.paxosConsensus.state != "L" {
		//rp.debug("Accepted a timeout for view "+strconv.Itoa(int(message.View)), 0)
		rp.sendPrepare()
	}
}

/*
	update SMR logic
*/

func (rp *Replica) updatePaxosSMR() {

	for i := rp.paxosConsensus.lastCommittedLogIndex + 1; i < int32(len(rp.paxosConsensus.replicatedLog)); i++ {

		if rp.paxosConsensus.replicatedLog[i].decided == true {
			var cllientResponses []*proto.ClientBatch
			cllientResponses = rp.updateApplicationLogic(rp.paxosConsensus.replicatedLog[i].decidedValue.Requests)
			if rp.paxosConsensus.replicatedLog[i].decidedValue.Sender == int64(rp.name) {
				rp.sendClientResponses(cllientResponses)
			}
			//rp.debug("Committed paxos consensus instance "+"."+strconv.Itoa(int(i)), 7)
			rp.paxosConsensus.lastCommittedLogIndex = i
		} else {
			break
		}

	}
}
