package src

import (
	"async-consensus/common"
	"async-consensus/proto"
	"fmt"
	"strconv"
	"time"
)

/*
	Upon a view change / upon bootstrap send a prepare message for all instances from last decided index +1 to len(log)
*/

func (rp *Replica) sendPrepare() {

	// replica ids start from 1 and goes to infinity
	replicaId := -1

	if replicaId == -1 {
		panic("replica id not found")
	}

	if ((rp.paxosConsensus.view+1)%int32(rp.numReplicas))+1 == rp.name {

		rp.createInstanceIfMissing(int(rp.paxosConsensus.lastCommittedLogIndex + 1))

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
			rp.debug("Sent prepare to "+strconv.Itoa(int(name)), 0)
		}
	} else {
		rp.paxosConsensus.state = "A" // become an acceptor
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
				Value:  rp.paxosConsensus.replicatedLog[i].acceptedValue,
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
			rp.debug("Sent promise to "+strconv.Itoa(int(message.Sender)), 1)

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
			// we have majority promise messages for the same view
			// update the highest accepted ballot and the values
			for i := 0; i < len(rp.paxosConsensus.promiseResponses[message.View]); i++ {
				lastAcceptedEntries := rp.paxosConsensus.promiseResponses[message.View][i].PromiseReply
				for j := 0; j < len(lastAcceptedEntries); j++ {
					instanceNumber := lastAcceptedEntries[j].Number
					rp.createInstanceIfMissing(int(instanceNumber))
					if lastAcceptedEntries[j].Ballot > rp.paxosConsensus.replicatedLog[instanceNumber].highestSeenAcceptedBallot {
						rp.paxosConsensus.replicatedLog[instanceNumber].highestSeenAcceptedBallot = lastAcceptedEntries[j].Ballot
						rp.paxosConsensus.replicatedLog[instanceNumber].highestSeenAcceptedValue = lastAcceptedEntries[j].Value
					}
				}
			}
			rp.paxosConsensus.state = "L"
			rp.debug("Became the leader in "+strconv.Itoa(int(rp.paxosConsensus.view))+" at time "+fmt.Sprintf("%v", time.Now().Sub(rp.paxosConsensus.startTime)), 4)
			rp.paxosConsensus.currentLeader = rp.name
		}
	}

}

/*
	Leader invokes this function to replicate a new instance for lastProposedLogIndex +1
*/

func (rp *Replica) sendPropose(requests []*proto.ClientBatch) {
	// requests can be empty
	if rp.paxosConsensus.state == "L" &&
		rp.paxosConsensus.lastPreparedBallot >= rp.paxosConsensus.lastPromisedBallot &&
		(rp.paxosConsensus.lastProposedLogIndex-rp.paxosConsensus.lastCommittedLogIndex) < int32(rp.paxosConsensus.pipeLineLength) {
		rp.paxosConsensus.lastProposedLogIndex++
		rp.createInstanceIfMissing(int(rp.paxosConsensus.lastProposedLogIndex))

		proposeValue := &proto.ReplicaBatch{
			UniqueId: "",
			Requests: requests,
			Sender:   int64(rp.name),
		}
		if rp.paxosConsensus.replicatedLog[rp.paxosConsensus.lastProposedLogIndex].highestSeenAcceptedBallot != -1 {
			proposeValue = rp.paxosConsensus.replicatedLog[rp.paxosConsensus.lastProposedLogIndex].highestSeenAcceptedValue
			rp.incomingRequests = append(rp.incomingRequests, requests...)
		}

		// set the proposed ballot for this instance
		rp.paxosConsensus.replicatedLog[rp.paxosConsensus.lastProposedLogIndex].proposedBallot = rp.paxosConsensus.lastPreparedBallot
		rp.paxosConsensus.replicatedLog[rp.paxosConsensus.lastProposedLogIndex].proposeResponses = 0
		rp.paxosConsensus.replicatedLog[rp.paxosConsensus.lastProposedLogIndex].proposedValue = proposeValue

		decided_values := make([]*proto.PaxosConsensusInstance, 0)

		for i := 0; i < len(rp.paxosConsensus.decidedIndexes); i++ {
			decided_values = append(decided_values, &proto.PaxosConsensusInstance{
				Number: int32(rp.paxosConsensus.decidedIndexes[i]),
				Value:  rp.paxosConsensus.replicatedLog[rp.paxosConsensus.decidedIndexes[i]].decisions,
			})
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
			rp.debug("Sent propose to "+strconv.Itoa(int(name)), 1)
		}
	} else {
		rp.incomingRequests = append(rp.incomingRequests, requests...)
	}
}

/*
	Handler for propose message, If the propose ballot number is greater than or equal to the promised ballot number, set the accepted ballot and accepted values, and send
	an accept message, also record the decided message for the previous instance
*/

func (rp *Replica) handlePropose(message *proto.PaxosConsensus) {
	rp.createInstanceIfMissing(int(message.InstanceNumber))

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
		rp.paxosConsensus.replicatedLog[message.InstanceNumber].acceptedValue = message.ProposeValue

		for i := 0; i < len(message.DecidedValues); i++ {
			rp.paxosConsensus.replicatedLog[message.DecidedValues[i].Number].decided = true
			rp.paxosConsensus.replicatedLog[message.DecidedValues[i].Number].decisions = message.DecidedValues[i].Value
		}
		rp.updatePaxosSMR()

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
		rp.debug("Sent accept message to "+strconv.Itoa(int(message.Sender)), 1)

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

	if message.View == rp.paxosConsensus.view && message.Ballot == rp.paxosConsensus.replicatedLog[message.InstanceNumber].proposedBallot && rp.paxosConsensus.state == "L" {

		// add the accept to the instance
		rp.paxosConsensus.replicatedLog[message.InstanceNumber].proposeResponses++

		// if there are n-f accept messages
		if rp.paxosConsensus.replicatedLog[message.InstanceNumber].proposeResponses == rp.numReplicas/2+1 && rp.paxosConsensus.replicatedLog[message.InstanceNumber].decided == false {
			rp.paxosConsensus.replicatedLog[message.InstanceNumber].decided = true
			rp.paxosConsensus.replicatedLog[message.InstanceNumber].decisions = rp.paxosConsensus.replicatedLog[message.InstanceNumber].proposedValue

			rp.updatePaxosSMR()
			rp.debug("Decided upon receiving n-f accept message for instance "+strconv.Itoa(int(message.InstanceNumber)), 0)
		}
	}
}

/*
	handler for internal timeout messages, send a prepare message
*/

func (rp *Replica) handlePaxosInternalTimeout(message *proto.PaxosConsensus) {
	rp.debug("Received a timeout for view "+strconv.Itoa(int(message.View))+" while my view is "+strconv.Itoa(int(rp.paxosConsensus.view))+" at time "+fmt.Sprintf("%v", time.Now().Sub(rp.paxosConsensus.startTime)), 0)
	// check if the view timeout is still valid
	if rp.paxosConsensus.view == message.View {
		rp.debug("Accepted a timeout for view "+strconv.Itoa(int(message.View))+" at time "+fmt.Sprintf("%v", time.Now().Sub(rp.paxosConsensus.startTime)), 0)
		rp.sendPrepare()
	}
}
