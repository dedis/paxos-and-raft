package src

import (
	"async-consensus/common"
	"async-consensus/proto"
	"fmt"
	"strconv"
	"time"
)

// the leader appending new slots and propose

func (rp *Replica) appendEntries(proposals []*proto.ClientBatch) {
	if rp.raftConsensus.state == "L" && rp.raftConsensus.votedFor[int(rp.raftConsensus.currentTerm)] == rp.name &&
		(len(rp.raftConsensus.log)-1-int(rp.raftConsensus.commitIndex)) < int(rp.raftConsensus.pipelineLength) {
		rp.createRaftInstanceIfMissing(len(rp.raftConsensus.log))
		rp.raftConsensus.batchCounter++

		proposeValue := &proto.ReplicaBatch{
			UniqueId: fmt.Sprintf("%v:%v", rp.name, rp.raftConsensus.batchCounter),
			Requests: proposals,
			Sender:   int64(rp.name),
		}

		rp.raftConsensus.log[len(rp.raftConsensus.log)-1] = raftInstance{
			term:        rp.raftConsensus.currentTerm,
			commands:    proposeValue,
			decided:     rp.raftConsensus.log[len(rp.raftConsensus.log)-1].decided,
			decisions:   rp.raftConsensus.log[len(rp.raftConsensus.log)-1].decisions,
			numSucccess: 1,
		}

		prevLogIndex := int64(len(rp.raftConsensus.log)) - 2
		prevLogTerm := rp.raftConsensus.log[prevLogIndex].term
		prevLogValues := rp.raftConsensus.log[prevLogIndex].commands.UniqueId
		leaderCommit := rp.raftConsensus.commitIndex
		leaderId := rp.name
		entries := []*proto.RaftConsensusEntry{{Value: proposeValue, Term: rp.raftConsensus.currentTerm}}

		// send a append request message
		for name, _ := range rp.replicaAddrList {
			if name == rp.name {
				continue
			}
			appendMsg := proto.RaftConsensus{
				Sender:        leaderId,
				Receiver:      name,
				Type:          1,
				Note:          "",
				Term:          rp.raftConsensus.currentTerm,
				PrevLogIndex:  prevLogIndex,
				PrevLogTerm:   prevLogTerm,
				PrevLogValues: prevLogValues,
				Entries:       entries,
				LeaderCommit:  leaderCommit,
			}

			rpcPair := common.RPCPair{
				Code: rp.messageCodes.RaftConsensus,
				Obj:  &appendMsg,
			}

			rp.sendMessage(name, rpcPair)
			rp.debug("Sent append request to "+strconv.Itoa(int(name)), 1)
		}
	} else if rp.raftConsensus.state == "L" && rp.raftConsensus.votedFor[int(rp.raftConsensus.currentTerm)] == rp.name {
		rp.incomingRequests = append(rp.incomingRequests, proposals...)
	}
}

// handler for append requests

func (rp *Replica) handleAppendRequest(message *proto.RaftConsensus) {
	var appendEntryResponse *proto.RaftConsensus

	if message.Term < rp.raftConsensus.currentTerm {

		rp.debug(fmt.Sprintf("rejected append entry %v because it is from a previous term", message), 0)

		appendEntryResponse.Term = rp.raftConsensus.currentTerm
		appendEntryResponse.Success = false
		rp.sendMessage(message.Sender, common.RPCPair{
			Code: rp.messageCodes.RaftConsensus,
			Obj:  appendEntryResponse,
		})
		return
	}

	if rp.raftConsensus.viewTimer != nil {
		rp.raftConsensus.viewTimer.Cancel()
		rp.raftConsensus.viewTimer = nil
	}

	lenLog := int64(len(rp.raftConsensus.log))

	if message.PrevLogIndex+1 > lenLog {
		rp.debug(fmt.Sprintf("rejected append entry %v because i do not have the prev index", message), 0)
		appendEntryResponse.Term = rp.raftConsensus.currentTerm
		appendEntryResponse.Success = false
		appendEntryResponse.PrevLogIndex = message.PrevLogIndex
		rp.sendMessage(message.Sender, common.RPCPair{
			Code: rp.messageCodes.RaftConsensus,
			Obj:  appendEntryResponse,
		})
		rp.setRaftViewTimer(int32(rp.raftConsensus.currentTerm))
		return
	} else {
		// PrevLogIndex exists
		prevLogTerm := rp.raftConsensus.log[message.PrevLogIndex].term
		prevLogValues := rp.raftConsensus.log[message.PrevLogIndex].commands.UniqueId

		if (prevLogTerm != message.PrevLogTerm) || (prevLogTerm == message.PrevLogTerm && prevLogValues != message.PrevLogValues) {

			rp.debug(fmt.Sprintf("rejected append entry %v because the prev entries do not match", message), 0)

			appendEntryResponse.Term = rp.raftConsensus.currentTerm
			appendEntryResponse.Success = false
			appendEntryResponse.PrevLogIndex = message.PrevLogIndex
			rp.sendMessage(message.Sender, common.RPCPair{
				Code: rp.messageCodes.RaftConsensus,
				Obj:  appendEntryResponse,
			})
			rp.setRaftViewTimer(int32(rp.raftConsensus.currentTerm))
			return
		} else {
			//prevLogTerm matching!!!

			lenLeaderLog := message.PrevLogIndex + int64(len(message.Entries))
			rp.createRaftInstanceIfMissing(int(lenLeaderLog))

			if message.Term > rp.raftConsensus.currentTerm {
				rp.raftConsensus.currentTerm = message.Term
				rp.raftConsensus.state = "F"
				rp.raftConsensus.votedFor[int(rp.raftConsensus.currentTerm)] = message.Sender

			} else if message.Term == rp.raftConsensus.currentTerm {
				rp.raftConsensus.state = "F"
				rp.raftConsensus.votedFor[int(rp.raftConsensus.currentTerm)] = message.Sender
			}
			appendEntryResponse.Entries = make([]*proto.RaftConsensusEntry, len(message.Entries))

			for i := int64(0); i < int64(len(message.Entries)); i++ {

				if rp.raftConsensus.log[message.PrevLogIndex+1+i].decided && rp.raftConsensus.log[message.PrevLogIndex+1+i].decisions.UniqueId != message.Entries[i].Value.UniqueId {
					panic("safety violation")
				}
				rp.raftConsensus.log[message.PrevLogIndex+1+i].term = message.Entries[i].Term
				rp.raftConsensus.log[message.PrevLogIndex+1+i].commands = message.Entries[i].Value
				appendEntryResponse.Entries[i] = &proto.RaftConsensusEntry{
					Value: &proto.ReplicaBatch{
						UniqueId: message.Entries[i].Value.UniqueId,
					},
					Term: message.Entries[i].Term,
				}
			}

			rp.debug(fmt.Sprintf("accepted append entry %v", message), 0)

			rp.raftSMR(message.LeaderCommit)

			appendEntryResponse.Term = message.Term
			appendEntryResponse.Success = true
			rp.sendMessage(message.Sender, common.RPCPair{
				Code: rp.messageCodes.RaftConsensus,
				Obj:  appendEntryResponse,
			})
			return

		}
	}
}

// handler for append responses

func (rp *Replica) handleAppendResponse(message *proto.RaftConsensus) {
	if rp.raftConsensus.state != "L" {
		return
	}
	respTerm := message.Term
	respSuccess := message.Success
	prevIndex := message.PrevLogIndex

	if respSuccess {
		for i := 0; i < len(message.Entries); i++ {
			rp.raftConsensus.log[int(prevIndex)+1+i].numSucccess++
			if rp.raftConsensus.log[int(prevIndex)+1+i].numSucccess == int(rp.raftConsensus.numNodes)/2+1 {
				if !rp.raftConsensus.log[int(prevIndex)+1+i].decided {
					rp.raftConsensus.log[int(prevIndex)+1+i].decided = true
				}
			}
		}
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

	if in.isMajority(int64(nay)) || foundHigherTerm {
		in.state = "FOLLOWER"
	} else if in.isMajority(int64(yea)) && len(values) > 0 {
		// decide
		lastAddedIndex := int64(len(in.log)) - 1
		in.updateLastDecidedEntry(lastAddedIndex)
	}
}

// compare the log against the values of the leader request

func (rp *Replica) compareLog(lastLogIndex int64, lastLogTerm int64) bool {

	myLastLogIndex := int64(len(rp.raftConsensus.log) - 1)
	myLastLogTerm := rp.raftConsensus.log[myLastLogIndex].term

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

// handler for leader requests

func (rp *Replica) handleLeaderRequest(message *proto.RaftConsensus) {
	var leaderResponse *proto.RaftConsensus

	if rp.raftConsensus.currentTerm >= message.Term {
		leaderResponse.Term = rp.raftConsensus.currentTerm
		leaderResponse.Success = false
	} else {
		votedFor, ok := rp.raftConsensus.votedFor[int(message.Term)]
		if !ok || votedFor == message.Sender {
			result := rp.compareLog(message.PrevLogIndex, message.PrevLogTerm)
			if result {
				rp.raftConsensus.currentTerm = message.Term
				rp.raftConsensus.votedFor[int(message.Term)] = message.Sender
				rp.debug(fmt.Sprintf("became a follower, leader for term %v is %v", message.Term, message.Sender), 0)
				rp.raftConsensus.state = "F"
				leaderResponse.Term = message.Term
				leaderResponse.Success = true
				if rp.raftConsensus.viewTimer != nil {
					rp.raftConsensus.viewTimer.Cancel()
					rp.setRaftViewTimer(int32(rp.raftConsensus.currentTerm))
				}

			} else {
				leaderResponse.Term = rp.raftConsensus.currentTerm
				leaderResponse.Success = false
			}
		} else {
			leaderResponse.Term = rp.raftConsensus.currentTerm
			leaderResponse.Success = false
		}
	}

	rp.sendMessage(message.Sender, common.RPCPair{
		Code: rp.messageCodes.RaftConsensus,
		Obj:  leaderResponse,
	})
}

// handler for leader responses

func (rp *Replica) handleLeaderResponse(message *proto.RaftConsensus) {
	if rp.raftConsensus.state == "C" && rp.raftConsensus.currentTerm == message.Term && message.Success {
		reponses, ok := rp.raftConsensus.leaderResponses[int(message.Term)]
		if !ok {
			rp.raftConsensus.leaderResponses[int(message.Term)] = []*proto.RaftConsensus{message}
		} else {
			rp.raftConsensus.leaderResponses[int(message.Term)] = append(reponses, message)
		}

		reponses, ok = rp.raftConsensus.leaderResponses[int(message.Term)]

		if !ok {
			panic("should not happen")
		}

		if len(reponses) == int(rp.raftConsensus.numNodes)/2 {
			rp.debug("received majority responses for the leader request for term"+fmt.Sprintf(" %v ", message.Term), 0)
			rp.raftConsensus.state = "L"
			if rp.raftConsensus.viewTimer != nil {
				rp.raftConsensus.viewTimer.Cancel()
				rp.setRaftViewTimer(int32(rp.raftConsensus.currentTerm))
			}
		}
	}

}

// handler for timeouts

func (rp *Replica) handleRaftInternalTimeout(message *proto.RaftConsensus) {

	if rp.raftConsensus.currentTerm > message.Term {
		return
	}

	rp.debug(fmt.Sprintf("%v timeout in view %v, and becoming and candidate", rp.name, rp.raftConsensus.currentTerm), 0)

	rp.raftConsensus.currentTerm++
	rp.raftConsensus.state = "C"
	rp.raftConsensus.votedFor[int(rp.raftConsensus.currentTerm)] = rp.name
	rp.sendRequestVote()

}

// broadcast request vote to everyone except the self

func (rp *Replica) sendRequestVote() {
	term := rp.raftConsensus.currentTerm
	candidateId := rp.raftConsensus.name
	lastLogIndex := int64(len(rp.raftConsensus.log) - 1)
	lastLogTerm := rp.raftConsensus.log[lastLogIndex].term

	// send a propose message
	for name, _ := range rp.replicaAddrList {
		if name == rp.name {
			continue
		}
		leaderRequest := proto.RaftConsensus{
			Sender:       candidateId,
			Receiver:     name,
			Type:         3,
			Term:         term,
			PrevLogIndex: lastLogIndex,
			PrevLogTerm:  lastLogTerm,
		}

		rpcPair := common.RPCPair{
			Code: rp.messageCodes.RaftConsensus,
			Obj:  &leaderRequest,
		}

		rp.sendMessage(name, rpcPair)
	}

	rp.debug("Sent leader request"+fmt.Sprintf(" term: %v, prevLogIndex: %v, prevLogTerm: %v ", term, lastLogIndex, lastLogTerm)+" at time "+fmt.Sprintf("%v", time.Now().Sub(rp.raftConsensus.startTime).Milliseconds()), 0)

	if rp.raftConsensus.viewTimer != nil {
		rp.raftConsensus.viewTimer.Cancel()
		rp.setRaftViewTimer(int32(term))
	}
}
