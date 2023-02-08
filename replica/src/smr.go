package src

import (
	"async-consensus/common"
	"async-consensus/proto"
	"fmt"
	"time"
)

// add the client batch to buffer and propose

func (rp *Replica) handleClientBatch(batch *proto.ClientBatch) {
	rp.incomingRequests = append(rp.incomingRequests, batch)
	if rp.consAlgo == "paxos" {
		if time.Now().Sub(rp.paxosConsensus.lastProposedTime).Microseconds() > int64(rp.replicaBatchTime) || len(rp.incomingRequests) >= rp.replicaBatchSize {
			var proposals []*proto.ClientBatch
			if len(rp.incomingRequests) > rp.replicaBatchSize {
				proposals = rp.incomingRequests[:rp.replicaBatchSize]
				rp.incomingRequests = rp.incomingRequests[rp.replicaBatchSize:]
			} else {
				proposals = rp.incomingRequests
				rp.incomingRequests = make([]*proto.ClientBatch, 0)
			}
			rp.sendPropose(proposals)
			rp.paxosConsensus.lastProposedTime = time.Now()
		}
	}
	if rp.consAlgo == "raft" {
		if time.Now().Sub(rp.raftConsensus.lastProposedTime).Microseconds() > int64(rp.replicaBatchTime) || len(rp.incomingRequests) >= rp.replicaBatchSize {
			var proposals []*proto.ClientBatch
			if len(rp.incomingRequests) > rp.replicaBatchSize {
				proposals = rp.incomingRequests[:rp.replicaBatchSize]
				rp.incomingRequests = rp.incomingRequests[rp.replicaBatchSize:]
			} else {
				proposals = rp.incomingRequests
				rp.incomingRequests = make([]*proto.ClientBatch, 0)
			}
			rp.appendEntries(proposals)
			rp.raftConsensus.lastProposedTime = time.Now()
		}
	}

}

// call the state machine

func (rp *Replica) updateApplicationLogic(requests []*proto.ClientBatch) []*proto.ClientBatch {
	return rp.state.Execute(requests)
}

// send back the client responses

func (rp *Replica) sendClientResponses(responses []*proto.ClientBatch) {
	for i := 0; i < len(responses); i++ {
		if responses[i].Sender == -1 {
			continue
		}
		rp.sendMessage(int32(responses[i].Sender), common.RPCPair{
			Code: rp.messageCodes.ClientBatchRpc,
			Obj:  responses[i],
		})
		rp.debug("send client response to "+fmt.Sprintf("%v", responses[i].Sender), 0)
	}
}

// empty replica batch

func (rp *Replica) sendDummyRequests() {
	go func() {
		for true {
			time.Sleep(time.Duration(rp.viewTimeout/4) * time.Microsecond)
			clientBatch := proto.ClientBatch{
				UniqueId: "nil",
				Requests: make([]*proto.SingleOperation, 0),
				Sender:   -1,
			}
			rp.sendMessage(rp.name, common.RPCPair{
				Code: rp.messageCodes.ClientBatchRpc,
				Obj:  &clientBatch,
			})
		}
	}()
}
