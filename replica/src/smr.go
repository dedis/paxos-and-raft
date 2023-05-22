package src

import (
	"paxos_raft/common"
	"paxos_raft/proto"
	"time"
)

// add the client batch to buffer and propose

func (rp *Replica) handleClientBatch(batch *proto.ClientBatch) {
	rp.incomingRequests = append(rp.incomingRequests, batch)

	if (time.Now().Sub(rp.lastProposedTime).Microseconds() > int64(rp.replicaBatchTime) && len(rp.incomingRequests) > 0) || len(rp.incomingRequests) >= rp.replicaBatchSize {
		var proposals []*proto.ClientBatch
		if len(rp.incomingRequests) > rp.replicaBatchSize {
			proposals = rp.incomingRequests[:rp.replicaBatchSize]
			rp.incomingRequests = rp.incomingRequests[rp.replicaBatchSize:]
		} else {
			proposals = rp.incomingRequests
			rp.incomingRequests = make([]*proto.ClientBatch, 0)
		}
		if rp.consAlgo == "paxos" {
			rp.sendPropose(proposals)
		} else if rp.consAlgo == "raft" {
			select {
			case rp.requestsIn <- proposals:
				// message sent
				break
			default:
				rp.incomingRequests = append(rp.incomingRequests, proposals...)
				break
			}
		}
		rp.lastProposedTime = time.Now()
	} else {
		//rp.debug("Still did not invoke propose from smr, num client batches = "+strconv.Itoa(int(len(rp.incomingRequests)))+" ,time since last proposal "+strconv.Itoa(int(time.Now().Sub(rp.lastProposedTime).Microseconds())), 0)
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
		//rp.debug("send client response to "+strconv.Itoa(int(responses[i].Sender)), 0)
	}
}

// send dummy requests to avoid leader revokes

func (rp *Replica) sendDummyRequests(cancel chan bool) {
	go func() {
		for true {
			select {
			case _ = <-cancel:
				return
			default:
				time.Sleep(time.Duration(rp.viewTimeout/2) * time.Microsecond)
				clientBatch := proto.ClientBatch{
					UniqueId: "nil",
					Requests: make([]*proto.SingleOperation, 0),
					Sender:   -1,
				}
				rp.sendMessage(rp.name, common.RPCPair{
					Code: rp.messageCodes.ClientBatchRpc,
					Obj:  &clientBatch,
				})
				break
			}

		}
	}()
}
