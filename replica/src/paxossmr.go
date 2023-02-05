package src

import (
	"async-consensus/proto"
	"fmt"
	"strconv"
	"time"
)

/*
	update SMR logic
*/

func (rp *Replica) updatePaxosSMR() {

	for i := rp.paxosConsensus.lastCommittedLogIndex + 1; i < int32(len(rp.paxosConsensus.replicatedLog)); i++ {

		if rp.paxosConsensus.replicatedLog[i].decided == true {
			var cllientResponses []*proto.ClientBatch
			cllientResponses = rp.updateApplicationLogic(rp.paxosConsensus.replicatedLog[i].decisions.Requests)
			if rp.paxosConsensus.replicatedLog[i].decisions.Sender == int64(rp.name) {
				rp.sendClientResponses(cllientResponses)
			}
			rp.debug("Committed paxos consensus instance "+"."+strconv.Itoa(int(i))+" at time "+fmt.Sprintf("%v", time.Now().Sub(rp.paxosConsensus.startTime).Milliseconds()), 5)
			rp.paxosConsensus.lastCommittedLogIndex = i
			rp.paxosConsensus.lastCommittedTime = time.Now()
		} else {
			break
		}

	}
}
