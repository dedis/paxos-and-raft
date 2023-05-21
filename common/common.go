package common

import (
	"bytes"
	"encoding/gob"
	"paxos_raft/configuration"
	"paxos_raft/proto"
	"strconv"
	"time"
)

/*
	RPC pair assigns a unique uint8 to each type of message defined in the proto files
*/

type RPCPair struct {
	Code uint8
	Obj  proto.Serializable
}

/*
	Outgoing RPC assigns a rpc to its intended destination
*/

type OutgoingRPC struct {
	RpcPair *RPCPair
	Peer    int32
}

/*
	Returns the self ip:port
*/

func GetAddress(nodes []configuration.Instance, name int32) string {
	for i := 0; i < len(nodes); i++ {
		if nodes[i].Name == strconv.Itoa(int(name)) {
			return nodes[i].Address
		}
	}
	panic("should not happen")
}

/*
	Timer for triggering event upon timeout
*/

type TimerWithCancel struct {
	d time.Duration
	t *time.Timer
	c chan interface{}
	f func()
}

/*
	instantiate a new timer with cancel
*/

func NewTimerWithCancel(d time.Duration) *TimerWithCancel {
	t := &TimerWithCancel{}
	t.d = d
	t.c = make(chan interface{}, 5)
	return t
}

/*
	Start the timer
*/

func (t *TimerWithCancel) Start() {
	t.t = time.NewTimer(t.d)
	go func() {
		select {
		case <-t.t.C:
			t.f()
			return
		case <-t.c:
			return
		}
	}()
}

/*
	Set a function to call when timeout
*/

func (t *TimerWithCancel) SetTimeoutFuntion(f func()) {
	t.f = f
}

/*
	Cancel timer
*/
func (t *TimerWithCancel) Cancel() {
	select {
	case t.c <- nil:
		// Success
		break
	default:
		//Unsuccessful
		break
	}

}

/*
	util function to get the size of a message
*/

func GetRealSizeOf(v interface{}) (int, error) {
	b := new(bytes.Buffer)
	if err := gob.NewEncoder(b).Encode(v); err != nil {
		return 0, err
	}
	return b.Len(), nil
}
