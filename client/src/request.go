package src

import (
	"fmt"
	"math"
	"math/rand"
	"paxos_raft/common"
	"paxos_raft/proto"
	"strconv"
	"time"
	"unsafe"
)

/*
	Upon receiving a client response, add the request to the received requests map
*/

func (cl *Client) handleClientResponseBatch(batch *proto.ClientBatch) {
	if cl.finished {
		return
	}

	_, ok := cl.receivedResponses[batch.UniqueId]
	if ok {
		return
	}

	cl.receivedResponses[batch.UniqueId] = requestBatch{
		batch: *batch,
		time:  time.Now(), // record the time when the response was received
	}
	cl.receivedNumMutex.Lock()
	cl.numReceivedBatches++
	cl.receivedNumMutex.Unlock()
	//cl.debug("Added response Batch with id "+batch.UniqueId, 0)
}

/*
	start the poisson arrival process (put arrivals to arrivalTimeChan) in a separate thread
	start request generation processes  (get arrivals from arrivalTimeChan and generate batches and send them) in separate threads, and send them to all replicas, and write batch to the correct array in sentRequests
	start the scheduler that schedules new requests
	the thread sleeps for test duration and then starts processing the responses. This is to handle inflight responses after the test duration
*/

func (cl *Client) SendRequests() {
	cl.generateArrivalTimes()
	cl.startRequestGenerators()
	cl.startScheduler() // this is sync, main thread waits for this to finish

	// end of test

	time.Sleep(time.Duration(10) * time.Second) // additional sleep duration to make sure that all the in-flight responses are received
	fmt.Printf("Finish sending requests \n")
	cl.finished = true
	cl.computeStats()
}

/*
	Each request generator generates requests by generating string requests, forming batches, send batches and save them in the correct sent array
*/

func (cl *Client) startRequestGenerators() {
	for i := 0; i < numRequestGenerationThreads; i++ { // i is the thread number
		go func(threadNumber int) {
			localCounter := 0
			lastSent := time.Now() // used to get how long to wait
			for true {             // this runs forever
				if cl.finished {
					return
				}
				numRequests := 0
				var requests []*proto.SingleOperation
				// this loop collects requests until the minimum batch size is met OR the batch time is timeout
				for !(numRequests >= cl.clientBatchSize || (time.Now().Sub(lastSent).Microseconds() > int64(cl.clientBatchTime) && numRequests > 0)) {
					_ = <-cl.arrivalChan // keep collecting new requests arrivals
					requests = append(requests, &proto.SingleOperation{
						Command: fmt.Sprintf("%d%v%v", rand.Intn(2),
							cl.RandString(cl.keyLen),
							cl.RandString(cl.valueLen)),
					})
					numRequests++
				}
				cl.receivedNumMutex.Lock()
				if (cl.numSentBatches - cl.numReceivedBatches) > cl.window {
					cl.receivedNumMutex.Unlock()
					continue
				}
				cl.receivedNumMutex.Unlock()

				for i, _ := range cl.replicaAddrList {

					var requests_i []*proto.SingleOperation

					for j := 0; j < len(requests); j++ {
						requests_i = append(requests_i, requests[j])
					}

					// create a new client batch
					batch := proto.ClientBatch{
						UniqueId: strconv.Itoa(int(cl.clientName)) + "." + strconv.Itoa(threadNumber) + "." + strconv.Itoa(localCounter), // this is a unique string id,
						Requests: requests_i,
						Sender:   int64(cl.clientName),
					}
					//cl.debug("Sending "+strconv.Itoa(int(cl.clientName))+"."+strconv.Itoa(threadNumber)+"."+strconv.Itoa(localCounter)+" batch size "+strconv.Itoa(len(requests)), 0)

					rpcPair := common.RPCPair{
						Code: cl.messageCodes.ClientBatchRpc,
						Obj:  &batch,
					}

					cl.sendMessage(i, rpcPair)
				}

				batch := proto.ClientBatch{
					UniqueId: strconv.Itoa(int(cl.clientName)) + "." + strconv.Itoa(threadNumber) + "." + strconv.Itoa(localCounter), // this is a unique string id,
					Requests: requests,
					Sender:   int64(cl.clientName),
				}

				cl.numSentBatches++

				localCounter++
				lastSent = time.Now()
				cl.sentRequests[threadNumber] = append(cl.sentRequests[threadNumber], requestBatch{
					batch: batch,
					time:  time.Now(),
				})
			}

		}(i)
	}

}

/*
	After the request arrival time is arrived, inform the request generators
*/

func (cl *Client) startScheduler() {
	cl.startTime = time.Now()
	for time.Now().Sub(cl.startTime).Nanoseconds() < int64(cl.testDuration*1000*1000*1000) {
		nextArrivalTime := <-cl.arrivalTimeChan

		for time.Now().Sub(cl.startTime).Nanoseconds() < nextArrivalTime {
			// busy waiting until the time to dispatch this request arrives
		}
		cl.arrivalChan <- true
	}
}

/*
	generate Poisson arrival times
*/

func (cl *Client) generateArrivalTimes() {
	go func() {
		lambda := float64(cl.arrivalRate) / (1000.0 * 1000.0 * 1000.0) // requests per nano second
		arrivalTime := 0.0

		for true {
			// Get the next probability value from Uniform(0,1)
			p := rand.Float64()

			//Plug it into the inverse of the CDF of Exponential(_lamnbda)
			interArrivalTime := -1 * (math.Log(1.0-p) / lambda)

			// Add the inter-arrival time to the running sum
			arrivalTime = arrivalTime + interArrivalTime

			cl.arrivalTimeChan <- int64(arrivalTime)
		}
	}()
}

/*
	random string generation adapted from the Rabia SOSP 2021 code base https://github.com/haochenpan/rabia/
*/

const (
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" // low conflict
	letterIdxBits = 6                                                      // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1                                   // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits                                     // # of letter indices fitting in 63 bits
)

/*
	generate a random string of length n
*/

func (cl *Client) RandString(n int) string {
	b := make([]byte, n)
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return *(*string)(unsafe.Pointer(&b))
}
