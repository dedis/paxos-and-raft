package src

import (
	"fmt"
	"github.com/montanaflynn/stats"
	"os"
	"paxos_raft/proto"
	"strconv"
)

const CLIENT_TIMEOUT = 2000000

/*
	calculate the number of elements in the 2d array
*/
func (cl *Client) getNumberOfSentRequests(requests [][]requestBatch) int {
	count := 0
	for i := 0; i < numRequestGenerationThreads; i++ {
		sentRequestArrayOfI := requests[i] // requests[i] is an array of batch of requests
		for j := 0; j < len(sentRequestArrayOfI); j++ {
			count += len(sentRequestArrayOfI[j].batch.Requests)
		}
	}
	return count
}

/*
	Add value N to list, M times
*/

func (cl *Client) addValueNToArrayMTimes(list []int64, N int64, M int) []int64 {
	for i := 0; i < M; i++ {
		list = append(list, N)
	}
	return list
}

/*
	Count the number of individual responses in responses array
*/

func (cl *Client) getNumberOfReceivedResponses(responses map[string]requestBatch) int {
	count := 0
	for _, element := range responses {
		count += len(element.batch.Requests)
	}
	return count

}

/*
	Map the request with the response batch
    Compute the time taken for each request
	Computer the error rate
	Compute the throughput as successfully committed requests per second (doesn't include failed requests)
	Compute the latency
	Print the basic stats to the stdout and the logs to a file
*/

func (cl *Client) computeStats() {

	f, err := os.Create(cl.logFilePath + strconv.Itoa(int(cl.clientName)) + ".txt") // log file
	if err != nil {
		panic("Error creating the output log file")
	}
	defer f.Close()

	numTotalSentRequests := cl.getNumberOfSentRequests(cl.sentRequests)
	var latencyList []int64 // contains the time duration spent requests in micro seconds
	responses := 0
	for i := 0; i < numRequestGenerationThreads; i++ {
		fmt.Printf("Calculating stats for thread %d \n", i)
		for j := 0; j < len(cl.sentRequests[i]); j++ {
			batch := cl.sentRequests[i][j]
			batchId := batch.batch.UniqueId
			matchingResponseBatch, ok := cl.receivedResponses[batchId]
			if ok {
				responseBatch := matchingResponseBatch
				startTime := batch.time
				endTime := responseBatch.time
				batchLatency := endTime.Sub(startTime).Microseconds()
				if batchLatency < CLIENT_TIMEOUT {
					latencyList = cl.addValueNToArrayMTimes(latencyList, batchLatency, len(batch.batch.Requests))
					cl.printRequests(batch.batch, startTime.Sub(cl.startTime).Microseconds(), endTime.Sub(cl.startTime).Microseconds(), f)
					responses += len(batch.batch.Requests)
				} else {
					latencyList = cl.addValueNToArrayMTimes(latencyList, CLIENT_TIMEOUT, len(batch.batch.Requests))
					cl.printRequests(batch.batch, startTime.Sub(cl.startTime).Microseconds(), startTime.Sub(cl.startTime).Microseconds()+CLIENT_TIMEOUT, f)
				}
			} else {
				startTime := batch.time
				batchLatency := CLIENT_TIMEOUT
				latencyList = cl.addValueNToArrayMTimes(latencyList, int64(batchLatency), len(batch.batch.Requests))
				cl.printRequests(batch.batch, startTime.Sub(cl.startTime).Microseconds(), startTime.Sub(cl.startTime).Microseconds()+CLIENT_TIMEOUT, f)
			}

		}
	}

	medianLatency, _ := stats.Median(cl.getFloat64List(latencyList))
	percentile99, _ := stats.Percentile(cl.getFloat64List(latencyList), 99.0) // tail latency
	duration := cl.testDuration
	errorRate := (numTotalSentRequests - responses) * 100.0 / numTotalSentRequests

	fmt.Printf("Total time := %v seconds\n", duration)
	fmt.Printf("Throughput (successfully committed requests) := %v requests per second\n", responses/duration)
	fmt.Printf("Median Latency := %v micro seconds per request\n", medianLatency)
	fmt.Printf("99 pecentile latency := %v micro seconds per request\n", percentile99)
	fmt.Printf("Error Rate := %v \n", float64(errorRate))
}

/*
	Converts int64[] to float64[]
*/

func (cl *Client) getFloat64List(list []int64) []float64 {
	var array []float64
	for i := 0; i < len(list); i++ {
		array = append(array, float64(list[i]))
	}
	return array
}

/*
	print a client request batch with arrival time and end time w.r.t test start time
*/

func (cl *Client) printRequests(messages proto.ClientBatch, startTime int64, endTime int64, f *os.File) {
	for i := 0; i < len(messages.Requests); i++ {
		_, _ = f.WriteString(messages.Requests[i].Command + "," + strconv.Itoa(int(startTime)) + "," + strconv.Itoa(int(endTime)) + "\n")
	}
}
