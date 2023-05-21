package main

import (
	"flag"
	"paxos_raft/client/src"
	"paxos_raft/configuration"
	"time"
)

func main() {
	name := flag.Int64("name", 21, "name of the client as specified in the configuration.yml")
	configFile := flag.String("config", "configuration/local/configuration.yml", "configuration file")
	logFilePath := flag.String("logFilePath", "logs/", "log file path")
	batchSize := flag.Int("batchSize", 50, "client batch size")
	batchTime := flag.Int("batchTime", 5000, "maximum time to wait for collecting a batch of requests in micro seconds")
	testDuration := flag.Int("testDuration", 60, "test duration in seconds")
	arrivalRate := flag.Int("arrivalRate", 1000, "poisson arrival rate in requests per second")
	requestType := flag.String("requestType", "status", "request type: [status , request]")
	operationType := flag.Int("operationType", 1, "Type of operation for a status request: 1 (bootstrap server), 2: (print log), 3: start consensus")
	debugOn := flag.Bool("debugOn", false, "false or true")
	debugLevel := flag.Int("debugLevel", -1, "debug level int")
	keyLen := flag.Int("keyLen", 8, "key length")
	valLen := flag.Int("valLen", 8, "value length")
	window := flag.Int64("window", 1000, "number of out standing client batches")

	flag.Parse()

	cfg, err := configuration.NewInstanceConfig(*configFile, *name)
	if err != nil {
		panic(err.Error())
	}

	cl := src.New(int32(*name), cfg, *logFilePath, *batchSize, *batchTime, *testDuration, *arrivalRate, *requestType, *operationType, *debugOn, *debugLevel, *keyLen, *valLen, *window)

	cl.WaitForConnections()
	cl.Run()
	cl.StartOutgoingLinks()
	cl.ConnectToReplicas()

	time.Sleep(5 * time.Second)

	if cl.RequestType == "status" {
		cl.SendStatus(cl.OperationType)
	} else if cl.RequestType == "request" {
		cl.SendRequests()
	}
}
