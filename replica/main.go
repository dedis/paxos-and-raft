package main

import (
	"flag"
	"fmt"
	"os"
	"paxos_raft/configuration"
	"paxos_raft/replica/src"
)

func main() {
	name := flag.Int64("name", 1, "name of the replica as specified in the configuration.yml")
	configFile := flag.String("config", "configuration/local/configuration.yml", "configuration file")
	consAlgo := flag.String("consAlgo", "paxos", "consensus algo [raft, paxos]")
	logFilePath := flag.String("logFilePath", "logs/", "log file path")
	batchSize := flag.Int("batchSize", 50, "batch size")
	batchTime := flag.Int("batchTime", 5000, "maximum time to wait for collecting a batch of requests in micro seconds")
	debugOn := flag.Bool("debugOn", false, "false or true")
	isAsync := flag.Bool("isAsync", false, "false or true to simulate asynchrony")
	debugLevel := flag.Int("debugLevel", 0, "debug level")
	viewTimeout := flag.Int("viewTimeout", 2000000, "view timeout in micro seconds")
	keyLen := flag.Int("keyLen", 8, "key length")
	valLen := flag.Int("valLen", 8, "value length")
	benchmarkMode := flag.Int("benchmarkMode", 0, "0: resident store, 1: redis")
	pipelineLength := flag.Int("pipelineLength", 1, "pipeline length")
	asyncTimeout := flag.Int("asyncTimeout", 500, "artificial asynchronous timeout in milli seconds")
	timeEpochSize := flag.Int("timeEpochSize", 500, "duration of a time epoch for the attacker in milli seconds")

	flag.Parse()

	cfg, err := configuration.NewInstanceConfig(*configFile, *name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		panic(err)
	}

	rp := src.New(int32(*name), cfg, *logFilePath, *batchSize, *batchTime, *debugOn, *debugLevel, *viewTimeout, *consAlgo, *benchmarkMode, *keyLen, *valLen, *pipelineLength, *isAsync, *asyncTimeout, *timeEpochSize)

	rp.WaitForConnections()
	rp.StartOutgoingLinks()
	rp.Run() // this is run in main thread

}
