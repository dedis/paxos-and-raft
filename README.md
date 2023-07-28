# Paxos and Raft

This repository implements Paxos and Raft.

This repository uses [Protocol Buffers](https://developers.google.com/protocol-buffers/).
It requires the ```protoc``` compiler with the ```go``` output plugin installed.

This repository uses [Redis](https://redis.io/topics/quickstart) and it should be installed with default options.

All implementations are tested in ```Ubuntu 20.04.3 LTS```

run ```go get -u github.com/golang/protobuf/protoc-gen-go``` and ```go get -u google.golang.org/grpc``` to install ```protobuff``` and ```grpc```


run ```go mod vendor``` to install dependencies


run ```go build -v -o ./client/bin/client ./client/``` and ```go build -v -o ./replica/bin/replica ./replica/``` to build the client and the replica


All the commands to run replicas and the clients are available in the ```integration-test/``` directory 

This repository was implemented as part of the [QuePaxa](https://github.com/dedis/quepaxa/) project, which is accepted in the SOSP 2023 conference.

The Paxos implementation of this repository was inspired by [EPaxos](https://github.com/dedis/ePaxos-open-loop), however it improves over the existing implementation in 3 respects.

- The new implementation supports correct view change implementation 
- The new implementation supports client side batching
- The new implementation supports REDIS

