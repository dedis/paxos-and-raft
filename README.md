This repo implements Paxos and Raft.

This repo uses [Protocol Buffers](https://developers.google.com/protocol-buffers/).
It requires the ```protoc``` compiler with the ```go``` output plugin installed.

This repo uses [Redis](https://redis.io/topics/quickstart) and it should be installed with default options.

All implementations are tested in ```Ubuntu 20.04.3 LTS```

run ```go get -u github.com/golang/protobuf/protoc-gen-go``` and ```go get -u google.golang.org/grpc``` to install ```protobuff``` and ```grpc```


run ```protoc --go_out=./ --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ./proto/definitions.proto``` to generate the stubs, and then copy the ```definitions_grpc.pb.go``` from ```proto``` to ```replica/src```



run ```go mod vendor``` to install dependencies


run ```go build -v -o ./client/bin/client ./client/``` and ```go build -v -o ./replica/bin/replica ./replica/``` to build the client and the replica


All the commands to run replicas and the clients are available in the ```integration-test/``` directory