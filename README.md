This repo implements Paxos and Raft.

Our protocol uses [Protocol Buffers](https://developers.google.com/protocol-buffers/).
It requires the ```protoc``` compiler with the ```go``` output plugin installed.

Our protocol uses [Redis](https://redis.io/topics/quickstart) and it should be installed with default options.

All implementations are tested in ```Ubuntu 20.04.3 LTS```

run ```sudo go get -u github.com/golang/protobuf/protoc-gen-go``` and ```sudo go get -u google.golang.org/grpc``` to install protobuff and grpc


run ```protoc --go_out=./ --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ./proto/definitions.proto``` to generate the stubs


run ```sudo go mod vendor``` to add dependencies


run ```go build -v -o ./client/bin/client ./client/``` and ```go build -v -o ./replica/bin/replica ./replica/``` to build the client and the replica


All the commands to run replicas and the clients are available in the respective directories