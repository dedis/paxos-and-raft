Client implementation supports two operations.

(1) Send a ```status``` request to replicas

(2) Send SMR ```request```s to replicas

To send a status request ```./client/bin/client --name 11 --requestType status --operationType [1, 2, 3]```

```OperationType 1``` for server bootstrapping, ```OperationType 2``` for server log printing, and ```OperationType 3``` for starting consensus layer

To send client requests with minimal options ```./client/bin/client --name 11 --defaultReplica 1 --requestType request```

You can find a bunch of supported paramters in the ```client/main.go``` 