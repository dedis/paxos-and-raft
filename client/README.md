Client implementation supports two operations.

(1) Send a ```status``` request to replicas

(2) Send SMR ```request```s to replicas

Status supports 3 operations: ```OperationType 1``` for server bootstrapping, ```OperationType 2``` for server log printing, and ```OperationType 3``` for starting consensus layer

```integration-test/safety_test.sh``` contains different client operation examples