import os
import sys

print("Test-1 base case")
sys.stdout.flush()

arrivalRate=10
viewTimeoutTime=30000000
batchTime=100
batchSize=1
pipelineLength=1
window=1
asyncTime=0

os.system("/bin/bash integration-test/safety_test.sh "+str(arrivalRate)+" "+"paxos"+" "+str(viewTimeoutTime)+" "+str(batchTime)+" "+str(batchSize)+" "+str(pipelineLength)+ " "+str(window) + " "+str(asyncTime))
os.system("/bin/bash integration-test/safety_test.sh "+str(arrivalRate)+" "+"raft"+" "+str(viewTimeoutTime)+" "+str(batchTime)+" "+str(batchSize)+" "+str(pipelineLength)+ " "+str(window) + " "+str(asyncTime))


print("Test-2 batching")
sys.stdout.flush()

arrivalRate=10000
viewTimeoutTime=30000000
batchTime=5000
batchSize=100
pipelineLength=1
window=1000
asyncTime=0

os.system("/bin/bash integration-test/safety_test.sh "+str(arrivalRate)+" "+"paxos"+" "+str(viewTimeoutTime)+" "+str(batchTime)+" "+str(batchSize)+" "+str(pipelineLength)+ " "+str(window) + " "+str(asyncTime))
os.system("/bin/bash integration-test/safety_test.sh "+str(arrivalRate)+" "+"raft"+" "+str(viewTimeoutTime)+" "+str(batchTime)+" "+str(batchSize)+" "+str(pipelineLength)+ " "+str(window) + " "+str(asyncTime))

print("Test-3 pipelining")
sys.stdout.flush()

arrivalRate=500
viewTimeoutTime=30000000
batchTime=500
batchSize=1
pipelineLength=10
window=100
asyncTime=0

os.system("/bin/bash integration-test/safety_test.sh "+str(arrivalRate)+" "+"paxos"+" "+str(viewTimeoutTime)+" "+str(batchTime)+" "+str(batchSize)+" "+str(pipelineLength)+ " "+str(window) + " "+str(asyncTime))


print("Test-4 timeouts")
sys.stdout.flush()

arrivalRate=5000
viewTimeoutTime=3000
batchTime=2000
batchSize=100
pipelineLength=1
window=1000
asyncTime=0

os.system("/bin/bash integration-test/safety_test.sh "+str(arrivalRate)+" "+"paxos"+" "+str(viewTimeoutTime)+" "+str(batchTime)+" "+str(batchSize)+" "+str(pipelineLength)+ " "+str(window) + " "+str(asyncTime))
os.system("/bin/bash integration-test/safety_test.sh "+str(arrivalRate)+" "+"raft"+" "+str(viewTimeoutTime)+" "+str(batchTime)+" "+str(batchSize)+" "+str(pipelineLength)+ " "+str(window) + " "+str(asyncTime))

print("Test-5 attack")
sys.stdout.flush()

arrivalRate=5000
viewTimeoutTime=3000
batchTime=2000
batchSize=100
pipelineLength=1
window=1000
asyncTime=10

os.system("/bin/bash integration-test/safety_test.sh "+str(arrivalRate)+" "+"paxos"+" "+str(viewTimeoutTime)+" "+str(batchTime)+" "+str(batchSize)+" "+str(pipelineLength)+ " "+str(window) + " "+str(asyncTime))
os.system("/bin/bash integration-test/safety_test.sh "+str(arrivalRate)+" "+"raft"+" "+str(viewTimeoutTime)+" "+str(batchTime)+" "+str(batchSize)+" "+str(pipelineLength)+ " "+str(window) + " "+str(asyncTime))