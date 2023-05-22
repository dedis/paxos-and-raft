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

os.system("/bin/bash integration-test/safety_test.sh "+str(arrivalRate)+" "+"paxos"+" "+str(viewTimeoutTime)+" "+str(batchTime)+" "+str(batchSize)+" "+str(pipelineLength)+ " "+str(window))
os.system("/bin/bash integration-test/safety_test.sh "+str(arrivalRate)+" "+"raft"+" "+str(viewTimeoutTime)+" "+str(batchTime)+" "+str(batchSize)+" "+str(pipelineLength)+ " "+str(window))

print("Test-2 batching")
sys.stdout.flush()

arrivalRate=10000
viewTimeoutTime=30000000
batchTime=5000
batchSize=100
pipelineLength=1
window=1000

os.system("/bin/bash integration-test/safety_test.sh "+str(arrivalRate)+" "+"paxos"+" "+str(viewTimeoutTime)+" "+str(batchTime)+" "+str(batchSize)+" "+str(pipelineLength)+ " "+str(window))
os.system("/bin/bash integration-test/safety_test.sh "+str(arrivalRate)+" "+"raft"+" "+str(viewTimeoutTime)+" "+str(batchTime)+" "+str(batchSize)+" "+str(pipelineLength)+ " "+str(window))

print("Test-3 pipelining")
sys.stdout.flush()

arrivalRate=500
viewTimeoutTime=30000000
batchTime=500
batchSize=1
pipelineLength=10
window=100

os.system("/bin/bash integration-test/safety_test.sh "+str(arrivalRate)+" "+"paxos"+" "+str(viewTimeoutTime)+" "+str(batchTime)+" "+str(batchSize)+" "+str(pipelineLength)+ " "+str(window))


print("Test-4 timeouts")
sys.stdout.flush()

arrivalRate=5000
viewTimeoutTime=3000
batchTime=2000
batchSize=100
pipelineLength=1
window=1000

os.system("/bin/bash integration-test/safety_test.sh "+str(arrivalRate)+" "+"paxos"+" "+str(viewTimeoutTime)+" "+str(batchTime)+" "+str(batchSize)+" "+str(pipelineLength)+ " "+str(window))
os.system("/bin/bash integration-test/safety_test.sh "+str(arrivalRate)+" "+"raft"+" "+str(viewTimeoutTime)+" "+str(batchTime)+" "+str(batchSize)+" "+str(pipelineLength)+ " "+str(window))

print("Test-5 crash-recovery")
sys.stdout.flush()

arrivalRate=5000
viewTimeoutTime=300000
batchTime=2000
batchSize=100
pipelineLength=1
window=1000

os.system("/bin/bash integration-test/safety_test_crash_recovery.sh "+str(arrivalRate)+" "+"paxos"+" "+str(viewTimeoutTime)+" "+str(batchTime)+" "+str(batchSize)+" "+str(pipelineLength)+ " "+str(window))
os.system("/bin/bash integration-test/safety_test_crash_recovery.sh "+str(arrivalRate)+" "+"raft"+" "+str(viewTimeoutTime)+" "+str(batchTime)+" "+str(batchSize)+" "+str(pipelineLength)+ " "+str(window))