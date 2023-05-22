import os
import sys

print("Test-1 base case")
sys.stdout.flush()

arrivalRate=100
algo="paxos"
viewTimeoutTime=30000000
batchTime=100
batchSize=1
pipelineLength=1
window=1

os.system("/bin/bash integration-test/safety-test.sh "+str(arrivalRate)+" "+algo+" "+str(viewTimeoutTime)+" "+str(batchTime)+" "+str(batchSize)+" "+str(pipelineLength)+ " "+str(window))

