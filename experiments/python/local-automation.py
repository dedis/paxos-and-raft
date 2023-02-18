import os


testTime=60 # seconds
pipelineLength=1

def run(arrivalRate, algo, viewTimeoutTime, batchTime, batchSize,pipelineLength):
    os.system("/bin/bash /home/pasindu/Documents/paxos_n_raft/experiments/bash/local/local_consensus_test.sh "+ str(arrivalRate) + " "
              + algo + " "
              + str(viewTimeoutTime) + " "
              + str(testTime) + " "
              + str(batchTime) + " "
              + str(batchSize) + " "
              + str(pipelineLength))

for algo in ["paxos", "raft"]:
    # case 1
    arrivalRate=1000
    viewTimeoutTime=3000000
    batchTime=100
    batchSize=1
    run(arrivalRate, algo, viewTimeoutTime, batchTime, batchSize,pipelineLength)

    # case 2
    arrivalRate=10000
    viewTimeoutTime=3000000
    batchTime=2000
    batchSize=50
    run(arrivalRate, algo, viewTimeoutTime, batchTime, batchSize,pipelineLength)

    # case 3
    arrivalRate=10000
    viewTimeoutTime=30000
    batchTime=2000
    batchSize=50
    run(arrivalRate, algo, viewTimeoutTime, batchTime, batchSize,pipelineLength)


    # case 4
    arrivalRate=10000
    viewTimeoutTime=3000
    batchTime=2000
    batchSize=50
    run(arrivalRate, algo, viewTimeoutTime, batchTime, batchSize,pipelineLength)

    # case 5
    arrivalRate=10000
    viewTimeoutTime=300
    batchTime=2000
    batchSize=50
    run(arrivalRate, algo, viewTimeoutTime, batchTime, batchSize,pipelineLength)

    # case 6
    arrivalRate=5000
    viewTimeoutTime=3000000
    batchTime=100
    batchSize=1
    pipelineLength=100
    run(arrivalRate, algo, viewTimeoutTime, batchTime, batchSize,pipelineLength)