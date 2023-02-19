# A local test that tests the consensus layer by sending client requests and printing the consensus logs while simulating asynchrony using SIGSTOP and SIGCONT
arrivalRate=$1
algo=$2
viewTimeoutTime=$3
testTime=$4 # seconds
batchTime=$5
batchSize=$6
pipelineLength=$7

replica_path="replica/bin/replica"
ctl_path="client/bin/client"
output_path="logs/"

rm -r ${output_path}
mkdir ${output_path}

pkill replica; pkill replica; pkill replica
pkill client; pkill client; pkill client

echo "Killed previously running instances"

nohup ./${replica_path} --name 1 --consAlgo "${algo}" --batchSize "${batchSize}" --batchTime "${batchTime}"   --debugOn --debugLevel 27 --viewTimeout "${viewTimeoutTime}" --pipelineLength "${pipelineLength}" >${output_path}1.log &
nohup ./${replica_path} --name 2 --consAlgo "${algo}" --batchSize "${batchSize}" --batchTime "${batchTime}"   --debugOn --debugLevel 27 --viewTimeout "${viewTimeoutTime}" --pipelineLength "${pipelineLength}" >${output_path}2.log &
nohup ./${replica_path} --name 3 --consAlgo "${algo}" --batchSize "${batchSize}" --batchTime "${batchTime}"   --debugOn --debugLevel 27 --viewTimeout "${viewTimeoutTime}" --pipelineLength "${pipelineLength}" >${output_path}3.log &

echo "Started 3 replicas"

sleep 5

./${ctl_path} --name 11 --requestType status --operationType 1  --debugOn --debugLevel 15 >${output_path}status1.log

sleep 5

echo "sent initial status"

./${ctl_path} --name 11 --requestType status --operationType 3  --debugOn --debugLevel 15 >${output_path}status3.log

sleep 5

echo "sent consensus start up status"

echo "starting clients"

nohup ./${ctl_path} --name 11 --requestType request --defaultReplica 2  --debugOn --debugLevel 6 --batchSize  "${batchSize}" --batchTime "${batchTime}" --arrivalRate "${arrivalRate}" --leaderTimeout "${viewTimeoutTime}" --testDuration "${testTime}"  >${output_path}11.log &
nohup ./${ctl_path} --name 12 --requestType request --defaultReplica 2  --debugOn --debugLevel 6 --batchSize  "${batchSize}" --batchTime "${batchTime}" --arrivalRate "${arrivalRate}" --leaderTimeout "${viewTimeoutTime}" --testDuration "${testTime}"  >${output_path}12.log &
./${ctl_path}       --name 13 --requestType request --defaultReplica 2  --debugOn --debugLevel 6 --batchSize  "${batchSize}" --batchTime "${batchTime}" --arrivalRate "${arrivalRate}" --leaderTimeout "${viewTimeoutTime}" --testDuration "${testTime}"  >${output_path}13.log

sleep 10

echo "finished running clients"


./${ctl_path} --name 11 --requestType status --operationType 2  --debugOn --debugLevel 10 >${output_path}status2.log

echo "sent status to print logs"

sleep 30

pkill replica; pkill replica; pkill replica
pkill client; pkill client; pkill client

python3 experiments/python/overlay-test.py "${testTime}" logs/1-consensus.txt logs/2-consensus.txt logs/3-consensus.txt > ${output_path}python-consensus.log

echo "Killed instances"

mkdir -p    /home/pasindu/Desktop/paxos_raft/"${arrivalRate}"/"${algo}"/"${viewTimeoutTime}"/"${testTime}"/"${batchTime}"/"${batchSize}"/"${pipelineLength}"
cp -r logs/ /home/pasindu/Desktop/paxos_raft/"${arrivalRate}"/"${algo}"/"${viewTimeoutTime}"/"${testTime}"/"${batchTime}"/"${batchSize}"/"${pipelineLength}"

echo "Finish test"
