# A local test that tests the consensus layer by sending client requests and printing the consensus logs while simulating asynchrony using SIGSTOP and SIGCONT
arrivalRate=$1
algo=$2
viewTimeoutTime=$3
batchTime=$4
batchSize=$5
pipelineLength=$6
window=$7

replica_path="replica/bin/replica"
ctl_path="client/bin/client"
output_path="logs/"

rm -r ${output_path}
mkdir ${output_path}

pkill replica; pkill replica; pkill replica
pkill client; pkill client; pkill client

echo "Killed previously running instances"

nohup ./${replica_path} --name 1 --consAlgo "${algo}" --batchSize "${batchSize}" --batchTime "${batchTime}"   --debugOn --debugLevel 0 --viewTimeout "${viewTimeoutTime}" --pipelineLength "${pipelineLength}" >${output_path}1.log &
nohup ./${replica_path} --name 2 --consAlgo "${algo}" --batchSize "${batchSize}" --batchTime "${batchTime}"   --debugOn --debugLevel 0 --viewTimeout "${viewTimeoutTime}" --pipelineLength "${pipelineLength}" >${output_path}2.log &
nohup ./${replica_path} --name 3 --consAlgo "${algo}" --batchSize "${batchSize}" --batchTime "${batchTime}"   --debugOn --debugLevel 0 --viewTimeout "${viewTimeoutTime}" --pipelineLength "${pipelineLength}" >${output_path}3.log &
nohup ./${replica_path} --name 4 --consAlgo "${algo}" --batchSize "${batchSize}" --batchTime "${batchTime}"   --debugOn --debugLevel 0 --viewTimeout "${viewTimeoutTime}" --pipelineLength "${pipelineLength}" >${output_path}4.log &
nohup ./${replica_path} --name 5 --consAlgo "${algo}" --batchSize "${batchSize}" --batchTime "${batchTime}"   --debugOn --debugLevel 0 --viewTimeout "${viewTimeoutTime}" --pipelineLength "${pipelineLength}" >${output_path}5.log &

echo "Started 3 replicas"

sleep 5

./${ctl_path} --name 11 --requestType status --operationType 1  --debugOn --debugLevel 0 >${output_path}status1.log

sleep 5

echo "sent initial status"

./${ctl_path} --name 11 --requestType status --operationType 3  --debugOn --debugLevel 0 >${output_path}status3.log

sleep 5

echo "sent consensus start up status"

echo "starting clients"

nohup ./${ctl_path} --name 11 --requestType request --debugOn --debugLevel 0 --batchSize  "${batchSize}" --batchTime "${batchTime}" --arrivalRate "${arrivalRate}" --window "${window}" >${output_path}11.log &
nohup ./${ctl_path} --name 12 --requestType request --debugOn --debugLevel 0 --batchSize  "${batchSize}" --batchTime "${batchTime}" --arrivalRate "${arrivalRate}" --window "${window}" >${output_path}12.log &
nohup ./${ctl_path} --name 13 --requestType request --debugOn --debugLevel 0 --batchSize  "${batchSize}" --batchTime "${batchTime}" --arrivalRate "${arrivalRate}" --window "${window}" >${output_path}13.log &
nohup ./${ctl_path} --name 14 --requestType request --debugOn --debugLevel 0 --batchSize  "${batchSize}" --batchTime "${batchTime}" --arrivalRate "${arrivalRate}" --window "${window}" >${output_path}14.log &
nohup ./${ctl_path} --name 15 --requestType request --debugOn --debugLevel 0 --batchSize  "${batchSize}" --batchTime "${batchTime}" --arrivalRate "${arrivalRate}" --window "${window}" >${output_path}15.log &

sleep 100

echo "finished running clients"


nohup ./${ctl_path} --name 11 --requestType status --operationType 2  --debugOn --debugLevel 10 >${output_path}status2.log &


echo "sent status to print logs"

sleep 30

pkill replica; pkill replica; pkill replica
pkill client; pkill client; pkill client

python3 experiments/python/overlay-test.py "${testTime}" logs/1-consensus.txt logs/2-consensus.txt logs/3-consensus.txt logs/4-consensus.txt logs/5-consensus.txt > ${output_path}python-consensus.log

echo "Killed instances"

echo "Finish test"
