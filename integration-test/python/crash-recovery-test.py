import atexit
import os
import random
import sys
import time

print("Starting crash recovery script")
sys.stdout.flush()
process_logs = [sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]]
wait_time = 1


def find_process_ids(process_logs):
    process_ids = []
    for i in range(len(process_logs)):
        with open(process_logs[i]) as file:
            lines = [line.rstrip() for line in file]
        
        line= ""
        
        for j in range(len(lines)):
            if lines[j].startswith("--Initialized"):
                line = lines[j]
                break
        
        process_id = line.split(" ")[7]
        process_ids.append(process_id)
    return process_ids


processes = find_process_ids(process_logs)

print("The process IDs are " + str(processes))
sys.stdout.flush()


def exit_handler():
    print("Ending!")
    sys.stdout.flush()
    os.system("kill -CONT " + processes[0])
    os.system("kill -CONT " + processes[1])
    os.system("kill -CONT " + processes[2])
    os.system("kill -CONT " + processes[3])
    os.system("kill -CONT " + processes[4])


atexit.register(exit_handler)

t_end = time.time() + 40
while time.time() < t_end:
    randomInstance = random.randint(0, 4)
    print("stopping" + str(processes[randomInstance]))
    os.system("kill -STOP " + processes[randomInstance])
    time.sleep(int(wait_time))
    os.system("kill -CONT " + processes[randomInstance])
    time.sleep(1)
