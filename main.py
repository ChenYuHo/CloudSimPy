import time

from core.machine import MachineConfig
from placement.random import Random
from placement.yarn import YARN
from scheduling.sjf import ShortestJobFirst
from scheduling.fcfs import FirstComeFirstServed

from utils.csv_reader import CSVReader
from utils.tools import average_completion, average_slowdown
from utils.episode import Episode

import sys

args = sys.argv
placement_algo = args[1]
scheduling_algo = args[2]
num_tors = int(args[3])
machines_per_tor = int(args[4])
gpus_per_machine = int(args[5])
placement = None
scheduling = None

if placement_algo == 'random':
    placement = Random()
elif placement_algo == 'yarn':
    placement = YARN()

if scheduling_algo == 'sjf':
    scheduling = ShortestJobFirst()
elif scheduling_algo == 'fcfs':
    scheduling = FirstComeFirstServed()

print(f'{placement_algo} placement, {scheduling_algo} scheduling')

# np.random.seed(41)
# ************************ Parameters Setting Start ************************
jobs_len = 3000
# jobs_csv = 'jobs_files/jobs2.csv'
jobs_csv = '103959.csv'
machine_config_csv = ''
# ************************ Parameters Setting End ************************


machine_configs = [MachineConfig(64, 1, 1, gpus_per_machine) for i in range(num_tors * machines_per_tor)]
csv_reader = CSVReader(jobs_csv)
jobs_configs = csv_reader.generate(0, jobs_len)

# print([(tc.task_index, tc.instances_number, tc.cpu, tc.gpu, tc.memory, tc.disk, tc.duration, tc.parent_indices) for
# tc in jobs_configs[0].task_configs])
tic = time.time()
episode = Episode(machine_configs, jobs_configs, scheduling, placement, None, machines_per_tor, gpus_per_machine)
episode.run()
print(episode.env.now, time.time() - tic, average_completion(episode), average_slowdown(episode))

completion_time = 0
number_task = 0
with open(f'103959_logs_2/{placement_algo}_{scheduling_algo}.log', 'w') as f:
    f.write('jid,jct,turnaround_time,pending_time\n')
    for job in episode.simulation.cluster.jobs:
        for task in job.tasks:
            print(f'{task.id},{task.finished_timestamp - task.started_timestamp},{job.finished_timestamp - job.submitted_time},{task.started_timestamp-job.submitted_time}')
            f.write(f'{task.id},{task.finished_timestamp - task.started_timestamp},{job.finished_timestamp - job.submitted_time},{task.started_timestamp-job.submitted_time}\n')


# tic = time.time()
# scheduling = FirstFitAlgorithm()
# episode = Episode(machine_configs, jobs_configs, scheduling, None)
# episode.run()
# print(episode.env.now, time.time() - tic, average_completion(episode), average_slowdown(episode))

# tic = time.time()
# scheduling = Tetris()
# episode = Episode(machine_configs, jobs_configs, scheduling, None)
# episode.run()
# print(episode.env.now, time.time() - tic, average_completion(episode), average_slowdown(episode))

