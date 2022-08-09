from core.machine import MachineConfig
from placement.random import Random
from placement.yarn import YARN
from scheduling.sjf import ShortestJobFirst
from scheduling.fcfs import FirstComeFirstServed
from utils.csv_reader import CSVReader
from utils.tools import average_completion, average_slowdown
from utils.episode import Episode
import sys
import time
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-p', '--placement', choices=['random', 'yarn'], default='random', help='placement algorithm')
parser.add_argument('-s', '--schedule', choices=['sjf', 'fcfs'], default='fcfs', help='scheduling policy')
parser.add_argument('-n', '--num_tors', type=int, default=1, help='number of Top of Rack switches')
parser.add_argument('-m', '--machines_per_tor', type=int, default=8, help='machines connected to a ToR')
parser.add_argument('-g', '--gpus_per_machine', type=int, default=1, help='GPUs per machine')
parser.add_argument('-j', '--job_file', default='jobs.csv')
parser.add_argument('--job_length', type=int, default=999999, help='Use job_length')
parser.add_argument('--log_dir', default='.')
args = parser.parse_args()

placement = None
scheduling = None

if args.placement == 'random':
    placement = Random()
elif args.placement == 'yarn':
    placement = YARN()

if args.schedule == 'sjf':
    scheduling = ShortestJobFirst()
elif args.schedule == 'fcfs':
    scheduling = FirstComeFirstServed()

print(f'{args.placement} placement, {args.schedule} scheduling')

# np.random.seed(41)
# ************************ Parameters Setting Start ************************
machine_config_csv = ''
# ************************ Parameters Setting End ************************


machine_configs = [MachineConfig(64, 1, 1, args.gpus_per_machine) for i in range(args.num_tors * args.machines_per_tor)]
csv_reader = CSVReader(args.job_file)
jobs_configs = csv_reader.generate(0, args.job_length)

# print([(tc.task_index, tc.instances_number, tc.cpu, tc.gpu, tc.memory, tc.disk, tc.duration, tc.parent_indices) for
# tc in jobs_configs[0].task_configs])
tic = time.time()
episode = Episode(machine_configs, jobs_configs, scheduling, placement, None, args.machines_per_tor, args.gpus_per_machine)
episode.run()
print(episode.env.now, time.time() - tic, average_completion(episode), average_slowdown(episode))

completion_time = 0
number_task = 0
with open(f'{args.log_dir}/{args.placement}_{args.schedule}.log', 'w') as f:
    f.write('jid,jct,turnaround_time,pending_time\n')
    for job in episode.simulation.cluster.jobs:
        for task in job.tasks:
            print(f'{task.id},{task.finished_timestamp - task.started_timestamp},{job.finished_timestamp - job.submitted_time},{task.started_timestamp-job.submitted_time}')
            f.write(f'{task.id},{task.finished_timestamp - task.started_timestamp},{job.finished_timestamp - job.submitted_time},{task.started_timestamp-job.submitted_time}\n')
