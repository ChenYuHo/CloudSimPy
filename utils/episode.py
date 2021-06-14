import simpy
from core.cluster import Cluster
from core.scheduler import Scheduler
from core.broker import Broker
from core.simulation import Simulation


class Episode(object):
    def __init__(self, machine_configs, task_configs, choose, placement, event_file=None, num_machines_per_tor=8, gpus_per_machine=8):
        self.env = simpy.Environment()
        cluster = Cluster(num_machines_per_tor, gpus_per_machine)
        cluster.add_machines(machine_configs)

        task_broker = Broker(self.env, task_configs)

        scheduler = Scheduler(self.env, choose, placement)

        self.simulation = Simulation(self.env, cluster, task_broker, scheduler, event_file)

    def run(self):
        self.simulation.run()
        self.env.run()
