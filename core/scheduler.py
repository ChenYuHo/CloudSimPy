import sys
class Scheduler(object):
    def __init__(self, env, choose, placement):
        self.env = env
        self.choose = choose
        self.placement = placement
        self.simulation = None
        self.cluster = None
        self.destroyed = False
        self.valid_pairs = {}

    def attach(self, simulation):
        self.simulation = simulation
        self.cluster = simulation.cluster

    def make_decision(self):
        while True:
            task = self.choose(self.cluster)
            if task is None:
                break
            print(f'[{self.env.now}]\tscheduler has chosen task {task.id}, which requires {task.task_config.gpu} to execute')
            machine_gpus = self.placement(task, self.cluster, self.env.now) # mid: gpus allocated
            if machine_gpus is None:
                print(f'[{self.env.now}]\tfailed to get a valid placement for task {task.id}', file=sys.stderr)
                break
            print(f'[{self.env.now}]\tplacement determined for task {task.id}: {machine_gpus}')
            task.start_task_instance([(self.cluster.machines[mid], machine_gpus[mid]) for mid in machine_gpus.keys()])

    def run(self):
        while not self.simulation.finished:
            self.make_decision()
            yield self.env.timeout(1)
        self.destroyed = True
