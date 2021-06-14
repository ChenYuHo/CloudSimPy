from core.machine import Machine
from core.switch import Switch


class Cluster(object):
    def __init__(self, num_machines_per_tor=8, num_gpus_per_machine=8):
        self.machines = []
        # self.switch = Switch()
        self.tors = []  # top-of-rack switch
        self.jobs = []
        self.num_machines_per_tor = num_machines_per_tor
        self.num_gpus_per_machine = num_gpus_per_machine

    @property
    def unfinished_jobs(self):
        ls = []
        for job in self.jobs:
            if not job.finished:
                ls.append(job)
        return ls

    @property
    def unfinished_tasks(self):
        ls = []
        for job in self.jobs:
            ls.extend(job.unfinished_tasks)
        return ls

    @property
    def ready_unfinished_tasks(self):
        ls = []
        for job in self.jobs:
            ls.extend(job.ready_unfinished_tasks)
        return ls

    @property
    def tasks_which_has_waiting_instance(self):
        ls = []
        for job in self.jobs:
            ls.extend(job.tasks_which_has_waiting_instance)
        return ls

    @property
    def ready_tasks_which_has_waiting_instance(self):
        ls = []
        for job in self.jobs:
            ls.extend(job.ready_tasks_which_has_waiting_instance)
        return ls

    @property
    def finished_jobs(self):
        ls = []
        for job in self.jobs:
            if job.finished:
                ls.append(job)
        return ls

    @property
    def finished_tasks(self):
        ls = []
        for job in self.jobs:
            ls.extend(job.finished_tasks)
        return ls

    @property
    def running_task_instances(self):
        task_instances = []
        for machine in self.machines:
            task_instances.extend(machine.running_task_instances)
        return task_instances

    def add_machines(self, machine_configs):
        # len(machine_configs) //
        num_tors = len(machine_configs) // self.num_machines_per_tor
        for i in range(num_tors):
            self.tors.append(Switch(self))
        for i, machine_config in enumerate(machine_configs):
            machine = Machine(machine_config)
            self.machines.append(machine)
            self.tors[i % num_tors].machines.append(machine)
            machine.attach(self)
            machine.attach_to_tor(self.tors[i % num_tors])

    def add_job(self, job):
        self.jobs.append(job)

    @property
    def cpu(self):
        return sum([machine.cpu for machine in self.machines])

    @property
    def memory(self):
        return sum([machine.memory for machine in self.machines])

    @property
    def disk(self):
        return sum([machine.disk for


                    machine in self.machines])

    @property
    def cpu_capacity(self):
        return sum([machine.cpu_capacity for machine in self.machines])

    @property
    def memory_capacity(self):
        return sum([machine.memory_capacity for machine in self.machines])

    @property
    def disk_capacity(self):
        return sum([machine.disk_capacity for machine in self.machines])

    @property
    def state(self):
        return {
            'arrived_jobs': len(self.jobs),
            'unfinished_jobs': len(self.unfinished_jobs),
            'finished_jobs': len(self.finished_jobs),
            'unfinished_tasks': len(self.unfinished_tasks),
            'finished_tasks': len(self.finished_tasks),
            'running_task_instances': len(self.running_task_instances),
            'machine_states': [machine.state for machine in self.machines],
            'cpu': self.cpu / self.cpu_capacity,
            'memory': self.memory / self.memory_capacity,
            'disk': self.disk / self.disk_capacity,
        }
