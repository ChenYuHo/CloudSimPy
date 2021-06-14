from core.machine import Machine


class Switch(object):
    idx = 0

    def __init__(self, cluster, bandwidth=100):
        self.machines = []
        self.cluster = cluster
        self.task_instances = set()
        self.id = Switch.idx
        self.bandwidth = bandwidth
        Switch.idx += 1

    def aggregate(self, size):
        # return aggregation time for given size
        bw = self.bandwidth / len(self.task_instances)

    @property
    def bw(self):
        return self.bandwidth / len(self.task_instances) if len(self.task_instances) > 1 else self.bandwidth

    def add_task_instance(self, task_instance):
        self.task_instances.add(task_instance.id)

    def remove_task_instance(self, task_instance):
        if task_instance.id in self.task_instances:
            self.task_instances.remove(task_instance.id)

    # @property
    # def unfinished_jobs(self):
    #     ls = []
    #     for job in self.jobs:
    #         if not job.finished:
    #             ls.append(job)
    #     return ls
    #
    # @property
    # def unfinished_tasks(self):
    #     ls = []
    #     for job in self.jobs:
    #         ls.extend(job.unfinished_tasks)
    #     return ls

    # @property
    # def ready_unfinished_tasks(self):
    #     ls = []
    #     for job in self.jobs:
    #         ls.extend(job.ready_unfinished_tasks)
    #     return ls
    #
    # @property
    # def tasks_which_has_waiting_instance(self):
    #     ls = []
    #     for job in self.jobs:
    #         ls.extend(job.tasks_which_has_waiting_instance)
    #     return ls

    # @property
    # def ready_tasks_which_has_waiting_instance(self):
    #     ls = []
    #     for job in self.jobs:
    #         ls.extend(job.ready_tasks_which_has_waiting_instance)
    #     return ls
    #
    # @property
    # def finished_jobs(self):
    #     ls = []
    #     for job in self.jobs:
    #         if job.finished:
    #             ls.append(job)
    #     return ls

    # @property
    # def finished_tasks(self):
    #     ls = []
    #     for job in self.jobs:
    #         ls.extend(job.finished_tasks)
    #     return ls
    #
    # @property
    # def running_task_instances(self):
    #     task_instances = []
    #     for machine in self.machines:
    #         task_instances.extend(machine.running_task_instances)
    #     return task_instances

    # def add_job(self, job):
    #     self.jobs.append(job)
    #
    # @property
    # def cpu(self):
    #     return sum([machine.cpu for machine in self.machines])
    #
    # @property
    # def memory(self):
    #     return sum([machine.memory for machine in self.machines])
    #
    # @property
    # def disk(self):
    #     return sum([machine.disk for machine in self.machines])
    #
    # @property
    # def cpu_capacity(self):
    #     return sum([machine.cpu_capacity for machine in self.machines])
    #
    # @property
    # def memory_capacity(self):
    #     return sum([machine.memory_capacity for machine in self.machines])
    #
    # @property
    # def disk_capacity(self):
    #     return sum([machine.disk_capacity for machine in self.machines])
    #
    # @property
    # def state(self):
    #     return {
    #         'arrived_jobs': len(self.jobs),
    #         'unfinished_jobs': len(self.unfinished_jobs),
    #         'finished_jobs': len(self.finished_jobs),
    #         'unfinished_tasks': len(self.unfinished_tasks),
    #         'finished_tasks': len(self.finished_tasks),
    #         'running_task_instances': len(self.running_task_instances),
    #         'machine_states': [machine.state for machine in self.machines],
    #         'cpu': self.cpu / self.cpu_capacity,
    #         'memory': self.memory / self.memory_capacity,
    #         'disk': self.disk / self.disk_capacity,
    #     }
