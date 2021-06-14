from enum import Enum


class MachineConfig(object):
    idx = 0

    def __init__(self, cpu_capacity, memory_capacity, disk_capacity, gpu_capacity, cpu=None, memory=None, disk=None, gpu=None):
        self.cpu_capacity = cpu_capacity
        self.memory_capacity = memory_capacity
        self.disk_capacity = disk_capacity
        self.gpu_capacity = gpu_capacity

        self.cpu = cpu_capacity if cpu is None else cpu
        self.memory = memory_capacity if memory is None else memory
        self.disk = disk_capacity if disk is None else disk
        self.gpu = gpu_capacity if gpu is None else gpu

        self.id = MachineConfig.idx
        MachineConfig.idx += 1


class MachineDoor(Enum):
    TASK_IN = 0
    TASK_OUT = 1
    NULL = 3


class Machine(object):
    def __init__(self, machine_config):
        self.id = machine_config.id
        self.cpu_capacity = machine_config.cpu_capacity
        self.memory_capacity = machine_config.memory_capacity
        self.disk_capacity = machine_config.disk_capacity
        self.gpu_capacity = machine_config.gpu_capacity
        self.cpu = machine_config.cpu
        self.memory = machine_config.memory
        self.disk = machine_config.disk
        self.gpu = machine_config.gpu

        self.cluster = None
        self.tor = None
        self.task_instances = []
        self.machine_door = MachineDoor.NULL

    def run_task_instance(self, task_instance, gpu=None):
        self.cpu -= task_instance.cpu
        self.memory -= task_instance.memory
        self.disk -= task_instance.disk
        self.task_instances.append(task_instance)
        self.gpu -= task_instance.gpu if gpu is None else gpu
        self.machine_door = MachineDoor.TASK_IN
        self.tor.add_task_instance(task_instance)

    def stop_task_instance(self, task_instance, gpu=None):
        self.cpu += task_instance.cpu
        self.memory += task_instance.memory
        self.disk += task_instance.disk
        self.gpu += task_instance.gpu if gpu is None else gpu
        self.machine_door = MachineDoor.TASK_OUT
        self.tor.remove_task_instance(task_instance)

    @property
    def running_task_instances(self):
        ls = []
        for task_instance in self.task_instances:
            if task_instance.started and not task_instance.finished:
                ls.append(task_instance)
        return ls

    @property
    def finished_task_instances(self):
        ls = []
        for task_instance in self.task_instances:
            if task_instance.finished:
                ls.append(task_instance)
        return ls

    def attach(self, cluster):
        self.cluster = cluster

    def attach_to_tor(self, switch):
        self.tor = switch

    def accommodate(self, task):
        return self.cpu >= task.task_config.cpu and \
               self.memory >= task.task_config.memory and \
               self.disk >= task.task_config.disk and \
               self.gpu >= task.task_config.gpu

    @property
    def feature(self):
        return [self.cpu, self.memory, self.disk]

    @property
    def capacity(self):
        return [self.cpu_capacity, self.memory_capacity, self.disk_capacity]

    @property
    def state(self):
        return {
            'id': self.id,
            'cpu_capacity': self.cpu_capacity,
            'memory_capacity': self.memory_capacity,
            'disk_capacity': self.disk_capacity,
            'cpu': self.cpu / self.cpu_capacity,
            'memory': self.memory / self.memory_capacity,
            'disk': self.disk / self.disk_capacity,
            'running_task_instances': len(self.running_task_instances),
            'finished_task_instances': len(self.finished_task_instances)
        }

    def __eq__(self, other):
        return isinstance(other, Machine) and other.id == self.id
