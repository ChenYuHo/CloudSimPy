class TaskInstanceConfig(object):
    def __init__(self, task_config):
        self.cpu = task_config.cpu
        self.gpu = task_config.gpu
        self.memory = task_config.memory
        self.disk = task_config.disk
        self.duration = task_config.duration
        self.iterations = task_config.iterations
        self.step_time = task_config.step_time


class TaskConfig(object):
    def __init__(self, task_index, instances_number, cpu, memory, disk, duration, gpu, iterations, parent_indices=None):
        self.task_index = task_index
        self.instances_number = instances_number
        self.cpu = cpu
        self.memory = memory
        self.disk = disk
        self.duration = duration
        self.gpu = gpu
        self.iterations = iterations
        self.step_time = float(duration)/float(iterations)
        # print(duration, iterations, self.step_time)
        self.parent_indices = parent_indices


class JobConfig(object):
    def __init__(self, idx, submit_time, task_configs):
        self.submit_time = submit_time
        self.task_configs = task_configs
        self.id = idx
