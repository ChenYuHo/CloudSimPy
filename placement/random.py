import numpy as np
from core.alogrithm import Placement
from collections import Counter


class Random(Placement):
    def __init__(self, threshold=0.8):
        self.threshold = threshold

    def __call__(self, task, cluster, clock):
        machines = cluster.machines
        placement_machines = []
        gpus = [m.gpu for m in machines]
        for _ in range(task.task_config.gpu):
            possible_machines = [m for i, m in enumerate(machines) if gpus[i] > 0]
            if len(possible_machines) == 0:
                return None
            random_index = np.random.randint(0, len(possible_machines))
            placement_machines.append(possible_machines[random_index].id)
            gpus[random_index] -= 1
        return Counter(placement_machines)
