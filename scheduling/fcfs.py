import numpy as np
from core.alogrithm import Choose


class FirstComeFirstServed(Choose):
    def __init__(self):
        pass

    def __call__(self, cluster):
        candidate_tasks = cluster.tasks_which_has_waiting_instance
        if len(candidate_tasks) > 0:
            task = sorted(candidate_tasks, key=lambda t: t.job.job_config.submit_time)[0]
            print(f"choose task {task.id} whose submit time is {task.job.job_config.submit_time}")
            return task
        return None
