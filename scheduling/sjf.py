import numpy as np
from core.alogrithm import Choose


class ShortestJobFirst(Choose):
    def __init__(self):
        pass

    def __call__(self, cluster):
        candidate_tasks = cluster.tasks_which_has_waiting_instance
        if len(candidate_tasks) > 0:
            task = sorted(candidate_tasks, key=lambda t: (sum(tc.duration for tc in t.job.job_config.task_configs),
                                                          t.job.job_config.submit_time))[0]
            print(f"choose task {task.id} whose duration is {list(tc.duration for tc in task.job.job_config.task_configs)}")
            return task
        return None
