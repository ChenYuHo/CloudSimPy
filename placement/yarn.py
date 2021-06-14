import numpy as np
from core.alogrithm import Placement
from collections import Counter


def try_cross_node_alloc(task, cluster):
    need_gpu = task.task_config.gpu
    num_full_nodes = need_gpu // cluster.num_gpus_per_machine
    print(f'need {num_full_nodes} * {cluster.num_gpus_per_machine} = {need_gpu}')
    last_node_gpu = need_gpu % cluster.num_gpus_per_machine
    for tor in cluster.tors:
        last_node = None
        full_node_list = []
        full_nodes = 0
        for node in tor.machines:
            if node.gpu == cluster.num_gpus_per_machine:
                # get idle node
                full_node_list.extend([node.id] * node.gpu)
                full_nodes += 1
                if full_nodes == num_full_nodes:
                    # enough full nodes
                    break
        if len(full_node_list) < num_full_nodes:
            continue

        if last_node_gpu:
            for node in tor.machines:
                if node.id not in full_node_list and node.gpu >= last_node_gpu:
                    last_node = node.id
                    full_node_list.extend([last_node] * last_node_gpu)
                    break
            if last_node is None:
                continue
        return full_node_list


def try_single_node_alloc(task, cluster):
    for node in cluster.machines:
        if node.gpu >= task.task_config.gpu:
            return [node.id] * task.task_config.gpu
    return None


class YARN(Placement):
    def __init__(self):
        pass

    def __call__(self, task, cluster, clock):
        need_gpu = task.task_config.gpu
        single_node = False
        if need_gpu > cluster.num_gpus_per_machine:
            ret = try_cross_node_alloc(task, cluster)
        else:
            ret = try_single_node_alloc(task, cluster)
        if ret is None:
            return None
        return Counter(ret)
