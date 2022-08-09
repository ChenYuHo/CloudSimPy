from core.config import *
import simpy
import sys

class Task(object):
    def __init__(self, env, job, task_config):
        self.env = env
        self.job = job
        self.task_index = task_config.task_index
        self.task_config = task_config
        self._ready = False
        self._parents = None

        self.task_instances = []
        task_instance_config = TaskInstanceConfig(task_config)
        for task_instance_index in range(int(self.task_config.instances_number)):
            self.task_instances.append(TaskInstance(self.env, self, task_instance_index, task_instance_config))
        self.next_instance_pointer = 0

    @property
    def id(self):
        return str(self.job.id) + '-' + str(self.task_index)

    @property
    def parents(self):
        if self._parents is None:
            if self.task_config.parent_indices is None:
                raise ValueError("Task_config's parent_indices should not be None.")
            self._parents = []
            for parent_index in self.task_config.parent_indices:
                self._parents.append(self.job.tasks_map[parent_index])
        return self._parents

    @property
    def ready(self):  # all prerequisite tasks are done
        if not self._ready:
            for p in self.parents:
                if not p.finished:
                    return False
            self._ready = True
        return self._ready

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

    # the most heavy
    def start_task_instance(self, machines):
        self.task_instances[self.next_instance_pointer].schedule(machines)
        self.next_instance_pointer += 1

    @property
    def started(self):
        for task_instance in self.task_instances:
            if task_instance.started:
                return True
        return False

    @property
    def waiting_task_instances_number(self):
        return self.task_config.instances_number - self.next_instance_pointer

    @property
    def has_waiting_task_instances(self):
        return self.task_config.instances_number > self.next_instance_pointer

    @property
    def finished(self):
        """
        A task is finished only if it has no waiting task instances and no running task instances.
        :return: bool
        """
        if self.has_waiting_task_instances:
            return False
        if len(self.running_task_instances) != 0:
            return False
        return True

    @property
    def started_timestamp(self):
        t = None
        for task_instance in self.task_instances:
            if task_instance.started_timestamp is not None:
                if (t is None) or (t > task_instance.started_timestamp):
                    t = task_instance.started_timestamp
        return t

    @property
    def finished_timestamp(self):
        if not self.finished:
            return None
        t = None
        for task_instance in self.task_instances:
            if (t is None) or (t < task_instance.finished_timestamp):
                t = task_instance.finished_timestamp
        return t


class Job(object):
    task_cls = Task

    def __init__(self, env, job_config):
        self.env = env
        self.job_config = job_config
        self.id = job_config.id
        self.submitted_time = None

        self.tasks_map = {}
        for task_config in job_config.task_configs:
            task_index = task_config.task_index
            self.tasks_map[task_index] = Job.task_cls(env, self, task_config)

    @property
    def tasks(self):
        return self.tasks_map.values()

    @property
    def unfinished_tasks(self):
        ls = []
        for task in self.tasks:
            if not task.finished:
                ls.append(task)
        return ls

    @property
    def ready_unfinished_tasks(self):
        ls = []
        for task in self.tasks:
            if not task.finished and task.ready:
                ls.append(task)
        return ls

    @property
    def tasks_which_has_waiting_instance(self):
        ls = []
        for task in self.tasks:
            if task.has_waiting_task_instances:
                ls.append(task)
        return ls

    @property
    def ready_tasks_which_has_waiting_instance(self):
        ls = []
        for task in self.tasks:
            if task.has_waiting_task_instances and task.ready:
                ls.append(task)
        return ls

    @property
    def running_tasks(self):
        ls = []
        for task in self.tasks:
            if task.started and not task.finished:
                ls.append(task)
        return ls

    @property
    def finished_tasks(self):
        ls = []
        for task in self.tasks:
            if task.finished:
                ls.append(task)
        return ls

    @property
    def started(self):
        for task in self.tasks:
            if task.started:
                return True
        return False

    @property
    def finished(self):
        for task in self.tasks:
            if not task.finished:
                return False
        return True

    @property
    def started_timestamp(self):
        t = None
        for task in self.tasks:
            if task.started_timestamp is not None:
                if (t is None) or (t > task.started_timestamp):
                    t = task.started_timestamp
        return t

    @property
    def finished_timestamp(self):
        if not self.finished:
            return None
        t = None
        for task in self.tasks:
            if (t is None) or (t < task.finished_timestamp):
                t = task.finished_timestamp
        return t


class TaskInstance(object):
    def __init__(self, env, task, task_instance_index, task_instance_config):
        self.env = env
        self.task = task
        self.task_instance_index = task_instance_index
        self.config = task_instance_config
        self.cpu = task_instance_config.cpu
        self.gpu = task_instance_config.gpu
        self.memory = task_instance_config.memory
        self.disk = task_instance_config.disk
        self.duration = task_instance_config.duration
        self.iterations = task_instance_config.iterations
        self.step_time = task_instance_config.step_time

        self.machines = None
        self.tors = None
        self.process = None
        self.new = True
        self.timer = None

        self.started = False
        self.finished = False
        self.started_timestamp = None
        self.finished_timestamp = None

    @property
    def id(self):
        return str(self.task.id) + '-' + str(self.task_instance_index)

    def do_work(self, same_tor=False):
        # self.cluster.waiting_tasks.remove(self)
        # self.cluster.running_tasks.append(self)
        # self.machine.run(self)
        for i in range(self.iterations):
            step_time = self.step_time * (max(len(tor.task_instances) for tor in self.tors) if len(self.tors) > 1 else 1)
            print(
                f'[{self.env.now}]\ttask {self.id} spans through tors {[t.id for t in self.tors]} running step {i}/{self.iterations} for {step_time}')
            for t in self.tors:
                print(f'tor {t.id} is running {t.task_instances}')
            yield self.env.timeout(step_time)

        # print(f'[{self.env.now}]\ttask {self.task.id} starts running')
        # [for s in self.switches]
        # yield self.env.timeout(self.duration)
        # print(f'[{self.env.now}]\ttask {self.task.id} finishes')

        self.finished = True
        self.finished_timestamp = self.env.now

        for machine, gpu in self.machines:
            machine.stop_task_instance(self, gpu)
            print(
                f'[{self.env.now}]\ttask {self.id} done, machine {machine.id} releases {gpu} gpu and has {machine.gpu} gpus now')
        print(f'[{self.env.now}]\ttask {self.id} done', file=sys.stderr)

    def schedule(self, machines):
        self.started = True
        self.started_timestamp = self.env.now

        self.machines = machines
        tors_spanned = set()
        for machine, gpu in machines:
            print(
                f'[{self.env.now}]\tmachine {machine.id} which has {machine.gpu} gpus is launching task {self.id} with {gpu} gpu')
            if machine.tor not in tors_spanned:
                tors_spanned.add(machine.tor)
            machine.run_task_instance(self, gpu)
        self.tors = tors_spanned
        self.process = self.env.process(self.do_work())

        # def callback():
        #     self.finished = True
        #     self.finished_timestamp = self.env.now
        #     for m, g in self.machines:
        #         m.stop_task_instance(self, g)
        #         print(
        #             f'[{self.env.now}]\ttask {self.id} done, machine {m.id} releases {g} gpu and has {m.gpu} gpus now')
        # self.timer = Timer(self.env, self.duration, callback)
        # self.timer.start()


class Timer(object):
    def __init__(self, env, delay, callback):
        self.env = env
        self.delay = delay
        self.action = None
        self.callback = callback
        self.running = False
        self.canceled = False

    def wait(self):
        """
        Calls a callback after time has elapsed.
        """
        try:
            yield self.env.timeout(self.delay)
            self.callback()
            self.running = False
        except simpy.Interrupt:
            self.canceled = True
            self.running = False

    def start(self):
        """
        Starts the timer
        """
        if not self.running:
            self.running = True
            self.action = self.env.process(self.wait())

    def stop(self):
        """
        Stops the timer
        """
        if self.running:
            self.action.interrupt()
            self.action = None

    def reset(self):
        """
        Interrupts the current timer and restarts.
        """
        self.stop()
        self.start()
