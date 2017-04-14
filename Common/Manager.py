#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import os
import sys
import thread

from threading import Thread
from multiprocessing import Queue, JoinableQueue

from Common.BaseTask import BaseTask
from Common.Worker import Worker
from Common.Monitor import Monitor


# manage rbd export tasks
class Manager(Thread):
    def __init__(self, log, worker_count=1, rest_time=2):
        self.log = log
        self.worker_count = int(worker_count)
        self.rest_time = rest_time

        self.workers = []
        self.workers_pid = {}
        self.workers_status = {}

        self.task_queue = JoinableQueue()
        self.finish_queue = Queue()

        self.monitor_pid_queue = Queue()

        self.task_add_count = 0
        self.task_finish_count = 0

        self.stop_task = None   # tell worker to stop

        self.log.info("worker manager initialized, set %s workers." % worker_count)

    def initialize_monitor(self):
        self.log.info("initializing worker monitoring.")

        worker_pid = []
        for name, pid in self.workers_pid.iteritems():
            print("monitoring worker %s" % name)
            worker_pid.append(pid)

        #monitor = Monitor(self.monitor_pid_queue, worker_pid)

        self.log.info("monitoring workers.")

        #thread.start_new_thread(self.monitoring, (self.monitor_pid_queue))

        return True

    def monitoring(self, monitor_pid_queue):
        count = 100
        while count < 1:
            monitor_pid_queue.put(self._get_cmd_pid())
            time.sleep(1)

    def _get_cmd_pid(self):

        cmd_pid = []
        for worker in self.workers:
            cmd_pid.append(worker.cmd_pid)
        return cmd_pid

    def _check_worker(self):
        # check worker stopped or not...
        for worker in self.workers:
            self.workers_status[worker.name] = worker.status

    def run_worker(self):
        try:
            self.log.debug("start runing %s workers." % self.worker_count)

            # todo: change to create new logger for worker processes.
            # ...

            workers = [ Worker(self.log, self.task_queue, self.finish_queue, self.rest_time, self.stop_task)
                        for i in xrange(self.worker_count) ]

            for worker in workers:
                worker.start()
                worker_name = worker.name
                worker_pid = worker.pid
                self.workers_pid[worker_name] = worker_pid

            self.workers = workers
            self._check_worker()

            return True
        except Exception as e:
            self.log.error("unable to run worker. %s" % e)
            return False

    # call after all tasks are done
    def stop_worker(self, count=0):
        try:
            # check number of worker to stop
            if count == 0 or count > self.worker_count:
                self.log.debug("stop all workers")
                stop_count = self.worker_count
            else:
                self.log.debug("stop %s workers" % count)
                stop_count = int(count)

            for count in range(0, stop_count):
                self.log.debug("sent stop singal to workers. %s." % count)
                self.task_queue.put(self.stop_task)

            self._check_worker()
        except Exception as e:
            self.log.error("unable to stop worker. %s" % e)
            return False

    def add_task(self, task, method_name=None):
        if method_name is not None:
            # the task is a function, packet to task class.
            self.log.info("pack task into bask task.")
            mgr_task = BaseTask(task, method_name)
        else:
            mgr_task = task

        self.task_queue.put(mgr_task)
        self.task_add_count += 1

        self.log.debug("added new task. name = %s" % task.name)

    def get_workers_status(self):
        self._check_worker()
        return self.workers_status

    def get_worker_pid(self):
        '''
            only when workers are running, the pid is updated.
        '''
        return self.workers_pid

    def get_finished_task(self):
        return self.finish_queue.get()
