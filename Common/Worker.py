#!/usr/bin/env python
# -*- coding: utf-8 -*-

import subprocess
import signal
import time

from multiprocessing import Process, Queue, JoinableQueue
from Common.Constant import *


# worker to execute rbd export task
class Worker(Process):
    def __init__(self, log, task_queue, finish_queue, rest_time, stop_task=None):

        Process.__init__(self)
        self.log = log
        self.task_queue = task_queue
        self.finish_queue = finish_queue

        self.rest_time = rest_time
        self.stop_task = stop_task
        self.stage = None
        self.task_get_count = 0
        self.task_done_count = 0

        self.status = READY

        self.log.info("%s initialized, rest time %s seconds, status %s." %
                      (self.name, self.rest_time, self.status))

    def run(self):
        pid = str(self.pid)

        while True:
            #self.log.set_stage(self.stage)

            try:
                self.status = WAIT
                self.log.debug("%s (pid=%s) is waiting for new task." % (self.name, pid))
                task = self.task_queue.get()

                if task is self.stop_task:
                    self.status = STOP
                    self.task_queue.task_done()
                    break

                self.status = RUN
                self.task_get_count += 1
                self.log.debug("%s is executing task. name = %s" % (self.name, task))
                result = task.execute(self.name)

                self.log.debug("%s completed task. name = %s" %(self.name, task))
                self.task_queue.task_done()
                self.finish_queue.put(task)

                self.task_done_count += 1
                self.status = REST
                time.sleep(self.rest_time)

            except Exception as e:
                self.log.error("%s could not execute task. name = %s, %s" %(self.name, task, e))
                self.finish_queue.put(task)
                # move on next task...
                continue

        self.log.info("%s stopped running." % (self.name))
        return True

    def set_rest_time(self, rest_time):
        self.rest_time = rest_time
