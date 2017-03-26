#!/usr/bin/env python
# -*- coding: utf-8 -*-

from multiprocessing import Queue, JoinableQueue

from Common.Worker import Worker


# manage rbd export tasks
class Manager(object):
    def __init__(self, log, worker_count=1, rest_time=2):
        self.log = log
        self.worker_count = int(worker_count)
        self.rest_time = rest_time

        self.workers = []
        self.workers_pid = {}
        self.workers_status = {}

        self.task_queue = JoinableQueue()
        self.finish_queue = Queue()

        self.task_add_count = 0
        self.task_finish_count = 0

        self.stop_task = None   # tell worker to stop

        self.log.info("worker manager initialized, set %s workers." % worker_count)

    def _check_worker(self):
        # check worker stopped or not...
        for worker in self.workers:
            self.workers_status[worker.name] = worker.status

    def run_worker(self):
        try:
            self.log.debug("start runing %s workers." % self.worker_count)

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
    def stop_worker(self, force=False):
        self.log.debug("stop all workers")

        for count in range(0, self.worker_count):
            ##self.log.debug("add exit task %s." % count)
            self.task_queue.put(None)

        if force:
            for worker in self.workers:
                worker.terminate()

        self._check_worker()

    def add_task(self, task):
        self.task_queue.put(task)
        self.task_add_count += 1
        self.log.debug("add new task. name = %s" % task.name)

    def get_workers_status(self):
        self._check_worker()
        return self.workers_status

    def get_worker_pid():
        '''
        only when workers are running, the pid is updated.
        '''
        return self.workers_pid

    def get_result_task(self):
        return self.finish_queue.get()
