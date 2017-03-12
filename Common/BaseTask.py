#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time, subprocess

from Common.Constant import *


class BaseTask(object):
    def __init__(self):
        #self.a = 10
        #self.b = 10

        self.init_timestamp = time.time()
        self.start_timestamp = 0
        self.complete_timestamp = 0
        self.elapsed_time = 0

        self.cmd = None
        self.id = None
        self.name = "BaseTask"
        self.worker_name = None
        self.task_status = INITIAL
        self.return_code = None

    def __call__(self):
        time.sleep(1)
        return self.name
        #return "%s * %s = %s" % (self.a, self.b, self.a * self.b)

    def __str__(self):
        return self.name
        #return '%s * %s = %s' % (self.a, self.b, self.a * self.b)

    def _exec_cmd(self, cmd):
        try:
            self.start_timestamp = time.time()
            self.cmd = cmd

            self.task_status = EXECUTE
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
            result = p.communicate()[0], p.returncode

            self.elapsed_time = self._get_elapsed_time_()
            return result
        except Exception as e:
            self.task_status = ERROR

    def _get_elapsed_time_(self):
        self.complete_timestamp = time.time()
        if self.start_timestamp == 0 or self.complete_timestamp == 0:
            return False
        else:
            if self.start_timestamp > self.complete_timestamp:
                return False
            else:
                return self.complete_timestamp - self.start_timestamp


    def _verify_result(self, result):
        #print self.start_timestamp, self.complete_timestamp
        if self.elapsed_time is not False:
            self.task_status = COMPLETE
        if result[1] is not 0:
            self.task_status = ERROR
        #print("%s, %s" % (self.cmd, result))

    def execute(self):
        result = self.__call__()
        self._get_elapsed_time_()

        return True
