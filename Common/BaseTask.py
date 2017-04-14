#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time, datetime, subprocess, os, sys, traceback


from Common.Constant import *


class BaseTask(object):
    def __init__(self, exec_class=None, method_name=''):
        #self.a = 10
        #self.b = 10

        self.exec_class = exec_class
        self.method_name = method_name

        self.init_timestamp = time.time()
        self.start_timestamp = 0
        self.complete_timestamp = 0
        self.elapsed_time = 0

        self.cmd = None
        self.name = "BaseTask"
        self.worker_name = None
        self.task_status = INITIAL
        self.return_code = None
        self.error = None

        self.output = None
        self.result = dict()
        self.cmd_pid = int()

    def __call__(self):
        time.sleep(1)
        return self.name
        #return "%s * %s = %s" % (self.a, self.b, self.a * self.b)

    def __str__(self):
        return self.name
        #return '%s * %s = %s' % (self.a, self.b, self.a * self.b)

    def _set_cmd_pid(self, ppid):
        try:
            cmd = "ps -elf | grep %s | grep -v ' grep\| /bin/sh' | awk '{print $4}'" % ppid
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
            result = p.communicate()[0]
            if result != '':
                self.cmd_pid = result
            #print cmd
            #print result
        except Exception as e:
            print e
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            self.error = traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)

    def _exec_cmd(self, cmd):
        try:
            self.start_timestamp = time.time()
            self.cmd = cmd

            self.task_status = EXECUTE
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
            self._set_cmd_pid(p.pid)
            #print("cmd pid = %s" % cmd_pid)
            result = p.communicate()[0], p.returncode

            self.output = result

            self.elapsed_time = self._get_elapsed_time_()
            self._verify_result(result)

            return result
        except Exception as e:
            self.task_status = ERROR
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            self.error = traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)

    def _get_elapsed_time_(self):
        try:
            self.complete_timestamp = time.time()
            if self.start_timestamp == 0 or self.complete_timestamp == 0:
                return False
            else:
                if self.start_timestamp > self.complete_timestamp:
                    return False
                else:
                    return self.complete_timestamp - self.start_timestamp
        except Exception as e:
            self.task_status = ERROR
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            self.error = traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)

    def _convert_datetime(self, timestamp, str_format=DEFAULT_TASK_TIME_FORMAT):
        return datetime.datetime.fromtimestamp(timestamp).strftime(str_format)

    def _convert_seconds(self, second):
        return str(datetime.timedelta(seconds=second))

    def _convert_to_timestamp(self, datetime_str, str_format=DEFAULT_TASK_TIME_FORMAT):
        return time.mktime(datetime.datetime.strptime(datetime_str, str_format).timetuple())

    def _verify_result(self, result):
        try:
            #print self.start_timestamp, self.complete_timestamp
            if self.elapsed_time is not False:
                self.task_status = COMPLETE
            if result[1] is not 0:
                self.task_status = ERROR
                self.error = self.output[0]

            #print("%s, %s" % (self.cmd, result))
            self.result['Task_Type'] = self.__class__.__name__
            self.result['Task_Name'] = self.name
            self.result['Task_Worker'] = self.worker_name
            self.result['Task_Status'] = self.task_status
            self.result['Task_Command'] = self.cmd
            self.result['Task_Error'] = self.error
            self.result['Task_Time'] = {'Began': self._convert_datetime(self.start_timestamp),
                                        'Completed': self._convert_datetime(self.complete_timestamp),
                                        'Elapsed': self._convert_seconds(self.elapsed_time)}
        except Exception as e:
            self.task_status = ERROR
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)

    def execute(self):
        if self.exec_class is not None and self.method_name != '':
            self.output = getattr(self.exec_class, self.method_name)()
        else:
            self.output = self.__call__()
            self._get_elapsed_time_()

        return self.output
