#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import time
import subprocess

from threading import Thread
from Queue import Queue


class Monitor(Thread):

    def __init__(self, req_queue, pid):

        Thread.__init__(self)

        self.req_queue = req_queue
        self.pid = pid

        self.start()

    def _exec_cmd(self, cmd):
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        output = p.communicate()[0]
        return_code = p.returncode

        #self.log.debug((cmd, output))

        if return_code == 0:
            return output
        return ''

    def run(self):

        count = 0
        while True:
            try:
                print("getting pid of command.")
                pid = self.req_queue.get()
                print("pid of command is %s" % pid)
                if os.path.isdir("/proc/%s" % pid):
                    print self.monitor_disk_io(pid)

                if pid is None:
                    break

                count += 1
                if count == 100:
                    break

                time.sleep(1)
            except Exception as e:
                print e

    def monitor_disk_io(self, pid):
        cmd = "cat /proc/%s/io | grep write_bytes" % pid
        return self._exec_cmd(cmd)

    def monitor_network_io(self, pid):
        cmd = "cat /proc/%s/io | grep write_bytes" % pid
        return self._exec_cmd(cmd)
