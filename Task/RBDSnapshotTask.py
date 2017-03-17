#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time, datetime

from Common.Constant import *
from Common.BaseTask import BaseTask


class RBDSnapshotTask(BaseTask):
    def __init__(self, cluster_name, pool_name, rbd_name,
                 action=CREATE, snap_name=None, retain_count=2, protect=False):
        super(RBDSnapshotTask, self).__init__()

        self.pool_name = pool_name
        self.rbd_name = rbd_name
        self.cluster_name = cluster_name
        self.action = action
        self.snap_name = snap_name
        self.protect = protect

        self.snap_time_format = '%Y_%m_%d_%H_%M_%S_%f'
        self.init_timestamp = time.time()
        self.name = "snapshot_%s_in_pool_%s" % (self.rbd_name,
                                                self.pool_name)

    def __str__(self):
        return self.name

    def _get_snapshot_id(self, snap_name=None):
        cmd = "rbd snap ls --cluster %s %s/%s | grep ' %s ' | awk '{print $1}'" % (self.cluster_name,
                                                                                   self.pool_name,
                                                                                   self.rbd_name,
                                                                                   self.snap_name)
        return self._exec_cmd(cmd)

    def _rm_snapshot(self):
        cmd = "rbd snap rm --cluster %s -p %s %s@%s" % (self.cluster_name,
                                                        self.pool_name,
                                                        self.rbd_name,
                                                        self.snap_name)
        return self._exec_cmd(cmd)

    def _create_snapshot(self):
        if self.snap_name is None:
            self.snap_name = datetime.datetime.now().strftime(self.snap_time_format)

        cmd = "rbd snap create --cluster %s -p %s %s@%s" % (self.cluster_name,
                                                            self.pool_name,
                                                            self.rbd_name,
                                                            self.snap_name)
        return self._exec_cmd(cmd)

    def _protect(self, protect):
        if protect:
            protect_op = 'protect'
        else:
            protect_op = 'unprotect'

        cmd = "rbd snap %s --cluster %s -p %s %s@%s" % (protect_op,
                                                        self.cluster_name,
                                                        self.pool_name,
                                                        self.rbd_name,
                                                        self.snap_name)
        print cmd
        return self._exec_cmd(cmd)

    def execute(self, worker_name=None):
        try:
            self.worker_name = worker_name
            result = None

            if self.action == CREATE:
                result = self._create_snapshot()
                if self.protect:
                    self._protect(protect=True)
            elif self.action == DELETE:
                self._protect(protect=False)
                result = self._rm_snapshot()

            # if verify successfully, change task status to 'completed'
            self._verify_result(result)
            return result
        except Exception as e:
            print("error: %s" %e)
            return False
