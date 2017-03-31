#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time, datetime

from Common.Constant import *
from Common.BaseTask import BaseTask


class RBDSnapshotTask(BaseTask):
    def __init__(self, cluster_name, pool_name, rbd_name,
                 action=CREATE, snap_name=None, protect=False, rbd_id=None):
        super(RBDSnapshotTask, self).__init__()

        self.pool_name = pool_name
        self.rbd_name = rbd_name
        self.cluster_name = cluster_name
        self.action = action
        self.snap_name = snap_name
        self.protect = protect
        self.rbd_id = rbd_id

        self.snap_time_format = '%Y_%m_%d_%H_%M_%S'
        self.init_timestamp = time.time()

        self.name = self.__str__()

    def __str__(self):
        return "%s_snapshot_%s_in_pool_%s" % (SNAP_ACT[self.action],
                                              self.rbd_name,
                                              self.pool_name)

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

    def _purge_snapshot(self):
        cmd = "rbd snap purge --cluster %s -p %s %s" % (self.cluster_name,
                                                        self.pool_name,
                                                        self.rbd_name)
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
            elif self.action == PURGE:
                result = self._purge_snapshot()

            return result
        except Exception as e:
            print("error: %s" %e)
            return False
