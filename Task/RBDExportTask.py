#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time, datetime

from Common.Constant import *
from Common.BaseTask import BaseTask

# represent a rbd export task
class RBDExportTask(BaseTask):
    def __init__(self, cluster_name, pool_name, rbd_name, export_destpath,
                 export_type=FULL, from_snap=None, to_snap=None):
        super(RBDExportTask, self).__init__()

        self.pool_name = pool_name
        self.rbd_name = rbd_name
        self.cluster_name = cluster_name
        self.export_destpath = export_destpath

        self.export_type = export_type    # full or diff
        self.from_snap = from_snap
        self.to_snap = to_snap

        self.rbd_size = 0

        self.init_timestamp = time.time()
        self.name = "export_%s_%s_in_pool_%s_@_%s" % (self.export_type,
                                                      self.rbd_name,
                                                      self.pool_name,
                                                      self.init_timestamp)

    def __str__(self):
        return self.name

    def _rbd_export(self):
        if self.to_snap is not None:
            self.rbd_name = "%s@%s" % (self.rbd_name, self.to_snap)

        cmd = "rbd export --cluster %s -p %s %s %s" %(self.cluster_name,
                                                      self.pool_name,
                                                      self.rbd_name,
                                                      self.export_destpath)
        return self._exec_cmd(cmd)

    def _rbd_export_diff(self):
        if self.from_snap is not None:
            self.from_snap = "--from-snap %s" % self.from_snap
        if self.to_snap is Null:
            return False

        cmd = "rbd export-diff --cluster %s -p %s %s@%s %s %s" % (self.cluster_name,
                                                                  self.pool_name,
                                                                  self.rbd_name,
                                                                  self.to_snap,
                                                                  self.from_snap,
                                                                  self.export_destpath)
        return self._exec_cmd(cmd)

    def execute(self, worker_name=None):
        try:
            self.worker_name = worker_name
            result = None

            if self.export_type == FULL:
                result = self._rbd_export()
            elif self.export_type == DIFF:
                result = self._rbd_export_diff()

            self._verify_result(result)
            return result
        except Exception as e:
            print("error: %s" %e)
            return False