#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time, datetime

from rbd import RBD, Image

from Common.Constant import *
from Common.BaseTask import BaseTask

class RBDDiffTask(BaseTask):

    def __init__(self, cluster_name, pool_name, rbd_name,
                       conffile='', from_snap=None, rbd_id=None):

        super(RBDSnapshotTask, self).__init__()

        self.cluster_name = cluster_name
        self.pool_name = pool_name
        self.rbd_name = rbd_name
        self.from_snap = from_snap
        self.rbd_id = rbd_id

        # sum output
        self.diff_count = 0
        self.diff_size = 0

    def __str__(self):
        return "diff_rbd_%s_in_pool_%s" % (self.rbd_name, self.pool_name)

    def _iterate_cb(self, offset, length, exists):
        if exists:
            self.diff_count += 1
            self.diff_size += length

    def execute(self, worker_name=None):
        try:
            '''
            from_snap_str = ''
            if self.from_snap is not None:
                from_snap_str = "--from-snap %s" % self.from_snap

            cmd = "rbd diff --cluster %s -p %s %s %s" % (self.cluster_name,
                                                          self.pool_name,
                                                          from_snap_str,
                                                          self.rbd_name)
            cmd = "%s | awk '{ SUM += $2} END { print SUM }'" % cmd
            size = self._exec_cmd(cmd)

            '''
            self.worker_name = worker_name

            self.start_timestamp = time.time()

            cluster = rados.Rados(conffile=self.conffile)
            cluster.connect()
            ioctx = cluster.open_ioctx(self.pool_name)
            image = Image(self.ioctx, rbd_name)

            size = image.size()

            image.diff_iterate(0, size, self.from_snap, self._iterate_cb)

            # just set the cmd as function all
            self.cmd = "image.diff_iterate(0, %s, %s, self._iterate_cb)" % (size, self.from_snap)

            self.elapsed_time = self._get_elapsed_time_()
            self._verify_result(result)

        except Exception as e:
            print("error: %s" %e)
            return False
