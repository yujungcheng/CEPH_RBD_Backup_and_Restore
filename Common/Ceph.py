#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import subprocess

class Ceph(object):
    def __init__(self, log=None, cluster_name=None, conffile=None):
        self.log = log
        self.cluster_name = cluster_name
        self.conffile = conffile

    def _exec_cmd(self, cmd):
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        output = p.communicate()[0]
        return_code = p.returncode
        if return_code == 0:
            return str(output).rstrip()
        return ''

    def get_fsid(self):
        cmd = "ceph fsid --cluster %s" % self.cluster_name
        return self._exec_cmd(cmd)

    def get_health(self):
        cmd = "ceph health detail --cluster %s" % self.cluster_name
        return self._exec_cmd(cmd)

    def get_mon_status(self):
        cmd = "ceph mon_status --cluster %s" % self.cluster_name
        return self._exec_cmd(cmd)

    def get_version(self):
        cmd = "ceph version --cluster %s" % self.cluster_name
        return self._exec_cmd(cmd)
