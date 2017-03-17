#!/usr/bin/env python
# -*- coding: utf-8 -*-

import rados, subprocess
from rbd import RBD, Image

class Pool(object):

    def __init__(self, log, pool_name, conffile):
        self.log = log
        self.pool_name = pool_name
        self.conffile = conffile

        try:
            self.cluster = rados.Rados(conffile='')
            self.cluster.connect()
            self.ioctx = self.cluster.open_ioctx(pool_name)
            self.rbd = RBD()

            self.connected = True
        except Exception as e:
            self.log.error("unable to open ioctx of pool %s, conffile=%s" % (pool_name, conffile))
            self.connected = False

    def _exec_cmd(self, cmd):
        try:
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
            result = p.communicate()[0]
            result = result.strip()
            self.log.debug((cmd, result))
            return result
        except Exception as e:
            self.log.error("command (%s) execution failed" % cmd)

    def get_rbd_list(self):
        try:
            rbd_list = self.rbd.list(self.ioctx)
            self.log.info(("RBD image name list in pool %s:" % self.pool_name, rbd_list))
            return rbd_list
        except Exception as e:
            self.log.error("unable to list rbd image in pool %s, %s" %(self.pool_name, e))
            return False

    def get_rbd_size(self, rbd_name):
        try:
            image = Image(self.ioctx, rbd_name)
            size = image.size()
            self.log.info("%s has image size %s bytes in pool %s." %(str(rbd_name), size, self.pool_name))

            return int(size)
        except Exception as e:
            self.log.error("unable to get size of rbd image (%s). %s" %(rbd_name, e))
            return False
        finally:
            image.close()

    def get_used_size(self, rbd_name, method=2):
        ''' trick to get rbd used size
        method 1 uses rbd du command to retrieve used size
        method 2 uses rbd diff command
        '''
        try:
            size = 0
            if method == 1:
                cmd = "rbd du -p %s %s | grep \'%s \' | awk \'{print $2}\'" % (self.pool_name,
                                                                               rbd_name,
                                                                               rbd_name)
                result = self._exec_cmd(cmd)
                result = result.rstrip()
                n_size = result[:-1]
                if 'k' in result:
                    size = int(n_size) * 1024
                elif 'M' in result:
                    size = int(n_size) * 1024 * 1024
                elif 'G' in result:
                    size = int(n_size) * 1024 * 1024 * 1024
                elif 'T' in result:
                    size = int(n_size) * 1024 * 1024 * 1024 * 1024
                else:
                    size = None
            elif method == 2:
                cmd = "rbd diff -p %s %s | awk '{ SUM += $2} END { print SUM }'" %(self.pool_name,
                                                                                   rbd_name)
                size = self._exec_cmd(cmd)

            if size is None or size is '':
                return False

            self.log.info("%s used %s bytes. (method %s)" % (rbd_name, size, method))
            return int(size)
        except Exception as e:
            self.log.error("unable to get used size of rbd image %s in pool %s. %s" % (rbd_name,
                                                                                       self.pool_name,
                                                                                       e))
            return False

    def get_rbd_stat(self, rbd_name):
        try:
            image = Image(self.ioctx, rbd_name)
            stat = image.stat()
            self.log.info(("stat of rbd image %s" %rbd_name, stat))
            return stat
        except Exception as e:
            self.log.error("unable to get stat of rbd image (%s). %s" % (rbd_name, e))
            return False
        finally:
            image.close()

    def get_rbd_features(self, rbd_name):
        try:
            image = Image(self.ioctx, rbd_name)
            feature = image.features()
            self.log.info(("feature of rbd image %s" %rbd_name, feature))
            return feature
        except Exception as e:
            self.log.error("unable to get feature of rbd image (%s). %s" % (rbd_name, e))
            return False
        finally:
            image.close()

    def get_rbd_snap_list(self, rbd_name):
        try:
            image = Image(self.ioctx, rbd_name)
            snap_list = image.list_snaps()
            self.log.info(("snapshot list of rbd image %s" %rbd_name, snap_list))
            return snap_list
        except Exception as e:
            self.log.error("unable to get snapshot list of rbd image %s" % rbd_name)
            return False
        finally:
            image.close()

    def get_version(self):
        self.log.info("librbd version: %s" % self.rbd.version())
        return self.rbd.version()

    def close(self):
        self.ioctx.close()
        self.cluster.shutdown()
