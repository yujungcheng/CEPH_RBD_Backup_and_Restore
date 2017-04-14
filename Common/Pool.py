#!/usr/bin/env python
# -*- coding: utf-8 -*-

import rados, subprocess, sys, traceback
from rbd import RBD, Image

class Pool(object):

    def __init__(self, log, cluster_name, pool_name, conffile=''):
        self.log = log
        self.cluster_name = cluster_name
        self.pool_name = pool_name
        self.conffile = conffile

        self.rbd_name = []     # [rbd_name, ...]
        self.snap_name = {}    # {rbd_name: [snap_name, ...], ...}
        self.rbd_info = {}     # {rbd_name: {}, ...}
        self.snap_info = {}    # {rbd_name: {}, ...}

        self.snap_id_list = {}      # {rbd_name, [snap_id, ...], ...}

        self.rbd_snap_id = {}    # {rbd_snap_name: id, ... }

        try:
            self.cluster = rados.Rados(conffile=self.conffile)
            self.cluster.connect()
            self.ioctx = self.cluster.open_ioctx(pool_name)
            self.rbd = RBD()

            self.connected = True
        except Exception as e:
            self.log.error("unable to open ioctx of pool %s, conffile=%s, %s" %
                          (pool_name, conffile, e))
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            self.connected = False

    def _exec_cmd(self, cmd):
        try:
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
            result = p.communicate()[0]
            return_code = p.returncode

            self.log.debug((cmd,
                            "output = %s" %result.strip(),
                            "return code = %s" %return_code))

            if return_code != 0:
                return False

            return result.strip()
        except Exception as e:
            self.log.error("command (%s) execution failed" % cmd)

    def _calc_size_in_bytes(self, result):
        try:
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
                size = 0

            return size
        except Exception as e:
            self.log.error("unable to calcue size in bytes. input = %s" % result)

    def _pack_rbd_snap_name(self, rbd_name, snap_name):
        return "%s@%s" % (rbd_name, snap_name)

    def collect_info(self):
        ''' collect all rbd and snapshot information in the pool
        '''
        try:
            self.log.info("start collect rbd info in pool %s" % self.pool_name)

            rbd_list = self.get_rbd_name_list()
            self.rbd_list = rbd_list

            for rbd_name in rbd_list:

                snap_name_list = []
                snap_id_list = []
                rbd_snap_list = []

                rbd_info = {}
                rbd_info['features'] = self.get_rbd_features(rbd_name)
                rbd_info['size'] = self.get_rbd_size(rbd_name)
                rbd_info['used'] = self.get_used_size(rbd_name)
                self.rbd_info[rbd_name] = rbd_info

                snap_list = self.get_rbd_snap_list(rbd_name)
                if snap_list is False:
                    self.log.error("unable to get snapshot info.")
                    return False

                for snap in snap_list:
                    rbd_snap_name = self._pack_rbd_snap_name(rbd_name, snap['name'])

                    snap_info = {}
                    snap_info['id'] = snap['id']
                    snap_info['name'] = snap['name']
                    snap_info['size'] = snap['size']
                    snap_info['used'] = self.get_used_size(rbd_snap_name)

                    snap_name_list.append(snap['name'])
                    snap_id_list.append(snap['id'])
                    self.rbd_snap_id[rbd_snap_name] = snap['id']

                    rbd_snap_list.append(snap_info)

                self.snap_info[rbd_name] = rbd_snap_list
                self.snap_name[rbd_name] = snap_name_list
                self.snap_id_list[rbd_name] = snap_id_list.sort()

            self.log.info("completed collect rbd info in pool %s" % self.pool_name)
            return True
        except Exception as e:
            self.log.error("collect rbd info failed. %s" % e)
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            sys.exit(2)
            return False

    def get_rbd_info(self, rbd_name, from_snap=None):
        ''' if self.rbd_info.has_key(rbd_name):
                return self.rbd_info[rbd_name]
            return False
        '''
        try:
            rbd_info = {}
            rbd_info['features'] = self.get_rbd_features(rbd_name)
            rbd_info['rbd_full_size'] = self.get_rbd_size(rbd_name)
            rbd_info['rbd_used_size'] = self.get_used_size(rbd_name, from_snap)
            rbd_info['snapshot_list'] = self.get_snap_name_list(rbd_name)

            return rbd_info
        except Exception as e:
            self.log.error("collect rbd info failed. %s" % e)
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            sys.exit(2)
            return False

    def get_snap_info(self, rbd_name):
        if self.snap_info.has_key(rbd_name):
            return self.snap_info[rbd_name]
        return False

    def get_rbd_name_list(self):
        try:
            rbd_list = self.rbd.list(self.ioctx)
            self.log.info(("RBD image name list in pool %s:" % self.pool_name, rbd_list))
            return rbd_list
        except Exception as e:
            self.log.error("unable to list rbd image in pool %s, %s" %(self.pool_name, e))
            return False

    def get_snap_name_list(self, rbd_name):
        try:
            snap_list = self.get_rbd_snap_list(rbd_name)
            snap_name_list = []
            for snap in snap_list:
                snap_name_list.append(snap['name'])
            return snap_name_list
        except Exception as e:
            self.log.error("collect rbd info failed. %s" % e)
            return False

    def get_rbd_size(self, rbd_name):
        ''' get size of rbd or rbd snapshot '''
        try:
            image = Image(self.ioctx, rbd_name)
            size = image.size()
            image.close()

            if size is False:
                return False

            self.log.info("%s has image size %s bytes in pool %s." %(str(rbd_name), size, self.pool_name))
            return int(size)
        except Exception as e:
            self.log.error("unable to get size of rbd image (%s). %s" %(rbd_name, e))
            return False

    def get_used_size(self, rbd_name, snap_name=None, from_snap=None):
        ''' trick to get rbd/snap used size by rbd diff command
        '''
        try:

            # todo: try use diff_iterate from librbdpy
            #
            #image = Image(self.ioctx, rbd_name)
            #extents = image.diff_iterate()
            #for extents in image.diff_iterate():

            if snap_name is not None:
                rbd_name = "%s@%s" % (rbd_name, snap_name)

            from_snap_str = ''
            if from_snap is not None:
                from_snap_str = "--from-snap %s" % from_snap

            cmd = "rbd diff --cluster %s -p %s %s %s" % (self.cluster_name,
                                                          self.pool_name,
                                                          from_snap_str,
                                                          rbd_name)
            cmd = "%s | awk '{ SUM += $2} END { print SUM }'" % cmd
            size = self._exec_cmd(cmd)

            if size is False:
                self.log.error("unable to get used size of %s in pool %s." % (rbd_name,
                                                                              self.pool_name))
                return False
            if size == '':
                size = 0

            self.log.info("%s used %s bytes." % (rbd_name, size))
            return int(size)
        except Exception as e:
            self.log.error("unable to get used size of %s in pool %s. %s" % (rbd_name,
                                                                             self.pool_name,
                                                                             e))
            return False

    def get_rbd_features(self, rbd_name):
        try:
            image = Image(self.ioctx, rbd_name)
            feature = image.features()
            self.log.info("feature of rbd image %s = %s" % (rbd_name, feature))
            image.close()

            return feature
        except Exception as e:
            self.log.error("unable to get feature of rbd image (%s). %s" % (rbd_name, e))
            return False

    def get_rbd_snap_list(self, rbd_name):
        try:
            image = Image(self.ioctx, rbd_name)
            snap_list = image.list_snaps()
            image.close()

            snap_id_list = []

            rbd_snap_list = []
            for snap in snap_list:
                snap_info = {}
                snap_info['id'] = snap['id']
                snap_info['size'] = snap['size']
                snap_info['name'] = snap['name']
                rbd_snap_list.append(snap_info)
                snap_id_list.append(snap['id'])

            self.snap_id_list[rbd_name] = snap_id_list.sort()

            if len(rbd_snap_list) == 0:
                self.log.info("no snapshot exist in rbd image %s." % rbd_name)
            else:
                self.log.info(("snapshot list of rbd image %s:" % rbd_name, rbd_snap_list))

            return rbd_snap_list
        except Exception as e:
            self.log.error("unable to get snapshot list of rbd image %s, %s" %
                          (rbd_name, e))
            return False

    def get_rbd_snap_id(self, rbd_name, snap_name):
        try:
            rbd_snap_name = self._pack_rbd_snap_name(rbd_name, snap_name)

            if self.rbd_snap_id.has_key(rbd_snap_name):
                return self.rbd_snap_id[rbd_snap_name]

            snap_list = self.get_rbd_snap_list(rbd_name)

            for snap in snap_list:
                if snap['name'] == snap_name:
                    self.rbd_snap_id[rbd_snap_name] = snap['id']
                    return snap['id']

            return False
        except Exception as e:
            self.log.error("unable to get snapshot id of rbd image %s, snap %s. %s" %
                          (rbd_name, snap_name, e))
            return False

    def get_rbd_stat(self, rbd_name):
        try:
            image = Image(self.ioctx, rbd_name)
            stat = image.stat()
            image.close()

            self.log.info(("stat of rbd image %s" %rbd_name, stat))
            return stat
        except Exception as e:
            self.log.error("unable to get stat of rbd image (%s). %s" % (rbd_name, e))
            return False

    def get_version(self):
        self.log.info("librbd version: %s" % self.rbd.version())
        return self.rbd.version()

    def close(self):
        try:
            self.ioctx.close()
            self.cluster.shutdown()
            self.log.info("close ioctx connection.")
        except Exception as e:
            self.log.error("unable to close cluster pool connection. pool name = %s" % self.pool_name)
            return False
