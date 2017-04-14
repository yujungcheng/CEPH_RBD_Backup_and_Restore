#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# This module responsible for backup Ceph RBD image
# Author: Yu-Jung Cheng

import sys
import os
import errno
import datetime
import time
import traceback
import json

from collections import  OrderedDict
from argparse import ArgumentParser

from Common.Constant import *
from Common.Ceph import Ceph
from Common.Pool import Pool
from Common.Config import RBDConfig
from Common.Logger import Logger
from Common.Manager import Manager
from Common.Directory import Directory
from Common.Metafile import Metafile
from Common.Yaml import Yaml
from Common.OpenStack import OpenStack


from Task.RBDExportTask import RBDExportTask
from Task.RBDSnapshotTask import RBDSnapshotTask
from Task.RBDDiffTask import RBDDiffTask


class RBDBackup(object):

    def __init__(self):
        self.backup_time = datetime.datetime.now().strftime(DEFAULT_BACKUP_TIME_FORMAT)

        self.cfg = None
        self.log = None

        self.backup_config_file = DEFAULT_BACKUP_CONFIG_FILE
        self.backup_config_section = DEFAULT_BACKUP_CONFIG_SECTION

        # backup information, list of rbd, snapshot, pool of cluster
        self.backup_type = None
        self.pool_list = {}
        self.backup_rbd_info_list = []

        # size of backup data and backup item count
        self.total_backup_full_size = 0
        self.total_backup_used_size = 0
        self.total_backup_rbd_count = 0

        # store generated tasks for execution
        self.create_snapshot_tasks = {}
        self.diff_tasks = {}    # currently not used.
        self.export_tasks = {}

        self.removed_snapshots = []
        self.deleted_backup = []

        # data of metafiles
        self.meta_rbd_backup_info = OrderedDict()
        self.meta_rbd_snapshot_list = {}
        self.meta_rbd_backup_list = {}

        self.meta_rbd_info_list = {}

        self.meta_network_usage = {}
        self.meta_disk_io_usage = {}
        self.meta_memory_usage = {}

        self.ceph = Ceph()
        self.manager = None
        self.backup_directory = None
        self.metafile = None

        self.openstck_mapping_enabled = False
        self.cache_clean_enabled = False
        self.monitor_enabled = False

    def _get_rbd_id(self, pool_name, rbd_name):
        return "%s_%s_%s" % (self.ceph.cluster_name, pool_name, rbd_name)

    def _clean_cache(self):
        try:
            drop_cache_level = self.cfg.drop_cache_level
            flush_fs_buffer = self.cfg.flush_file_system_buffer

            if flush_fs_buffer == 'True':
                os.system("sync; sync")
            if drop_cache_level in ['1', '2', '3']:
                os.system("echo %s > /proc/sys/vm/drop_caches" % drop_cache_level)

            self.log.info("clean file system cache.")
            return True
        except Exception as e:
            print("Error occur when cleaning system cache. %s" % e)
            self.log.error("Error occur when cleaning system cache. %s" % e)

    def _initialize_logging(self, cfg, start_log_title='Start RBD Backup'):
        try:
            self.log = Logger(cfg)
            self.log.blank_line(4)
            log_begin_line = " %s %s " %(start_log_title, self.backup_time)
            self.log.start_line(title="", symbol_count=40)
            self.log.start_line(title=log_begin_line, symbol_count=21)
            self.log.start_line(title="", symbol_count=40)
            self.log.set_logger(name='RBDBackup')
            return True
        except Exception as e:
            print("Error, fail to initialize logging. %s" % e)
            return False

    def _get_backup_type(self):
        '''
        the weekday number get from datetime.today() is start from 0,
        so plus 1 to match number from monday to sunday (1 to 7).
        '''
        try:
            self.log.info("get backup type of weekday.")
            full_weekday = self.cfg.backup_full_weekday
            incr_weekday = self.cfg.backup_incr_weekday
            full_weekdays = full_weekday.split(',')
            incr_weekdays = incr_weekday.split(',')

            weekday = str(int(datetime.datetime.today().weekday()) + 1)
            backup_type = None

            # read snapshot list
            # find last snapshot name from the list. we need this name to
            # perform export diff between snapshots. check this snapshot exist
            # in cluster will performed later. if not found in cluster, do full backup
            self.log.info("read snapshot list from metafile %s" % RBD_SNAPSHOT_MAINTAIN_LIST)
            meta_snapshot_list = self.metafile.read(RBD_SNAPSHOT_MAINTAIN_LIST)
            if meta_snapshot_list is False:
                self.log.warning("unable to read metafile %s. do full backup."
                                 % RBD_SNAPSHOT_MAINTAIN_LIST)
                meta_snapshot_list = {}
                backup_type = FULL
            self.meta_rbd_snapshot_list = meta_snapshot_list

            # read last full backup name
            # we have to ensure last full backup file is exist in backup directory
            # in order to maintain continuous incremental backup file from a full backup
            # will check all incremental backup later
            self.log.info("read backup list from metafile %s" % RBD_BACKUP_CIRCULATION_LIST)
            meta_backup_list = self.metafile.read(RBD_BACKUP_CIRCULATION_LIST)
            if meta_backup_list is False:
                self.log.warning("unable to read metafile %s. do full backup."
                                 % RBD_BACKUP_CIRCULATION_LIST)
                meta_backup_list = {}
                backup_type = FULL
            self.meta_rbd_backup_list = meta_backup_list

            # if backup is set to FULL already, no need to check weekdays.
            if backup_type == FULL:
                return backup_type

            # verifying backup type by weekdays
            if weekday in full_weekdays:
                backup_type = FULL
                self.log.info("today (%s) is within full backup weekdays %s.\n"
                              "do full backup. backup_type = %s"
                              %(weekday, full_weekday, backup_type))
                return backup_type
            elif weekday in incr_weekday:
                backup_type = DIFF
                self.log.info("today (%s) is within incremental backup weekdays %s." %
                              (weekday, incr_weekday))

                if backup_type == DIFF:
                    self.log.info("do incremental backup. backup_type = %s" % backup_type)
                else:
                    self.log.info("do full backup. backup_type = %s" % backup_type)

                return backup_type
            else:
                self.log.info("no bacakup triggered on today(%s)." % weekday)

            return False
        except Exception as e:
            self.log.error("unable to match backup type. %s"% e)
            return False

    def _get_backup_rbd_info_list(self):

        def __set_pool(pool_name):
            try:
                pool = Pool(self.log, self.ceph.cluster_name, pool_name, self.ceph.conffile)
                if pool.connected is False:
                    self.log.error("unable to connect cluster pool %s" % pool_name)
                    return False
                else:
                    self.log.info("\nconnected to ceph cluster pool %s" % pool_name)
                    self.pool_list[pool_name] = pool
                    return True
            except Exception as e:
                self.log.error("unable to set pool %s" % (pool_name, e))
                return False

        def __pack_rbd_info(pool_name, rbd_name, volume_name=None):
            self.log.info("\npacking RBD info, pool_name = %s"
                          ", rbd_name= %s"
                          ", volume_name = %s"
                          % (pool_name, rbd_name, volume_name))

            # get ID to identify the RBD image cross entire backup process.
            # this ID is used to identify the RBD during entire backup states.
            # ----------------------------------------
            rbd_id = self._get_rbd_id(pool_name, rbd_name)
            self.log.info("RBD ID is %s" % rbd_id)

            # generate RBD info, get size of rbd, snapshot list and features from cluster.
            # ----------------------------------------
            self.log.info("get RBD info from cluster.")
            pool = self.pool_list[pool_name]
            rbd_info = {}
            try:
                # in beginning, we dont calculate rbd used size of the RBD.
                # we calculate it after completed its snapshot and get used size of the
                # snapshot, so set to 0 first
                #rbd_info['rbd_used_size'] = pool.get_used_size(rbd_name, from_snap=None)
                rbd_info['rbd_used_size'] = 0
                rbd_info['rbd_full_size'] = pool.get_rbd_size(rbd_name)
                rbd_info['features'] = pool.get_rbd_features(rbd_name)
                rbd_info['snapshot_list'] = pool.get_snap_name_list(rbd_name)

                if rbd_info['features'] is False:
                    return False
                if rbd_info['rbd_full_size'] is False:
                    return False
                if rbd_info['rbd_used_size'] is False:
                    return False
                if rbd_info['snapshot_list'] is False:
                    return False
            except Exception as e:
                self.log.error("unable to get info from ceph cluster. "
                               "skip this RBD backup.")
                return False

            # when backup type is incremental (export diff), do additional check.
            #   check last snappshot name exist in ceph cluster
            #   check last full backup name
            # if failed verifying, set backup to full backup
            # ----------------------------------------
            last_snapshot_name = None
            last_backup_name = None
            backup_type = self.backup_type
            if backup_type == DIFF:
                self.log.info("verify last snapsho name, full backup file and "
                              "completeness of incremental backup file for "
                              "incremental backup.")

                # get last full backup name, it is required for storing
                # incremental backup file to belonging full backup directory
                # if unable to create or get RBD directory, return False
                if self.meta_rbd_backup_list.has_key(rbd_id):
                    backup_list = self.meta_rbd_backup_list[rbd_id]
                    if len(backup_list) == 0:
                        backup_type = FULL
                    else:
                        last_backup_name = backup_list[-1]
                        self.log.info("last full backup file name is %s" % last_backup_name)
                else:
                    self.log.warning("unable to get last full backup file name.")
                    backup_type = FULL

                # verify the last snapshot exist in cluster
                if self.meta_rbd_snapshot_list.has_key(rbd_id):
                    snap_list = self.meta_rbd_snapshot_list[rbd_id]
                    if len(snap_list) == 0:
                        backup_type = FULL
                    else:
                        last_snapshot_name = snap_list[-1]
                        self.log.info("set snapshot name of last backup = %s" % last_snapshot_name)

                        if last_snapshot_name not in rbd_info['snapshot_list']:
                            self.log.warning("last snapshot name not exist in cluster. "
                                             "set to full backup.")
                            backup_type = FULL
                            last_snapshot_name = None
                        # todo: check the last incrmental backup file name has last snapshot name
                        # ????
                else:
                    self.log.warning("unable to get last snapshot name. "
                                     "set to full backup.")
                    backup_type = FULL

            # add additional info to rbd_info
            # ----------------------------------------
            self.log.info("add additonal info of RBD.")
            rbd_info['id'] = rbd_id
            rbd_info['rbd_name'] = rbd_name
            rbd_info['pool_name'] = pool_name
            rbd_info['volume_name'] = volume_name
            rbd_info['backup_type'] = backup_type

            rbd_info['last_snapshot_name'] = last_snapshot_name
            rbd_info['last_backup_name'] = last_backup_name    # None if backup type is Full
            rbd_info['snapshot_retain_count'] = 0  # todo: get from config
            rbd_info['backup_retain_count'] = 0  # todo: get from config

            # update total size of RBD to backup and RBD count
            # ----------------------------------------
            self.log.info("update total backup size.")
            self.total_backup_full_size += rbd_info['rbd_full_size']
            #self.total_backup_used_size += rbd_info['rbd_used_size']
            self.total_backup_rbd_count += 1

            self.log.info("return packed rbd_info to RBD list. rbd_id = %s" % rbd_id)
            return rbd_info

        try:
            # store all backup RBD image information
            rbd_list = []

            openstack_mapping = self.cfg.openstack_enable_mapping
            self.log.info("\nopenstack enable mapping is %s" % openstack_mapping)

            # get backup list from openstack or backup config yaml file
            # if openstack_mapping is True, read RBD list from openstack yaml file
            # ----------------------------------------
            if openstack_mapping is 'True':

                yaml_path = self.cfg.openstack_yaml_filepath
                yaml_section = self.cfg.openstack_section_name
                distribution = self.cfg.openstack_distribution
                pool_name = self.cfg.openstack_pool_name

                self.log.info("read RBD list from %s, "
                              "section = %s, "
                              "openstack distribution = %s, "
                              "pool_name = %s"
                              % (yaml_path, yaml_section, distribution, pool_name))
                yaml = Yaml(self.log, yaml_path)
                yaml_data = yaml.read(yaml_section)

                if yaml_data is False:
                    self.log.error("unable to read RBD list.")
                    return False

                openstack = OpenStack(self.log, yaml_data=yaml_data, distribution=distribution)
                if openstack.set_cinder_client() is False:
                    self.log.error("unable to connect openstack cinder client.")
                    return False

                if __set_pool(pool_name) is False:
                    return False

                volumes = openstack.get_cinder_volume()
                if volumes is False:
                    return False

                for volume_name, volume_id in volumes.iteritems():
                    rbd_info = __pack_rbd_info(pool_name, volume_id, volume_name)
                    if rbd_info is False:
                        self.log.warning("unable to pack RBD info. skip backup of it")
                    else:
                        rbd_list.append(rbd_info)

            else:
                yaml_path = self.cfg.backup_yaml_filepath
                yaml_section = self.cfg.backup_yaml_section_name

                self.log.info("read RBD list from %s, section = %s" %(yaml_path, yaml_section))
                yaml = Yaml(self.log, yaml_path)
                yaml_data = yaml.read(yaml_section)

                if yaml_data is False:
                    self.log.error("unable to read RBD list.")
                    return False

                for pool_name, rbd_name_list in yaml_data.iteritems():

                    if not __set_pool(pool_name):
                        return False

                    for rbd_name in rbd_name_list:
                        rbd_info = __pack_rbd_info(pool_name, rbd_name)
                        if rbd_info is False:
                            self.log.warning("unable to pack RBD info. skip backup of it")
                        else:
                            rbd_list.append(rbd_info)

            # if no RBD image get, nothing to do next, return false.
            if self.total_backup_rbd_count == 0:
                self.log.info("no RBD image to backup.")
                return False

            # verify sufficient spaces size for backup, if not, return false.
            # we use full RBD image size rather than actually used size.
            # if self.backup_directory.available_bytes <= self.total_backup_used_size:
            if self.backup_directory.available_bytes <= self.total_backup_full_size:
                self.log.info("no enough space size for backup.\n"
                              "total RBD image size to backup = %s bytes\n"
                              "available backup space size    = %s bytes\n"
                              "need %s bytes more space size."
                              % (self.total_backup_full_size,
                                 self.backup_directory.available_bytes,
                                (self.total_backup_full_size - self.backup_directory.available_bytes)))
                return False

            return rbd_list
        except Exception as e:
            self.log.error("unable to get RBD image list for backup. %s" % e)
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            return False

    def _sort_backup_list(self, backup_rbd_info_list, sort_key):
        ''' sort backup list by size '''
        try:
            if self.cfg.backup_small_size_first == 'True':
                self.log.info("\nsort backup RBD image list by %s, small size first." % sort_key)
                sorted_list = sorted(backup_rbd_info_list, key=lambda k: k[sort_key])

            else:
                self.log.info("\nsort backup RBD image list by %s, large size first." % sort_key)
                sorted_list = sorted(backup_rbd_info_list, key=lambda k: k[sort_key], reverse=True)

            temp_list = []
            for rbd_info in sorted_list:
                rbd_id = rbd_info['id']
                rbd_used = rbd_info['rbd_used_size']
                rbd_full = rbd_info['rbd_full_size']
                temp_list.append("ID = %s, size = %s, used = %s" % (rbd_id, rbd_full, rbd_used))
            self.log.info(("sorted backup list: ", temp_list))

            return sorted_list
        except Exception as e:
            self.log.warning("sorting RBD backup list failed. %s" % e)
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            return False

    def _write_metafile(self, metafile_name, metadata, overwrite=True):
        try:
            if metafile_name == RBD_SNAPSHOT_MAINTAIN_LIST:
                if not self.metafile.write(RBD_SNAPSHOT_MAINTAIN_LIST, metadata, overwrite=overwrite):
                    self.log.error("unable to write RBD snapshot maintain list to metafile")
                    return False
            elif metafile_name == RBD_BACKUP_CIRCULATION_LIST:
                if not self.metafile.write(RBD_BACKUP_CIRCULATION_LIST, metadata, overwrite=overwrite):
                    self.log.error("unable to write RBD backup circulation list to metafile")
                    return False
            elif metafile_name == BACKUP_INFO:
                if not self.metafile.write(BACKUP_INFO, metadata, overwrite=overwrite):
                    self.log.error("unable to write RBD backup info to metafile")
                    return False
            elif metafile_name == RBD_INFO_LIST:
                if not self.metafile.write(RBD_INFO_LIST, metadata, overwrite=overwrite):
                    self.log.error("unable to write RBD backup info list to metafile")
                    return False
            else:
                self.log.error("unknown metafile name %s" % metafile_name)
                return False
            return True
        except Exception as e:
            self.log.error("unable to remove exceed snapshot of RBD. %s" % e)
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            return False

    def read_argument_list(self, argument_list):
        try:
            parser = ArgumentParser(add_help=False)
            parser.add_argument('--backup_config_file')
            parser.add_argument('--backup_config_section')
            parser.add_argument('--ceph_conffile')
            parser.add_argument('--ceph_cluster_name')
            args = vars(parser.parse_args(argument_list[1:]))

            if args['backup_config_file'] is not None:
                self.backup_config_file = args['backup_config_file']
            if args['backup_config_section'] is not None:
                self.backup_config_section = args['backup_config_section']

            if args['ceph_conffile'] is not None:
                self.ceph.conffile = args['ceph_conffile']
            if args['ceph_cluster_name'] is not None:
                self.ceph.cluster_name = args['ceph_cluster_name']

        except Exception as e:
            print("invalid input argument. %s" % e)

        return True

    def read_config_file(self):
        ''' the RBDConfig class represents as backup config file.
        all options/configuration in file will set as attribute in the class.
        '''
        cfg = RBDConfig(self.backup_config_file)

        if cfg.path != self.backup_config_file:
            print("Error, backup config file not exist.\n"
                  "config file = %s" % self.backup_config_file)
            return False

        if not cfg.check_in_section(self.backup_config_section):
            print("Error, unable to check in config section.\n"
                  "config file = %s, section = %s" %
                  (self.backup_config_file, self.backup_config_section))
            return False

        if not cfg.read_log_config():
            print("Error, unable to read log config.")
            return False

        # logging config has read successfully, we initialize and start logging
        # -----------------------------------------------------------
        if not self._initialize_logging(cfg):
            return False

        if not cfg.read_ceph_config():
            self.log.error("unable to read ceph cluster config.")
            return False

        # read backup config
        if not cfg.read_backup_config():
            self.log.error("unable to read RBD backup config.")
            return False

        # read snapshot config
        if not cfg.read_snapshot_config():
            self.log.error("unable to read snapshot config.")
            return False

        # read openstack config
        if not cfg.read_openstack_config():
            self.log.error("unable to read openstack config.")
            return False

        # read monitor config
        if not cfg.read_monitor_config():
            self.log.warning("unable to read monitor config.")
        else:
            self.monitor_enabled = True

        # read cache clean config
        if not cfg.read_cache_config():
            self.log.warning("unable to read clean cache config.")
        else:
            self.cache_clean_enabled = True

        # set ceph cluster name and conffile if they are not read from argument.
        if self.ceph.conffile is None:
            self.ceph.conffile = cfg.ceph_conffile
        if self.ceph.cluster_name is None:
            self.ceph.cluster_name = cfg.ceph_cluster_name

        # assign cfg to self.cfg
        self.cfg = cfg

        if self.cache_clean_enabled:
            self._clean_cache()

        self.log.info("backup config file = %s\n"
                      "backup config section = %s\n"
                      "ceph config file = %s\n"
                      "ceph cluster name = %s"
                      % (self.backup_config_file,
                         self.backup_config_section,
                         self.ceph.conffile,
                         self.ceph.cluster_name))
        return True

    def read_backup_rbd_info_list(self):
        self.log.start_line(title="\n(2). READ RBD IMAGE LIST TO BACKUP", symbol_count=0)
        self.log.start_line(symbol="-", symbol_count=40)

        # ----------------------------------------
        # get backup type, diff or full backup base on configuration
        # ----------------------------------------
        self.backup_type = self._get_backup_type()
        if self.backup_type is False:
            return False

        backup_rbd_info_list = self._get_backup_rbd_info_list()
        if backup_rbd_info_list is False or len(backup_rbd_info_list) == 0:
            return False

        # ----------------------------------------
        # sorting RBD backup list if configured, sort by rbd full size first
        # ----------------------------------------
        sorted_backup_rbd_info_list = self._sort_backup_list(backup_rbd_info_list, sort_key='rbd_full_size')
        if sorted_backup_rbd_info_list is False:
            return False
        self.backup_rbd_info_list = sorted_backup_rbd_info_list

        self.log.info("\ntotal %s rbd(s) in RBD backup list\n"
                      "total backup RBD full size = %s bytes\n"
                      "total backup RBD used size = %s bytes\n"
                      % (len(self.backup_rbd_info_list),
                         self.total_backup_full_size,
                         self.total_backup_used_size))
        return True

    def initialize_backup_directory(self):
        ''' check the backup directory and initialize metafile
        create folder with {cluster name} in backup directory if not exist
        '''
        #self.log.info("\n(1). INITIALIZE BACKUP DIRECTORY %s" % self.cfg.backup_path)
        self.log.start_line(title="\n(1). INITIALIZE BACKUP DIRECTORY", symbol_count=0)
        self.log.start_line(symbol="-", symbol_count=40)
        #self.log.set_stage(1)

        path = self.cfg.backup_path
        self.log.info("config backup directory is %s" % path)

        directory = Directory(self.log, path)
        if directory.path is not None:

            # add backup cluster name folder in backup directory if not exist
            # ----------------------------------------------------------------
            try:
                self.log.info("initialize the backup directory.")
                cluster_path = directory.add_directory(self.ceph.cluster_name,
                                                       set_path=True,
                                                       check_size=True,
                                                       full_path=True)
                if cluster_path is False:
                    self.log.error("unable to add %s in %s" % (self.ceph.cluster_name,
                                                               path))
                    return False

                self.backup_directory = directory
            except Exception as e:
                self.log.error("directory %s fail initialized. %s" % (path, e))
                exc_type,exc_value,exc_traceback = sys.exc_info()
                traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
                return False

            # add metadata file which record cluster name and fsid
            # ----------------------------------------------------------------
            try:
                self.log.info("initialize metafile in %s" % cluster_path)
                metafile = Metafile(self.log, self.ceph.cluster_name, cluster_path)

                metafiles = [BACKUP_INFO,
                             RBD_INFO_LIST,
                             RBD_SNAPSHOT_MAINTAIN_LIST,
                             RBD_BACKUP_CIRCULATION_LIST]

                if metafile.initialize(self.ceph.cluster_name, metafiles):
                    self.metafile = metafile

                # store backup info
                self.log.info("write initial backup info to metafile %s" % BACKUP_INFO)
                self.meta_rbd_backup_info['time'] = self.backup_time
                self.meta_rbd_backup_info['fsid'] = self.ceph.get_fsid()
                self.meta_rbd_backup_info['name'] = self.ceph.cluster_name
                self.meta_rbd_backup_info['backup_dir_avai_bytes'] = self.backup_directory.available_bytes
                self.meta_rbd_backup_info['backup_dir_used_bytes'] = self.backup_directory.used_bytes

            except Exception as e:
                self.log.error("metafile fail initialized. %s" %e)
                exc_type,exc_value,exc_traceback = sys.exc_info()
                traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
                return False
            finally:
                if self._write_metafile(BACKUP_INFO, self.meta_rbd_backup_info) is False:
                    return False

            return True
        else:
            self.log.error("direcory path %s is invalid." % cfg.backup_directory)
            return False

    def initialize_backup_worker(self):
        #self.log.info("\n(3). INITIALIZE BACKUP WORKERS (child processes)")
        self.log.start_line(title="\n(3). INITIALIZE BACKUP WORKERS (child processes)", symbol_count=0)
        self.log.start_line(symbol="-", symbol_count=40)
        #self.log.set_stage(3)

        try:
            worker_count = self.cfg.backup_concurrent_worker_count
            manager = Manager(self.log, worker_count=worker_count)
            manager.run_worker()

            self.manager = manager

            time.sleep(1)

            if self.monitor_enabled:
                self.manager.initialize_monitor()

            return True
        except Exception as e:
            self.log.error("worker fail initialized. %s" % e )
            return False

    def initialize_snapshot_task(self):
        self.log.start_line(title="\n(4). INITIALIZE RBD SNAPSHOT CREATE TASKS", symbol_count=0)
        self.log.start_line(symbol="-", symbol_count=40)

        try:
            # currently not used...
            protect = self.cfg.snapshot_protect
            if protect == 'True':
                snap_protect = True
            else:
                snap_protect = False
            self.log.info("RBD snapshot protect = %s " % snap_protect)

            for rbd_info in self.backup_rbd_info_list:
                rbd_id = rbd_info['id']
                self.log.info("\ncreating RBD snapshot create task. rbd_id = %s" % rbd_id)

                # logging input parameters of RBDSnapshotTask
                task_info = dict()
                task_info['cluster'] = self.ceph.cluster_name
                task_info['pool'] = rbd_info['pool_name']
                task_info['rbd'] = rbd_info['rbd_name']
                task_info['action'] = SNAP_ACT[CREATE]
                task_info['protect'] = snap_protect
                self.log.info(("RBD snapshot task setting:", task_info))

                # create RBDSnapshotTask
                snapshot_task = RBDSnapshotTask(self.ceph.cluster_name,
                                                rbd_info['pool_name'],
                                                rbd_info['rbd_name'],
                                                action=CREATE,
                                                protect=snap_protect,
                                                rbd_id=rbd_id)

                # store created snapshot task
                self.create_snapshot_tasks[rbd_id] = snapshot_task
                self.log.info("created RBD snapshot create task. "
                              "task_name = %s" % snapshot_task)

            if len(self.create_snapshot_tasks) == 0:
                self.log.warning("there is no any snapshot task initialized.")
                return False

            self.log.info("\ntotal %s snapshot tasks created." % len(self.create_snapshot_tasks))
            return True
        except Exception as e:
            self.log.error("unable to create snapshot task. %s" % e)
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            sys.exit(2)
            return False

    def start_snapshot(self):
        self.log.start_line(title="\n(5). START RBD SNAPSHOT TASKS", symbol_count=0)
        self.log.start_line(symbol="-", symbol_count=40)

        submitted_task_count = 0
        completed_task_count = 0
        uncompleted_task_count = 0

        # Submit snapshot task to workers
        # ---------------------------------------------------------------------
        for rbd_info in self.backup_rbd_info_list:
            try:
                rbd_id = rbd_info['id']
                self.manager.add_task(self.create_snapshot_tasks[rbd_id])
                submitted_task_count += 1
            except Exception as e:
                self.log.error("unable to submit snapshot create task to worker manager. "
                               "rbd id = %s, %s" % (rbd_id, e))
                continue

        if submitted_task_count == 0:
            self.log.error("no any RBD snapshot create task submitted.")
            return False

        # Collect finished RBD snapshot tasks
        # after submited all snapshot tasks, wait them completed before move on next
        # get snapshot name from completed task.
        # if snapshot task failed (error), remove it from backup list
        # ----------------------------------------------------------------------
        while True:
            try:
                task = None
                # retrieve finished task
                # ----------------------------------------
                task = self.manager.get_finished_task()
                self.create_snapshot_tasks[task.rbd_id] = task

                self.log.info(("receive finished task %s" % task.name, task.result))

                if task.task_status == COMPLETE:
                    self.log.info("%s is completed." % task.name)
                    completed_task_count += 1

                    # append new snapshot name to snapshot list
                    # snapshot list is read from metafile
                    if self.meta_rbd_snapshot_list.has_key(task.rbd_id):
                        meta_snapshot_list = self.meta_rbd_snapshot_list[task.rbd_id]
                        if isinstance(meta_snapshot_list, list):
                            meta_snapshot_list.append(task.snap_name)
                        else:
                            meta_snapshot_list = [task.snap_name]
                    else:
                        meta_snapshot_list = [task.snap_name]

                    self.meta_rbd_snapshot_list[task.rbd_id] = meta_snapshot_list

                else:
                    # remove this backup item from backup list if snapshot failed
                    # todo: use better way to remove the item in list
                    self.log.warning("%s is not completed. remove it from backup list." % task.name)
                    self.backup_rbd_info_list = [i for i in self.backup_rbd_info_list if i.rbd_id != task.rbd_id]
                    '''
                    backup_rbd_info_list = []
                    for i in self.backup_rbd_info_list:
                        if i.rbd_id != task.rbd_id:
                            print i.rbd_id, task.rbd_id
                            backup_rbd_info_list.append(i)
                    '''
                    self.log.info("%s backup item left in RBD backup list." % len(self.backup_rbd_info_list))
                    uncompleted_task_count += 1

                if submitted_task_count == completed_task_count+uncompleted_task_count:
                    break

            except Exception as e:
                self.log.error("unable to check snapshot result task. %s" % e)
                exc_type,exc_value,exc_traceback = sys.exc_info()
                traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
                uncompleted_task_count += 1

        # update snapshot to metafile
        if self._write_metafile(RBD_SNAPSHOT_MAINTAIN_LIST, self.meta_rbd_snapshot_list) is False:
            return False

        self.log.info("\n%s submitted snapshot task.\n"
                      "%s completed snapshot task.\n"
                      "%s uncompleted snapshot task."
                      % (submitted_task_count,
                         completed_task_count,
                         uncompleted_task_count))
        return True

    # unused
    def initialize_diff_task(self):
        self.log.start_line(title="\n(5-1). INITIALIZE RBD DIFF TASKS", symbol_count=0)
        self.log.start_line(symbol="-", symbol_count=40)

        try:
            for rbd_info in self.backup_rbd_info_list:
                rbd_id = rbd_info['id']

            pass
        except Exception as e:
            self.log.error("unable to create export task. %s" % e)
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            sys.exit(2)
            return False

    # unused
    def start_diff_task(self):
        self.log.start_line(title="\n(5-2). START RBD DIFF TASKS", symbol_count=0)
        self.log.start_line(symbol="-", symbol_count=40)
        try:

            pass
        except Exception as e:
            self.log.error("unable to create export task. %s" % e)
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            sys.exit(2)
            return False

    def initialize_export_task(self):
        ''' this is export stage, all snahshot tasks in snapshot stage must
            be done before do export task
            check backup type, to generate full or incremental export task
        '''
        self.log.start_line(title="\n(6). INITIALIZE RBD EXPORT TASKS", symbol_count=0)
        self.log.start_line(symbol="-", symbol_count=40)

        try:

            # create RBD export tasks
            # ==================================================================
            for rbd_info in self.backup_rbd_info_list:
                self.log.info("\ncreating RBD export task.\n"
                              "rbd_id = %s\n"
                              "backup type = %s" % (rbd_info['id'], rbd_info['backup_type']))

                rbd_id = rbd_info['id']
                pool_name = rbd_info['pool_name']
                rbd_name = rbd_info['rbd_name']
                backup_type = rbd_info['backup_type']
                last_backup_name = rbd_info['last_backup_name']
                from_snap = rbd_info['last_snapshot_name']

                # 1. get snapshot name from the snapshot tasks
                # this snapshot was created in previous stage
                # ----------------------------------------
                snapshot_task = self.create_snapshot_tasks[rbd_id]
                new_snapshot_name = snapshot_task.snap_name
                if not isinstance(new_snapshot_name, str):
                    self.log.warning("invalid snapshot name from snapshot task, "
                                     "skip this RBD backup")
                    continue
                rbd_info['new_snapshot_name'] = new_snapshot_name
                self.log.info("snapshot name to export is %s" % new_snapshot_name)

                # 2. calculate used size of the created snapshot
                # ----------------------------------------
                self.log.info("calculate used size of the snapshot %s" % new_snapshot_name)
                pool = self.pool_list[pool_name]
                #snap_id = pool.get_rbd_snap_id(rbd_name, new_snapshot_name)
                rbd_info['rbd_used_size'] = pool.get_used_size(rbd_name,
                                                               snap_name=new_snapshot_name,
                                                               from_snap=from_snap)
                self.total_backup_used_size += rbd_info['rbd_used_size']

                # 3. produce export destination file path
                # ----------------------------------------
                if backup_type == DIFF:
                    export_filename = ''.join([from_snap, '_to_', new_snapshot_name])
                    incremental_group_dir = last_backup_name
                elif backup_type == FULL:
                    export_filename = new_snapshot_name
                    incremental_group_dir = new_snapshot_name

                    #self.meta_last_full_backup_name[task.rbd_id] = full_backup_file_name
                    #if not self.metafile.update(LAST_FULL_BACKUP_NAME, {task.rbd_id: full_backup_file_name}):
                    #    self.log.error("unable to write full backup file name to metafile")
                    self.log.info("backup type is full backup, update full backup filename")
                    if self.meta_rbd_backup_list.has_key(rbd_id):
                        backup_list = self.meta_rbd_backup_list[rbd_id]
                        backup_list.append(new_snapshot_name)
                    else:
                        backup_list = [new_snapshot_name]

                    self.meta_rbd_backup_list[rbd_id] = backup_list
                else:
                    return False

                self.log.info("creating export file path in backup directory.\n"
                              "pool_name = %s, rbd_name = %s, incremental_group_dir = %s"
                              % (pool_name, rbd_name, incremental_group_dir))

                rbd_path = self.backup_directory.add_directory(pool_name,
                                                               rbd_name,
                                                               incremental_group_dir,
                                                               full_path=True)
                if rbd_path is False:
                    self.log.warning("unable to create RBD backup path. "
                                     "skip this RBD backup. rbd_id = %s" % rbd_id)
                    continue
                else:
                    export_destpath = os.path.join(rbd_path, export_filename)

                # logging export task setting
                # ----------------------------------------
                task_info = dict()
                task_info['cluster'] = self.ceph.cluster_name
                task_info['pool'] = pool_name
                task_info['rbd'] = rbd_name
                task_info['export_destpath'] = export_destpath
                task_info['backup_type'] = EXPORT_TYP[backup_type]
                task_info['from_snap'] = from_snap
                task_info['to_snap'] = new_snapshot_name
                self.log.info(("RBD export task setting:", task_info))

                # 4. create RBDExportTask
                # ----------------------------------------
                export_task = RBDExportTask(self.ceph.cluster_name,
                                            pool_name,
                                            rbd_name,
                                            export_destpath,
                                            export_type=backup_type,
                                            from_snap=from_snap,
                                            to_snap=new_snapshot_name,
                                            rbd_id=rbd_id)

                # 5. store created export tasks
                # ----------------------------------------
                self.export_tasks[rbd_id] = export_task
                self.log.info("created RBD export task. "
                              "task name = %s" % export_task.name)

            # ----------------------------------------
            # sorting RBD backup list again by rbd used size
            # ----------------------------------------
            backup_rbd_info_list = self.backup_rbd_info_list
            sorted_backup_rbd_info_list = self._sort_backup_list(backup_rbd_info_list, sort_key='rbd_used_size')
            if sorted_backup_rbd_info_list is False:
                return False
            self.backup_rbd_info_list = sorted_backup_rbd_info_list

            self.log.info("\ntotal %s export tasks created." % len(self.export_tasks))
            return True
        except Exception as e:
            self.log.error("unable to create export task. %s" % e)
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            sys.exit(2)
            return False
        finally:
            self.log.info("\nupdate backup RBD info to metafile %s" % BACKUP_INFO)
            # update backup info metadata
            backup_info = OrderedDict()
            backup_info['total_rbd_count'] = self.total_backup_rbd_count
            backup_info['total_full_bytes'] = self.total_backup_full_size
            backup_info['total_used_bytes'] = self.total_backup_used_size
            if self._write_metafile(BACKUP_INFO, backup_info, overwrite=False) is False:
                return False

    def start_export(self):
        self.log.start_line(title="\n(7). START RBD EXPORT TASKS", symbol_count=0)
        self.log.start_line(symbol="-", symbol_count=40)

        submitted_task_count = 0
        completed_task_count = 0
        uncompleted_task_count = 0

        # Submit export task to workers
        # ---------------------------------------------------------------------
        for rbd_info in self.backup_rbd_info_list:
            try:
                rbd_id = rbd_info['id']
                self.manager.add_task(self.export_tasks[rbd_id])
                submitted_task_count += 1
            except Exception as e:
                self.log.error("unable to submit export task to worker manager. "
                               "rbd id = %s, %s" % (rbd_id, e))
                continue

        if submitted_task_count == 0:
            self.log.error("no any RBD export task submitted.")
            return False

        # Collect finished RBD export tasks
        # get and update backup file name for full backup task
        # ----------------------------------------------------------------------
        new_full_export_filename = {}
        while True:
            try:
                task = None
                # retrieve finished task
                # ----------------------------------------
                task = self.manager.get_finished_task()
                self.export_tasks[task.rbd_id] = task

                self.log.info(("receive finished task %s" % task.name, task.result))

                if task.task_status == COMPLETE:
                    self.log.info("%s is completed." % task.name)
                    completed_task_count += 1

                else:
                    self.log.warning("%s is not completed" % task.name)
                    uncompleted_task_count += 1

                if submitted_task_count == completed_task_count + uncompleted_task_count:
                    break

            except Exception as e:
                self.log.error("unable to check export result task. %s" % e)
                exc_type,exc_value,exc_traceback = sys.exc_info()
                traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
                uncompleted_task_count += 1

        # write backup list
        if self._write_metafile(RBD_BACKUP_CIRCULATION_LIST, self.meta_rbd_backup_list) is False:
            return False

        self.log.info("\n%s submitted export task.\n"
                      "%s completed export task.\n"
                      "%s uncompleted export task."
                      % (submitted_task_count,
                         completed_task_count,
                         uncompleted_task_count))
        return True

    def remove_exceed_rbd_snapshot(self):
        self.log.start_line(title="\n(8) REMOVE EXCEED RBD SNAPSHOT", symbol_count=0)
        self.log.start_line(symbol="-", symbol_count=40)

        try:
            snap_retain_count = int(self.cfg.snapshot_retain_count)
            self.log.info("retain %s of RBD snapshot in cluster." % snap_retain_count)

            snap_purge = False
            if snap_retain_count == 0:
                self.log.warning("retain 0 of snapshot, removing all RBD snapshots.")
                snap_purge = Ture

            rm_snapshot_task_count = 0
            for rbd_info in self.backup_rbd_info_list:
                self.log.info("\nremoving exceed snapshot of RBD. rbd_id = %s" % rbd_info['id'])

                rbd_id = rbd_info['id']
                pool_name = rbd_info['pool_name']
                rbd_name = rbd_info['rbd_name']

                # reuse previous snapshot task to delete snapshot.
                snapshot_task = self.create_snapshot_tasks[rbd_id]

                # get number of snapshot in the rbd.
                if snap_purge:
                    self.log.info("purge all snapshots in RBD. rbd_id = %s" % rbd_id)

                    snapshot_task.action = PURGE
                    result = snapshot_task.execute()

                    if snapshot_task.task_status == COMPLETE:
                        self.log.info(("purged snapshots of RBD. rbd_id = %s" %
                                        rbd_id, snapshot_task.result))
                    else:
                        self.log.info(("unable to purge snapshots of RBD. rbd_id = %s" %
                                        rbd_id, snapshot_task.result))
                else:
                    pool = self.pool_list[pool_name]
                    pool_snap_name_list = pool.get_snap_name_list(rbd_name)
                    self.log.info(("snapshot name list in cluster:", pool_snap_name_list))

                    # compare snapshot list between cluster and metafile
                    # generated matched snapshot name list
                    # ----------------------------------------
                    rbd_snap_count = 0
                    matched_snapshot_list = []
                    if self.meta_rbd_snapshot_list.has_key(rbd_id):

                        meta_rbd_snap_list = self.meta_rbd_snapshot_list[rbd_id]
                        self.log.info(("snapshot name list in meta:", meta_rbd_snap_list))

                        for snap_name in meta_rbd_snap_list:
                            if snap_name in pool_snap_name_list:
                                rbd_snap_count += 1
                                matched_snapshot_list.append(snap_name)

                        self.log.info("number of matched snapshot name = %s" % rbd_snap_count)

                    # remove exceed snapshot of RBD in cluster.
                    # ----------------------------------------
                    diff_count = (rbd_snap_count - snap_retain_count)
                    if diff_count <= 0:
                        self.log.info("number of tracked RBD snapshot less than "
                                      "or equal to snapshot retain count.\n"
                                      "no snapshot need to be removed.\n")
                    else:
                        self.log.info("%s exceed snapshot to be removed." % diff_count)

                        remove_i = 0    # trace index for pop
                        for i in range(0, diff_count):
                            self.log.info("removing snapshot, name = %s" % matched_snapshot_list[remove_i])
                            rm_snapshot_task_count += 1

                            snapshot_task.action = REMOVE
                            snapshot_task.snap_name = matched_snapshot_list[i]
                            task_name = snapshot_task    # just update task name
                            result = snapshot_task.execute()    # execute remove cmd

                            if snapshot_task.task_status == COMPLETE:
                                self.log.info(("removed RBD snapshot, snapshot name = %s" %
                                                matched_snapshot_list[i], snapshot_task.result))
                                matched_snapshot_list.pop(remove_i)
                                removed_snap = "%s %s %s" % (snapshot_task.pool_name,
                                                             snapshot_task.rbd_name,
                                                             snapshot_task.snap_name)
                                self.removed_snapshots.append(removed_snap)
                            else:
                                self.log.info(("unable to remove RBD snapshot, snapshot_name = %s" %
                                                matched_snapshot_list[i], snapshot_task.result))
                                remove_i += 1

                    # update snapshot list of RBD
                    # ----------------------------------------
                    self.meta_rbd_snapshot_list[rbd_id] = matched_snapshot_list

            self.log.info("\ntotal removed %s snapshots." % rm_snapshot_task_count)
            return True
        except Exception as e:
            self.log.error("unable to remove exceed snapshot of RBD. %s" % e)
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            return False

        finally:
            # write snapshot to metafile
            if self._write_metafile(RBD_SNAPSHOT_MAINTAIN_LIST, self.meta_rbd_snapshot_list) is False:
                return False

    def remove_exceed_backup(self):
        self.log.start_line(title="\n(10) DELETE EXCEED BACKUP FILE", symbol_count=0)
        self.log.start_line(symbol="-", symbol_count=40)

        try:
            backup_retain_count = int(self.cfg.backup_retain_count)
            self.log.info("retain %s of backup in backup directory." % backup_retain_count)

            if backup_retain_count < 1:
                self.log.error("at least one backup must be retained")
                return False

            # remove exceed backups in backup directory
            rm_backup_count = 0
            for rbd_info in self.backup_rbd_info_list:
                self.log.info("\nremoving exceed RBD backup. rbd_id = %s" % rbd_info['id'])

                rbd_id = rbd_info['id']
                pool_name = rbd_info['pool_name']
                rbd_name = rbd_info['rbd_name']

                meta_rbd_backup_list = self.meta_rbd_backup_list[rbd_id]
                self.log.info(("RBD backup list in meta:", meta_rbd_backup_list))

                rbd_backup_count = len(meta_rbd_backup_list)

                diff_count = (rbd_backup_count - backup_retain_count)
                if diff_count <= 0:
                    self.log.info("number of backup in backup directory less than "
                                  "or equal to backup retain count.\n"
                                  "no backup need to be removed.\n")
                else:
                    self.log.info("%s exceed backup to be removed." % diff_count)

                    remove_i = 0    # trace index for pop
                    for i in range(0, diff_count):
                        self.log.info("removing backup, name = %s" % meta_rbd_backup_list[remove_i])

                        deleted_path = self.backup_directory.del_directory(pool_name,
                                                                           rbd_name,
                                                                           meta_rbd_backup_list[remove_i])
                        if deleted_path == False:
                            self.log.info("unable to delete backup. path = %s" % del_path)
                            remove_i += 1
                        else:
                            self.log.info("deleted backup. path = %s" % deleted_path)
                            meta_rbd_backup_list.pop(remove_i)

                            self.deleted_backup.append(deleted_path)

                # update backup list
                # ----------------------------------------
                self.meta_rbd_backup_list[rbd_id] = meta_rbd_backup_list

            self.log.info("\ntotal deleted %s backups." % rm_backup_count)
            return True
        except Exception as e:
            self.log.error("unable to delete exceed backup of RBD. %s" % e)
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            return False

        finally:
            # write backup list
            if self._write_metafile(RBD_BACKUP_CIRCULATION_LIST, self.meta_rbd_backup_list) is False:
                return False

    def finalize(self):
        self.log.start_line(title="\n(11) FINALIZE RBD BACKUP", symbol_count=0)
        self.log.start_line(symbol="-", symbol_count=40)

        # stop worker processes

        if self.manager is not None:
            self.manager.stop_worker()

            # check worker status
            # make sure all worker stopped
            countdown = 5
            worker_count = self.manager.worker_count
            while True:
                workers_status = self.manager.get_workers_status()
                for name, status in workers_status.iteritems():
                    #print name, status
                    if status == STOP or status == READY:
                        worker_count -= 1
                        continue
                    self.log.warning("%s is not stopped yet. status = %s" % (name, status))

                if worker_count == 0:
                    break

                time.sleep(1)
                countdown -= 1
                if countdown == 0:
                    break
        else:
            self.log.error("worker manager is invalid.")

        # close pool (rados connect)
        for pool_name, pool in self.pool_list.iteritems():
            self.log.info("close pool %s" % pool_name)
            pool.close()

        # clean cache
        if self.cache_clean_enabled:
            self._clean_cache()

def main(argument_list):
    try:
        rbdbackup = RBDBackup()
        print("\nStart CEPH RBD Backup @ %s" % rbdbackup.backup_time)
        print("pid = %s" % os.getpid())

        # ----------------------------------------------------------------------
        print("\n1. read RBD backup argument.")
        if rbdbackup.read_argument_list(argument_list) == False:
            return
        else:
            print("  - backup config         = %s" % rbdbackup.backup_config_file)
            print("  - backup config section = %s" % rbdbackup.backup_config_section)
            print("  - ceph connfile         = %s" % rbdbackup.ceph.conffile)
            print("  - ceph cluster name     = %s" % rbdbackup.ceph.cluster_name)

        # ----------------------------------------------------------------------
        print("\n2. read config file options. (Logging will start after loging option read successfully.)")
        if rbdbackup.read_config_file() == False:
            return
        else:
            cfg = rbdbackup.cfg.get_config_dict()
            for key, value in cfg.iteritems():
                if key != 'self':
                    print("  - %s = %s" % (key, value))

        # ----------------------------------------------------------------------
        print("\n3. initialze backup directory.")
        if rbdbackup.initialize_backup_directory() == False:
            return
        else:
            backup_dir = rbdbackup.backup_directory
            print("  - available bytes = %s" % backup_dir.available_bytes)
            print("  - used bytes      = %s" % backup_dir.used_bytes)

        # ----------------------------------------------------------------------
        print("\n4. read RBD list to backup.")
        if rbdbackup.read_backup_rbd_info_list() == False:
            return
        else:
            rbd_list = rbdbackup.backup_rbd_info_list
            for rbd_info in rbd_list:
                print("  - pool name = %s, rbd name = %s" %(rbd_info['pool_name'],
                                                            rbd_info['rbd_name']))

        # ----------------------------------------------------------------------
        print("\n5. initialze worker.")
        if rbdbackup.initialize_backup_worker() == False:
            return
        else:
            workers = rbdbackup.manager.get_worker_pid()
            for name, pid in workers.iteritems():
                print("  - worker = %s, pid = %s" % (name, pid))

        # ----------------------------------------------------------------------
        print("\n6. initialze RBD snapshot task.")
        if rbdbackup.initialize_snapshot_task() == False:
            return
        else:
            snapshot_tasks = rbdbackup.create_snapshot_tasks
            for rbd_id, task in snapshot_tasks.iteritems():
                print("  - rbd id = %s, task name = %s" % (rbd_id, task))

        # ----------------------------------------------------------------------
        print("\n7. start RBD snapshot create task.")
        if rbdbackup.start_snapshot() == False:
            return
        else:
            snapshot_tasks = rbdbackup.create_snapshot_tasks
            for rbd_id, task in snapshot_tasks.iteritems():
                print("  - rbd id = %s, task name = %s, status = %s" % (rbd_id,
                                                                        task,
                                                                        task.task_status))


        #rbdbackup.finalize()
        #return



        # ----------------------------------------------------------------------
        print("\n8. initialize RBD export RBD task.")
        if rbdbackup.initialize_export_task() == False:
            return
        else:
            export_tasks = rbdbackup.export_tasks
            for rbd_id, task in export_tasks.iteritems():
                print("  - rbd id = %s, task name = %s" % (rbd_id, task))

        # ----------------------------------------------------------------------
        print("\n9. start RBD export task.")
        if rbdbackup.start_export() == False:
            return
        else:
            export_tasks = rbdbackup.export_tasks
            for rbd_id, task in export_tasks.iteritems():
                print("  - rbd id = %s, task name = %s, status = %s" % (rbd_id,
                                                                        task,
                                                                        task.task_status))

        # ----------------------------------------------------------------------
        print("\n10, remove exceed RBD snapshot.")
        if rbdbackup.remove_exceed_rbd_snapshot() == False:
            return
        else:
            removed_snapshots = rbdbackup.removed_snapshots
            for snap in removed_snapshots:
                print("  - %s" % snap)

        # ----------------------------------------------------------------------
        print("\n11. remove exceed RBD backup file.")
        if rbdbackup.remove_exceed_backup() == False:
            return
        else:
            deleted_backup = rbdbackup.deleted_backup
            for deleted_path in deleted_backup:
                print("  - %s" % deleted_path)

    except Exception as e:
        exc_type,exc_value,exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
        print e

    finally:
        print("\n12. finalizing RBD backup.")
        rbdbackup.finalize()


if "__main__" == __name__:
    if len(sys.argv) < 1:
        print("Missing cluster name argument!")
        sys.exit()
    #print len(sys.argv)
    #print sys.argv
    sys.exit(main(sys.argv))
