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
from Common.Monitor import Monitor

from Task.RBDExportTask import RBDExportTask
from Task.RBDSnapshotTask import RBDSnapshotTask


class RBDBackup(object):

    def __init__(self):
        self.backup_time = datetime.datetime.now().strftime(DEFAULT_BACKUP_TIME_FORMAT)

        self.cfg = None
        self.log = None

        self.backup_config_file = DEFAULT_BACKUP_CONFIG_FILE
        self.backup_config_section = DEFAULT_BACKUP_CONFIG_SECTION

        # backup information, list of rbd, snapshot, pool of cluster
        self.backup_rbd_list = []
        self.rbd_snap_list = {}
        self.pool_list = {}
        self.backup_type = None

        # size of backup data and backup item count
        self.total_backup_full_size = 0
        self.total_backup_used_size = 0
        self.total_backup_rbd_count = 0

        # tasks generated to execute
        self.create_snapshot_tasks = {}
        self.export_tasks = {}
        self.rm_snapshot_tasks = {}

        # read from metafiles
        self.meta_last_snapshot = {}    # {'rbd_name': snap_name, ...}
        self.meta_snapshot_list = {}

        self.ceph = Ceph()
        self.manager = None
        self.backup_directory =None
        self.metafile = None

        self.openstck_mapping_enabled = False
        self.cache_clean_enabled = False
        self.monitor_enabled = False

    def _get_rbd_id(self, pool_name, rbd_name):
        return "%s_%s_%s" % (self.ceph.cluster_name, pool_name, rbd_name)

    def _clean_cache(self):
        try:
            drop_cache_level = 1
            os.system("sync; sync")
            os.system("echo %s > /proc/sys/vm/drop_caches" % drop_cache_level)
            os.system("sync; sync")
            return True
        except Exception as e:
            print("Error occur when cleaning system cache.")

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

    def _initialize_monitor(self):
        monitor = Monitor()
        return True

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

            if weekday in full_weekdays:
                backup_type = FULL
                self.log.info("today (%s) is in full backup weekdays %s.\n"
                              "do full backup. backup_type = %s"
                              %(weekday, full_weekday, backup_type))

            elif weekday in incr_weekday:
                backup_type = DIFF
                self.log.info("today (%s) is in incremental backup weekdays %s." % (weekday, incr_weekday))

                #
                # read last snapshot name
                self.log.info("read last snapshot name from metafile %s" % LAST_SNAPSHOT_NAME)
                self.meta_last_snapshot = self.metafile.read(LAST_SNAPSHOT_NAME)
                if self.meta_last_snapshot is False:
                    self.log.warning("unable to read metafile %s. set backup type to full backup" % LAST_SNAPSHOT_NAME)
                    backup_type = FULL

                # read last full backup name
                self.log.info("read last full backup name from metafile %s" % LAST_FULL_BACKUP_NAME)
                self.meta_last_full_backup_name = self.metafile.read(LAST_FULL_BACKUP_NAME)
                if self.meta_last_full_backup_name is False:
                    self.log.warning("unable to read metafile %s. set backup type to full backup" % LAST_FULL_BACKUP_NAME)
                    backup_type = FULL

                if backup_type == DIFF:
                    self.log.info("do incremental backup. backup_type = %s" % backup_type)
                else:
                    self.log.info("do full backup. backup_type = %s" % backup_type)
            else:
                self.log.info("no bacakup triggered on today(%s)." % weekday)

            return backup_type
        except Exception as e:
            self.log.error("unable to match backup type. %s"% e)
            return None

    def _get_backup_rbd_list(self):
        '''
        return a list of dict which contain RBD image info
            [ {'id': str
              'pool_name': str,
              'rbd_name': str,
              'rbd_full_size': int,
              'rbd_used_size': int,
              'volume_name': str,
              'snapshot_list': list,
              'features': int}, ... ]
        '''

        def __set_pool(pool_name):
            pool = Pool(self.log, self.ceph.cluster_name, pool_name, self.ceph.conffile)
            if pool.connected is False:
                self.log.error("unable to connect cluster pool %s" % pool_name)
                return False
            else:
                self.log.info("\nconnected to ceph cluster pool %s" % pool_name)
                self.pool_list[pool_name] = pool
                return True

        def __pack_rbd_info(pool_name, rbd_name, volume_name=None):
            self.log.info("\npacking RBD info, pool_name = %s, rbd_name= %s, volume_name = %s" % (pool_name,
                                                                                                  rbd_name,
                                                                                                  volume_name))
            # get id to identify the RBD image cross entire backup process.
            # this id is used to identify the RBD during entire backup states.
            # ----------------------------------------
            rbd_id = self._get_rbd_id(pool_name, rbd_name)
            self.log.info("packing rbd_id is %s" % rbd_id)

            backup_type = self.backup_type
            last_snap = None

            # create RBD backup directory if not exist
            # check RBD backup directory, if no backup exist, set to full backup
            # ----------------------------------------
            rbd_path = self.backup_directory.add_directory(pool_name, rbd_name, full_path=True)
            rbd_directory = Directory(self.log, rbd_path)
            self.log.info("check RBD backup path. rbd_path = %s" % rbd_path)
            if rbd_directory.path != rbd_path:
                self.log.warning("unable to create the backup directory %s, skip this RBD backup." % rbd_path)
                return False

            # generate RBD info, get size of rbd, snapshot list and features from cluster.
            # ----------------------------------------
            self.log.info("get RBD info from cluster.")
            pool = self.pool_list[pool_name]
            rbd_info = {}
            try:
                rbd_info['features'] = pool.get_rbd_features(rbd_name)
                rbd_info['rbd_full_size'] = pool.get_rbd_size(rbd_name)
                rbd_info['rbd_used_size'] = pool.get_used_size(rbd_name, last_snap)  # ?? may change to get used size from last snapshot
                rbd_info['snapshot_list'] = pool.get_snap_name_list(rbd_name)
            except Exception as e:
                self.log.error("unable to get info from ceph cluster. skip this RBD backup.")
                return False

            # when backup type is incremental (export diff), perform additional check.
            # (a). check last snappshot name exist in ceph cluster
            # (b). check last full backup file (last full backup file must exist for doing incremental backup)
            # ----------------------------------------
            if backup_type == DIFF:

                # verify the last snapshot exist in cluster
                if self.meta_last_snapshot.has_key(rbd_id):
                    last_snap = self.meta_last_snapshot[rbd_id]
                    self.log.info("set snapshot name of last backup = %s" % last_snap)

                    if last_snap not in rbd_info['snapshot_list']:
                        self.log.warning("last snapshot name not exist in cluster. set to full backup.")
                        backup_type = FULL
                        last_snap = None

                else:
                    self.log.warning("unable to get last snapshot name. set to full backup.")
                    backup_type = FULL

                # verify last full backup name exist in backup directory
                if self.meta_last_full_backup_name.has_key(rbd_id):
                    full_backup_name = self.meta_last_full_backup_name[rbd_id]
                    self.log.info("last full backup file name is %s" % full_backup_name)

                    if not rbd_directory.find_file(full_backup_name):
                        self.log.warning("unable to find last full backup file in %s. set to full backup." % (rbd_path))
                        backup_type = FULL

                else:
                    self.log.warning("unable to get last full backup name. set to full backup.")
                    backup_type = FULL

            #rbd_info = pool.get_rbd_info(rbd_name, from_snap=last_snap)
            #if rbd_info is False:
            #    self.log.warning("unable to get RBD info. rbd_id = %s." % rbd_id)
            #    return False

            # add additional info to rbd_info
            # ----------------------------------------
            self.log.info("add additonal info of RBD.")
            rbd_info['id'] = rbd_id
            rbd_info['rbd_name'] = rbd_name
            rbd_info['pool_name'] = pool_name
            rbd_info['volume_name'] = volume_name
            rbd_info['backup_type'] = backup_type
            rbd_info['rbd_path'] = rbd_path
            rbd_info['last_snapshot_name'] = last_snap

            # update total size of RBD to backup and RBD count
            # ----------------------------------------
            self.log.info("update total backup size.")
            self.total_backup_full_size += rbd_info['rbd_full_size']
            self.total_backup_used_size += rbd_info['rbd_used_size']
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
            self.cache_clean_enabled = True

        # read cache clean config
        if not cfg.read_cache_config():
            self.log.warning("unable to read clean cache config.")
        else:
            self.monitor_enabled = True

        # set ceph cluster name and conffile if they are not read from argument.
        if self.ceph.conffile is None:
            self.ceph.conffile = cfg.ceph_conffile
        if self.ceph.cluster_name is None:
            self.ceph.cluster_name = cfg.ceph_cluster_name

        # assign cfg to self.cfg
        self.cfg = cfg

        self.log.info("backup config file = %s\n"
                      "backup config section = %s\n"
                      "ceph config file = %s\n"
                      "ceph cluster name = %s"
                      % (self.backup_config_file,
                         self.backup_config_section,
                         self.ceph.conffile,
                         self.ceph.cluster_name))
        return True

    def read_backup_rbd_list(self):
        self.log.start_line(title="\n(2). READ RBD IMAGE LIST TO BACKUP", symbol_count=0)
        self.log.start_line(symbol="-", symbol_count=40)

        # ----------------------------------------
        # get backup type, diff or full backup base on configuration
        # ----------------------------------------
        self.backup_type = self._get_backup_type()
        if self.backup_type is None:
            return False

        backup_rbd_list = self._get_backup_rbd_list()
        if backup_rbd_list is False or len(backup_rbd_list) == 0:
            return False

        # ----------------------------------------
        # sorting RBD backup list if configured
        # ----------------------------------------
        try:
            if self.cfg.backup_small_size_first == 'True':
                self.log.info("\nsort backup RBD image list by used size, small size first.")
                self.backup_rbd_list = sorted(backup_rbd_list, key=lambda k: k['rbd_used_size'])
                #self.backup_rbd_list = sorted(backup_rbd_list, key=lambda k: k['rbd_full_size'])
            elif self.cfg.backup_small_size_first == 'False':
                self.log.info("\nsort backup RBD image list by used size, large size first.")
                self.backup_rbd_list = sorted(backup_rbd_list, key=lambda k: k['rbd_used_size'], reverse=True)
            else:
                self.backup_rbd_list = backup_rbd_list

        except Exception as e:
            self.log.warning("sorting RBD backup list failed. %s" % e)
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            return False

        # ----------------------------------------
        # write metadata, RBD list, snapshot list
        # ----------------------------------------
        try:
            self.log.info("\nwrite backup RBD info to metafile %s" % BACKUP_INFO)
            # write backup info metadata
            backup_info = OrderedDict()
            backup_info['total_rbd_count'] = self.total_backup_rbd_count
            backup_info['total_full_bytes'] = self.total_backup_full_size
            backup_info['total_used_bytes'] = self.total_backup_used_size
            if not self.metafile.update(BACKUP_INFO, backup_info):
                self.log.error("unable to update backup info to metafile.")
                return False

            # write RBD list, snap list
            '''
            meta_backup_list = []
            meta_snapshot_list = {}
            for rbd_info in backup_rbd_list:
                rbd_id = rbd_info['id']
                metadata = {'used': rbd_info['rbd_used_size'],
                            'size': rbd_info['rbd_full_size'],
                            'volume': rbd_info['volume_name'],
                            'features': rbd_info['features']}
                rbd_meta = {rbd_id: metadata}
                meta_backup_list.append(rbd_meta)
                #meta_snapshot_list[rbd_id] = rbd_info['snapshot_list']
            '''

            self.log.info("\nwrite backup RBD list to metafile %s" % RBD_LIST)
            #if not self.metafile.write(RBD_LIST, meta_backup_list, default_flow_style=True, overwrite=True):
            if not self.metafile.write(RBD_LIST, backup_rbd_list, default_flow_style=True, overwrite=True):
                self.log.error("unable to write backup list info to metafile.")
                return False

            '''
            self.log.info("\nwrite snapshot list to metafile %s" % SNAPSHOT_LIST)
            if not self.metafile.write(SNAPSHOT_LIST, meta_snapshot_list, overwrite=True):
                self.log.error("unable to write snapshot list info to metafile.")
                return False
            '''

        except Exception as e:
            self.log.warning("write backup list to metafile failed. %s" % e)
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            return False

        self.log.info("\ntotal %s rbd(s) in RBD backup list\n"
                      "total backup RBD full size = %s bytes\n"
                      "total backup RBD used size = %s bytes\n"
                      % (len(self.backup_rbd_list),
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
        directory = Directory(self.log, path)
        if directory.path is not None:

            # add backup cluster name folder in backup directory if not exist
            # ----------------------------------------------------------------
            try:
                cluster_path = directory.add_directory(self.ceph.cluster_name,
                                                       set_path=True,
                                                       check_size=True,
                                                       full_path=True)
                if cluster_path is False:
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
                self.log.info("set metafile in directory %s" % cluster_path)
                metafile = Metafile(self.log, self.ceph.cluster_name, cluster_path)

                metafiles = [BACKUP_INFO,
                             RBD_LIST,
                             SNAPSHOT_LIST,
                             LAST_SNAPSHOT_NAME,
                             NEW_SNAPSHOT_NAME,
                             LAST_FULL_BACKUP_NAME]
                if metafile.initialize(self.ceph.cluster_name, metafiles):
                    # write backup directory info to metafile
                    backup_info = OrderedDict()
                    backup_info['fsid'] = self.ceph.get_fsid()
                    backup_info['name'] = self.ceph.cluster_name
                    backup_info['time'] = self.backup_time
                    backup_info['backup_dir_avai_bytes'] = self.backup_directory.available_bytes
                    backup_info['backup_dir_used_bytes'] = self.backup_directory.used_bytes
                    if  metafile.write(BACKUP_INFO, backup_info, overwrite=True) is False:
                        return False

                    # read RBD snapshot list
                    self.log.info("read RBD snapshot list from metafile %s" % SNAPSHOT_LIST)
                    self.meta_snapshot_list = metafile.read(SNAPSHOT_LIST)
                    if self.meta_snapshot_list is False:
                        self.log.warning("unable to read metafile %s." % SNAPSHOT_LIST)
                        self.meta_snapshot_list = {}

                self.metafile = metafile
                return True
            except Exception as e:
                self.log.error("metafile fail initialized. %s" %e)
                exc_type,exc_value,exc_traceback = sys.exc_info()
                traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
                return False
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
            return True
        except Exception as e:
            self.log.error("worker fail initialized. %s" % e )
            return False

    def initialize_snapshot_task(self):
        ''' snapshot stage, this is pre stage of export task.
        get RBD information for snapshot/backup.
        the result in this stage will passed to
        '''
        #self.log.info("\n(4). INITIALIZE RBD SNAPSHOT TASKS")
        self.log.start_line(title="\n(4). INITIALIZE RBD SNAPSHOT TASKS", symbol_count=0)
        self.log.start_line(symbol="-", symbol_count=40)
        #self.log.set_stage(4)

        try:
            # currently not used...
            protect = self.cfg.snapshot_protect
            if protect == 'True':
                snap_protect = True
            else:
                snap_protect = False
            self.log.info("RBD snapshot protect = %s " % snap_protect)

            for rbd_info in self.backup_rbd_list:
                #print("%s, %s" % (backup_rbd['pool_name'], backup_rbd['rbd_name']))
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

                snapshot_task = RBDSnapshotTask(self.ceph.cluster_name,
                                                rbd_info['pool_name'],
                                                rbd_info['rbd_name'],
                                                action=CREATE,
                                                protect=snap_protect,
                                                rbd_id=rbd_id)
                self.create_snapshot_tasks[rbd_id] = snapshot_task
                self.log.info("created RBD snapshot task. task_name = %s" % snapshot_task)

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
        for rbd_info in self.backup_rbd_list:
            try:
                rbd_id = rbd_info['id']
                self.manager.add_task(self.create_snapshot_tasks[rbd_id])
                submitted_task_count += 1
            except Exception as e:
                self.log.error("unable to submit snapshot task(%s) to worker manager. %s" % (rbd_id, e))
                continue

        # Collect finished RBD snapshot tasks
        # after add all snapshot tasks, wait them completed before move on next
        # keep getting completed task and verify status
        # if snapshot task failed (error), remove it from backup list
        # ----------------------------------------------------------------------
        new_snapshot_name = {}
        snapshot_list = {}
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

                    # get new snapshot name of RBD
                    new_snapshot_name[task.rbd_id] = task.snap_name

                    # get new snapshot list.
                    pool = self.pool_list[task.pool_name]

                    #snap_id = pool.get_rbd_snap_id(task.rbd_name, task.snap_name)
                    if self.meta_snapshot_list.has_key(task.rbd_id):
                        meta_snapshot_list = self.meta_snapshot_list[task.rbd_id]
                        if isinstance(meta_snapshot_list, list):
                            meta_snapshot_list.append(task.snap_name)
                        else:
                            meta_snapshot_list = [task.snap_name]
                    else:
                        meta_snapshot_list = [task.snap_name]

                    self.meta_snapshot_list[task.rbd_id] = meta_snapshot_list

                else:
                    # remove this backup item from backup list if snapshot failed
                    # todo: use better way to remove the item in list
                    self.log.warning("%s is not completed. remove it from backup list." % task.name)
                    self.backup_rbd_list = [i for i in self.backup_rbd_list if i.id != task.rbd_id]
                    self.log.info("%s backup item left in RBD backup list." % len(self.backup_rbd_list))
                    uncompleted_task_count += 1

                if submitted_task_count == completed_task_count+uncompleted_task_count:
                    break

            except Exception as e:
                self.log.error("unable to check snapshot result task. %s" % e)
                uncompleted_task_count += 1

        # update new snapshot name and snapshot list
        # ---------------------------------------
        try:
            if not self.metafile.write(NEW_SNAPSHOT_NAME, new_snapshot_name, overwrite=True):
                self.log.warning("unable to write metafile %s" % NEW_SNAPSHOT_NAME)
        except Exception as e:
            self.log.error("unable write new snapshot info to metefile. %s" % e)
            return False

        self.log.info("\n%s submitted snapshot task.\n"
                      "%s completed snapshot task.\n"
                      "%s uncompleted snapshot task."
                      % (submitted_task_count,
                         completed_task_count,
                         uncompleted_task_count))
        return True

    def initialize_export_task(self):
        ''' this is export stage, all snahshot tasks in snapshot stage must
            be done before do export task
            check backup type, to generate full or incremental export task
        '''
        #self.log.info("\n(6). INITIALIZE RBD EXPORT TASKS")
        self.log.start_line(title="\n(6). INITIALIZE RBD EXPORT TASKS", symbol_count=0)
        self.log.start_line(symbol="-", symbol_count=40)
        #self.log.set_stage(6)

        try:

            # create RBD export tasks
            # ==================================================================
            for rbd_info in self.backup_rbd_list:
                rbd_id = rbd_info['id']
                pool_name = rbd_info['pool_name']
                rbd_name = rbd_info['rbd_name']
                snap_list = rbd_info['snapshot_list']
                backup_type = rbd_info['backup_type']
                rbd_path = rbd_info['rbd_path']
                last_snapshot_name = rbd_info['last_snapshot_name']

                self.log.info("\ncreating RBD export task. rbd_id = %s" % rbd_id)

                # get snapshot name from the snapshot tasks in this backup
                # this snapshot was created in previous stage
                # ----------------------------------------
                snapshot_task = self.create_snapshot_tasks[rbd_id]
                to_snap = snapshot_task.snap_name
                if not isinstance(to_snap, str):
                    self.log.warning("invalid snapshot name from snapshot task, skip this RBD backup")
                    continue

                # produce export destination file path
                # if unable to get snapshot name from last backup, do full backup
                # ----------------------------------------
                if backup_type == DIFF:
                    from_snap = last_snapshot_name
                    export_filename = ''.join([from_snap, '_to_', to_snap])
                    export_destpath = os.path.join(rbd_path, export_filename)

                if backup_type == FULL:
                    from_snap = None
                    export_filename = to_snap
                    export_destpath = "%s/%s" %(rbd_path, export_filename)

                # logging export task setting
                # ----------------------------------------
                task_info = dict()
                task_info['cluster'] = self.ceph.cluster_name
                task_info['pool'] = pool_name
                task_info['rbd'] = rbd_name
                task_info['export_destpath'] = export_destpath
                task_info['backup_type'] = EXPORT_TYP[backup_type]
                task_info['from_snap'] = from_snap
                task_info['to_snap'] = to_snap
                self.log.info(("RBD export task setting:", task_info))

                # create RBDExportTask
                # ----------------------------------------
                if backup_type in [FULL, DIFF]:
                    export_task = RBDExportTask(self.ceph.cluster_name,
                                                pool_name,
                                                rbd_name,
                                                export_destpath,
                                                export_type=backup_type,
                                                from_snap=from_snap,
                                                to_snap=to_snap)

                # save created export tasks
                # ----------------------------------------
                export_task.rbd_id = rbd_id
                self.export_tasks[rbd_id] = export_task
                self.log.info("created RBD export task. task name = %s" % export_task.name)

            self.log.info("\ntotal %s export tasks created." % len(self.export_tasks))
            return True
        except Exception as e:
            self.log.error("unable to create export task. %s" % e)
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            sys.exit(2)
            return False

    def start_export(self):
        #self.log.info("\n(7). START RBD EXPORT TASKS")
        self.log.start_line(title="\n(7). START RBD EXPORT TASKS", symbol_count=0)
        self.log.start_line(symbol="-", symbol_count=40)
        #self.log.set_stage(7)

        submitted_task_count = 0
        completed_task_count = 0
        uncompleted_task_count = 0

        # Submit export task to workers
        # ---------------------------------------------------------------------
        for rbd_info in self.backup_rbd_list:
            try:
                rbd_id = rbd_info['id']

                self.manager.add_task(self.export_tasks[rbd_id])
                submitted_task_count += 1
            except Exception as e:
                self.log.error("unable to submit export task (id = %s) to worker manager. %s" % (rbd_id, e))
                continue

        if submitted_task_count == 0:
            self.log.error("no any RBD export task submitted.")
            return False

        # Collect finished RBD export tasks
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

                    # track full backup filename
                    export_type = task.export_type
                    if export_type == FULL:
                        export_destpath = task.export_destpath
                        new_full_export_filename[task.rbd_id] = os.path.basename(export_destpath)
                else:
                    self.log.warning("%s is not completed" % task.name)
                    uncompleted_task_count += 1

                if submitted_task_count == completed_task_count + uncompleted_task_count:
                    break

            except Exception as e:
                self.log.error("unable to check export result task. %s" % e)
                uncompleted_task_count += 1
                #time.sleep(1)

        # if has new full backup, update new full backup filename to metadata
        # ----------------------------------------
        try:
            if not self.metafile.update(LAST_FULL_BACKUP_NAME, new_full_export_filename):
                self.log.warning("unable to write metafile %s" % NEW_SNAPSHOT_NAME)
        except Exception as e:
            self.log.error("unable update full backup filename to metefile. %s" % e)
            return False

        self.log.info("\n%s submitted export task.\n"
                      "%s completed export task.\n"
                      "%s uncompleted export task."
                      % (submitted_task_count,
                         completed_task_count,
                         uncompleted_task_count))
        return True

    def initialize_rm_snapshot_task(self):
        self.log.start_line(title="\n(8) INITIALIZE REMOVE SNAPSHOT TASK", symbol_count=0)
        self.log.start_line(symbol="-", symbol_count=40)

        try:
            snap_retain_count = self.cfg.snapshot_retain_count
            self.log.info("number of snapshot retain in RBD is %s" % snap_retain_count)

            snap_purge = False
            if int(snap_retain_count) == 0:
                self.log.warning("removing all snapshots of rbd.")
                snap_purge = Ture

            rm_snapshot_task_count = 0
            for rbd_info in self.backup_rbd_list:
                rbd_id = rbd_info['id']
                pool_name = rbd_info['pool_name']
                rbd_name = rbd_info['rbd_name']
                snap_list = rbd_info['snapshot_list'] # ???

                self.log.info("\ncreating RBD snapshot remove task. rbd_id = %s" % rbd_id)

                # reuse previous snapshot task to delete snapshot.
                snapshot_task = self.create_snapshot_tasks[rbd_id]

                # get number of snapshot in the rbd.
                if snap_purge:
                    self.log.info("purge all snapshots in rbd. rbd_id = %s" % rbd_id)
                    snapshot_task.action = PURGE

                else:
                    pool = self.pool_list[pool_name]
                    pool_snap_name_list = pool.get_snap_name_list(rbd_name)
                    self.log.info(("snapshot name list in cluster:", pool_snap_name_list))

                    # compare snapshot list between cluster and metafile
                    # generated matched snapshot name list
                    rbd_snap_count = 0
                    matched_snapshot_list = []
                    if self.meta_snapshot_list.has_key(rbd_id):

                        meta_rbd_snap_list = self.meta_snapshot_list[rbd_id]
                        self.log.info(("snapshot name list in metafile:", meta_rbd_snap_list))

                        for snap_name in meta_rbd_snap_list:
                            if snap_name in pool_snap_name_list:
                                rbd_snap_count += 1
                                matched_snapshot_list.append(snap_name)

                    if snap_retain_count >= rbd_snap_count:
                        self.log.info("do not need to remove snapshot. number of snapshot = %s" % rbd_snap_count)
                        continue

                    else:
                        # read snapshot info from metafile
                        # if no snapshot info found, just remove snapshot from last backup

                        snapshot_task.action = REMOVE
                        snapshot_task.snap_name = matched_snapshot_list[0]  # get oldest snapshot name from metafile

                        task_info = dict()
                        task_info['cluster'] = self.ceph.cluster_name
                        task_info['pool'] = rbd_info['pool_name']
                        task_info['rbd'] = rbd_info['rbd_name']
                        task_info['action'] = SNAP_ACT[snapshot_task.action]
                        task_info['snap_name'] = snapshot_task.snap_name
                        self.log.info(("RBD snapshot remove task setting:", task_info))

                        # set matched snapshot list for metafile
                        self.meta_snapshot_list[rbd_id] = matched_snapshot_list

                self.rm_snapshot_tasks[rbd_id] = snapshot_task
                rm_snapshot_task_count += 1
                self.log.info("created RBD snapshot remove task. task_name = %s" % snapshot_task)

            if rm_snapshot_task_count == 0:
                return False

            self.log.info("\ntotal %s remove snapshot tasks created." % rm_snapshot_task_count)
            return True
        except Exception as e:
            self.log.error("unable to create snapshot task. %s" % e)
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            return False

    def start_rm_snapshot_task(self):
        self.log.start_line(title="\n(9) START RM SNAPSHOT TASK", symbol_count=0)
        self.log.start_line(symbol="-", symbol_count=40)

        try:
            submitted_task_count = 0
            completed_task_count = 0
            uncompleted_task_count = 0

            # use one worker to remove snapshot one by one
            for rbd_id, rm_snapshot_task in self.rm_snapshot_tasks.iteritems():
                self.manager.add_task(rm_snapshot_task)

                submitted_task_count += 1

                task = self.manager.get_finished_task()

                if task.task_status == COMPLETE:
                    completed_task_count += 1
                    meta_rbd_snap_list = self.meta_snapshot_list[rbd_id]
                    meta_rbd_snap_list.pop(0)
                    self.meta_snapshot_list[rbd_id] = meta_rbd_snap_list
                else:
                    uncompleted_task_count += 1

            if not self.metafile.write(SNAPSHOT_LIST, self.meta_snapshot_list):
                self.log.error("unable to write snapshot list to metafile %s" % SNAPSHOT_LIST)

            print("submitted rm snapshot task count = %s\n"
                  "completed rm snapshot task count = %s\n"
                  "uncompleted rm snapshot task count = %s"
                  % (submitted_task_count, completed_task_count, uncompleted_task_count))
            return True
        except Exception as e:
            self.log.error("unable remove snapshot. %s" % e)
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            return False

    def delete_exceed_backup(self):
        self.log.info("\n(10) DELETE EXCEED BACKUP FILE")

        retain_count = self.cfg.backup_retain_count

        return


    def finalize(self):
        self.log.start_line(title="\n(11) FINALIZE RBD BACKUP", symbol_count=0)
        self.log.start_line(symbol="-", symbol_count=40)

        # stop worker processes
        if self.manager is not None:
            self.manager.stop_worker()

            # check worker status
            # make sure all worker stopped
            countdown = 5
            workers_status = self.manager.get_workers_status()
            running_worker = self.manager.worker_count
            while True:
                for name, status in workers_status.iteritems():
                    #print name, status
                    if status == STOP or status == READY:
                        self.manager.worker_count -= 1
                        continue
                    self.log.warning("%s is not stopped yet. status=%s" % (name, status))

                if self.manager.worker_count == 0:
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

        #self.log.info(("RBD backup completed.", summary))



def main(argument_list):

    print("\n******** Start RBD Backup ********")

    rbdbackup = RBDBackup()

    try:
        print("Step 1 - read RBD backup argument list.")
        if rbdbackup.read_argument_list(argument_list) == False:
            return

        print("Step 2 - read config file options. (Logging will start after loging option read successfully.)")
        if rbdbackup.read_config_file() == False:
            return

        print("Step 3 - initialze backup directory.")
        if rbdbackup.initialize_backup_directory() == False:
            return

        print("Step 4 - read RBD list to backup.")
        if rbdbackup.read_backup_rbd_list() == False:
            return

        print("Step 5 - initialze worker.")
        if rbdbackup.initialize_backup_worker() == False:
            return

        print("Step 6 - initialze RBD snapshot task.")
        if rbdbackup.initialize_snapshot_task() == False:
            return

        print("Step 7 - start RBD snapshot create task.")
        if rbdbackup.start_snapshot() == False:
            return

        print("Step 8 - initialize RBD export RBD task.")
        if rbdbackup.initialize_export_task() == False:
            return

        print("Step 9 - start RBD export task.")
        if rbdbackup.start_export() == False:
            return

        print("Step 10 - initialize RBD snapshot remove task")
        if rbdbackup.initialize_rm_snapshot_task() == False:
            return

        print("Step 11 - start RBD snapshot remove task")
        if rbdbackup.start_rm_snapshot_task() == False:
            return

        print("\nFinish...")

    except Exception as e:
        print e

    finally:
        rbdbackup.finalize()


if "__main__" == __name__:
    if len(sys.argv) < 1:
        print("Missing cluster name argument!")
        sys.exit()
    #print len(sys.argv)
    #print sys.argv
    sys.exit(main(sys.argv))
