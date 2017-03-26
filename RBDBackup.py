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
        self.backup_list = []
        self.rbd_snap_list = {}
        self.pool_list = {}
        self.backup_type = None

        # size of backup data and backup item count
        self.total_backup_full_size = 0
        self.total_backup_used_size = 0
        self.total_backup_rbd_count = 0

        # tasks generated to execute
        self.snapshot_tasks = {}
        self.export_tasks = {}

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
                self.log.info("today (%s) is in incremental backup weekdays %s.\n"
                              "do incremental backup. backup_type = %s"
                              %(weekday, incr_weekday, backup_type))
            else:
                self.log.info("no bacakup triggered on today(%s)." % weekday)

            return backup_type
        except Exception as e:
            self.log.error("unable to match backup type. %s"% e)
            return None

    def _get_backup_list(self):
        '''
        return a list of dict which contain rbd image info
            [ {'id': str
              'pool_name': str,
              'rbd_name': str,
              'rbd_full_size': int,
              'rbd_used_size': int,
              'volume_name': str,
              'snapshot_list': list,
              'features': int}, ... ]
        '''
        def __pack_rbd_info(pool_name, rbd_name, volume_name=None):
            # an id to identify the rbd image cross entire backup process.
            rbd_id = self._get_rbd_id(pool_name, rbd_name)
            pool = self.pool_list[pool_name]

            rbd_info = pool.get_rbd_info(rbd_name)
            if rbd_info is False:
                self.log.warning("unable to get rbd info. skip backup of %s." % rbd_name)
                return False

            rbd_full_size = rbd_info['size']
            rbd_used_size = rbd_info['used']
            rbd_features  = rbd_info['features']

            self.total_backup_full_size += rbd_full_size
            self.total_backup_used_size += rbd_used_size
            self.total_backup_rbd_count += 1

            new_rbd_info = {'id': rbd_id,
                            'pool_name': pool_name,
                            'rbd_name': rbd_name,
                            'rbd_full_size': rbd_full_size,
                            'rbd_used_size': rbd_used_size,
                            'volume_name': volume_name,
                            'snapshot_list': pool.get_snap_name(rbd_name),
                            'features': rbd_features}

            '''
            snap_info_list = pool.get_snap_info(rbd_name)
            if snap_info_list is False:
                self.log.warning("unable to get snapshot list of rbd image %s. skip backup of it." % rbd_name)
                return False
            return (new_rbd_info, snap_info_list)
            '''
            return new_rbd_info

        try:
            # store all backup rbd image information
            rbd_list = []

            openstack_mapping = self.cfg.openstack_enable_mapping
            self.log.info("openstack enable mapping is %s" % openstack_mapping)

            if openstack_mapping is 'True':
                yaml_path = self.cfg.openstack_yaml_filepath
                yaml_section = self.cfg.openstack_section_name
                distribution = self.cfg.openstack_distribution
                pool_name = self.cfg.openstack_pool_name

                self.log.info("read backup list from %s, section = %s" %(yaml_path, yaml_section))
                yaml = Yaml(self.log, yaml_path)
                yaml_data = yaml.read(yaml_section)
                if yaml_data is not False:
                    openstack = OpenStack(self.log,
                                          yaml_data=yaml_data,
                                          distribution=distribution)

                    if openstack.set_cinder_client():
                        self.log.info("openstack cinder client connectted.")

                        pool = Pool(self.log, self.ceph.cluster_name, pool_name, self.ceph.conffile)
                        if not pool.collect_info():
                            return False
                        self.pool_list[pool_name] = pool

                        volumes = openstack.get_cinder_volume()
                        for volume_name, volume_id in volumes.iteritems():
                            rbd_info = __pack_rbd_info(pool_name, volume_id, volume_name)
                            if rbd_info is not False:
                                rbd_list.append(rbd_info)
                                #self.rbd_snap_list[volume_id] = rbd_info[1]

            else:
                yaml_path = self.cfg.backup_yaml_filepath
                yaml_section = self.cfg.backup_yaml_section_name
                self.log.info("read backup list from %s, section = %s" %(yaml_path, yaml_section))
                yaml = Yaml(self.log, yaml_path)
                yaml_data = yaml.read(yaml_section)
                if yaml_data is not False:
                    for pool_name, rbd_name_list in yaml_data.iteritems():
                        pool = Pool(self.log, self.ceph.cluster_name, pool_name, self.ceph.conffile)
                        if not pool.collect_info():
                            return False
                        self.pool_list[pool_name] = pool

                        for rbd_name in rbd_name_list:
                            rbd_info = __pack_rbd_info(pool_name, rbd_name)
                            if rbd_info is not False:
                                rbd_list.append(rbd_info)
                                #self.rbd_snap_list[rbd_name] = rbd_info[1]

            # if no rbd image get, nothing to do next, return false.
            if self.total_backup_rbd_count == 0:
                self.log.info("no rbd image to backup.")
                return False

            # verify sufficient spaces size for backup, if not, return false.
            # we use full rbd image size rather than actually used size.
            # if self.backup_directory.available_bytes <= self.total_backup_used_size:
            if self.backup_directory.available_bytes <= self.total_backup_full_size:
                self.log.info("no enough space size for backup.\n"
                              "total backup rbd image size = %s bytes\n"
                              "available backup space size = %s bytes\n"
                              "need %s bytes more space size."
                              % (self.total_backup_full_size,
                                 self.backup_directory.available_bytes,
                                (self.total_backup_full_size - self.backup_directory.available_bytes)))
                return False

            backup_info = OrderedDict()
            backup_info['total_rbd_count'] = self.total_backup_rbd_count
            backup_info['total_full_bytes'] = self.total_backup_full_size
            backup_info['total_used_bytes'] = self.total_backup_used_size
            if not self.metafile.update(BACKUP_INFO, backup_info):
                self.log.error("unable to update backup info to metafile.")
                return False

            self.log.info("total %s rbd(s) in rbd backup list\n"
                          "total backup rbd full size = %s bytes\n"
                          "total backup rbd used size = %s bytes\n"
                          % (len(rbd_list),
                             self.total_backup_full_size,
                             self.total_backup_used_size))

            return rbd_list
        except Exception as e:
            self.log.error("unable to get rbd image list for backup. %s" % e)
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            return False

        finally:
            pool.close()

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
            self.log.error("unable to read rbd backup config.")
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
        #self.log.info("\n(2). READ RBD IMAGE LIST TO BACKUP")
        self.log.start_line(title="\n(2). READ RBD IMAGE LIST TO BACKUP", symbol_count=0)
        self.log.start_line(symbol="-", symbol_count=40)
        #self.log.set_stage(1)

        # get backup list from backup_list config or openstack yaml config
        backup_list = self._get_backup_list()
        if backup_list is False:
            return False

        # sorting rbd backup list if configured
        try:
            if self.cfg.backup_small_size_first == 'True':
                self.log.info("sort backup rbd image list by used size, small size first.")
                self.backup_list = sorted(backup_list, key=lambda k: k['rbd_used_size'])
                #self.backup_list = sorted(backup_list, key=lambda k: k['rbd_full_size'])
            elif self.cfg.backup_small_size_first == 'False':
                self.log.info("sort backup rbd image list by used size, large size first.")
                self.backup_list = sorted(backup_list, key=lambda k: k['rbd_used_size'], reverse=True)
            else:
                self.backup_list = backup_list

        except Exception as e:
            self.log.warning("sorting rbd backup list failed. %s" % e)
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            return False

        # write metadata, rbd list, snapshot list
        try:
            rbd_meta_list = []

            #snap_meta_list = []

            for rbd_info in self.backup_list:

                metadata = {'used': rbd_info['rbd_used_size'],
                            'size': rbd_info['rbd_full_size'],
                            'volume': rbd_info['volume_name'],
                            'features': rbd_info['features']}

                rbd_meta = {rbd_info['id']: metadata}
                rbd_meta_list.append(rbd_meta)

                '''
                rbd_name = rbd_info['rbd_name']
                snap_info_list = self.rbd_snap_list[rbd_name]
                for snap_info in snap_info_list:
                    rbd_snap_name = "%s@%s" % (rbd_info['rbd_name'], snap_info['name'])
                    snap_meta = "[%s] id=%s, size=%s/%s" % (rbd_snap_name,
                                                            snap_info['id'],
                                                            snap_info['used'],
                                                            snap_info['size'])
                    snap_meta_list.append(snap_meta)
                '''

            self.metafile.clear(RBD_LIST)
            if not self.metafile.write(RBD_LIST, rbd_meta_list, default_flow_style=True, overwrite=True):
                self.log.error("unable to update backup list info to metafile.")
                return False

            '''
            self.metafile.clear(SNAPSHOT_LIST)
            if not self.metafile.write(SNAPSHOT_LIST, snap_meta_list, overwrite=True):
                self.log.error("unable to update snapshot list info to metafile.")
                return False
            '''

            return True
        except Exception as e:
            self.log.warning("write backup list to metafile failed. %s" % e)
            return False
        #self.backup_list = backup_list
        #return True

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
                             LAST_SNAPSHOT_NAME,
                             NEW_SNAPSHOT_NAME,
                             MAINTAIN_SNAPSHOT_NAME,
                             SNAPSHOT_LIST]
                if metafile.initialize(self.ceph.cluster_name, metafiles):
                    backup_info = OrderedDict()
                    backup_info['fsid'] = self.ceph.get_fsid()
                    backup_info['name'] = self.ceph.cluster_name
                    backup_info['time'] = self.backup_time
                    backup_info['backup_dir_avai_bytes'] = self.backup_directory.available_bytes
                    backup_info['backup_dir_used_bytes'] = self.backup_directory.used_bytes
                    if not metafile.write(BACKUP_INFO, backup_info, overwrite=True):
                        return False
                    self.metafile = metafile

                    # todo: read metadata

                    self.last_snapshot = self.metafile.read(LAST_SNAPSHOT_NAME)



                    return True
                else:
                    return False
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
        get rbd information for snapshot/backup.
        the result in this stage will passed to
        '''
        #self.log.info("\n(4). INITIALIZE RBD SNAPSHOT TASKS")
        self.log.start_line(title="\n(4). INITIALIZE RBD SNAPSHOT TASKS", symbol_count=0)
        self.log.start_line(symbol="-", symbol_count=40)
        #self.log.set_stage(4)

        try:
            protect = self.cfg.snapshot_protect
            if protect == 'True':
                snap_protect = True
            else:
                snap_protect = False

            self.log.info("rbd snapshot protect = %s " % snap_protect)

            for rbd_info in self.backup_list:
                rbd_id = rbd_info['id']
                #print("%s, %s" % (backup_rbd['pool_name'], backup_rbd['rbd_name']))

                self.log.info("\ncreating RBD snapshot create task. rbd_id = %s" % rbd_id)

                task_info = dict()
                task_info['cluster'] = self.ceph.cluster_name
                task_info['pool'] = rbd_info['pool_name']
                task_info['rbd'] = rbd_info['rbd_name']
                task_info['action'] = SNAP_ACT[CREATE]
                task_info['protect'] = snap_protect
                self.log.info(("rbd snapshot task setting:", task_info))

                snapshot_task = RBDSnapshotTask(self.ceph.cluster_name,
                                                rbd_info['pool_name'],
                                                rbd_info['rbd_name'],
                                                action=CREATE,
                                                protect=snap_protect)
                snapshot_task.id = rbd_id
                self.snapshot_tasks[rbd_info['id']] = snapshot_task
                self.log.info("created RBD snapshot task. task_name = %s" % snapshot_task)

            if len(self.snapshot_tasks) == 0:
                self.log.warning("there is no any snapshot task initialized.")
                return False

            self.log.info("\ntotal %s snapshot tasks created." % len(self.snapshot_tasks))
            return True
        except Exception as e:
            self.log.error("unable to create snapshot task. %s" % e)
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            sys.exit(2)
            return False

    def start_snapshot(self):
        #self.log.info("\n(5). START RBD SNAPSHOT TASKS")
        self.log.start_line(title="\n(5). START RBD SNAPSHOT TASKS", symbol_count=0)
        self.log.start_line(symbol="-", symbol_count=40)
        #self.log.set_stage(5)

        submitted_task_count = 0
        completed_task_count = 0
        uncompleted_task_count = 0

        # Submit snapshot task to workers
        # ---------------------------------------------------------------------
        for rbd_info in self.backup_list:
            try:
                rbd_id = rbd_info['id']
                self.manager.add_task(self.snapshot_tasks[rbd_id])
                submitted_task_count += 1
            except Exception as e:
                self.log.error("unable to submit snapshot task(%s) to worker manager. %s" % (rbd_id, e))
                continue

        # Collect finished snapshot tasks
        # after add all snapshot tasks, wait them completed before move on next
        # keep getting completed task and verify status
        # if snapshot task is failed (error), remove it from backup list
        # ----------------------------------------------------------------------
        while True:
            try:
                task = None
                # retrieve finished task
                # ----------------------------------------
                task = self.manager.get_result_task()
                self.snapshot_tasks[task.id] = task

                if task.task_status == COMPLETE:
                    self.log.info("%s is completed. spend %s seconds." % (task.name, task.elapsed_time))
                    completed_task_count += 1

                    # update new snapshot name to metadata
                    if not self.metafile.update(NEW_SNAPSHOT_NAME, {task.id: task.snap_name}):
                        self.log.warning("unable to update metafile %s" % NEW_SNAPSHOT_NAME)
                else:
                    # remove this backup item from backup list if snapshot failed
                    # todo:
                    #        use better way to remove the item in list
                    self.log.warning("%s is not completed. remove it from backup list." % task.name)
                    self.backup_list = [i for i in self.backup_list if i.id != task.id]
                    self.log.info("%s backup item left in rbd backup list." % len(self.backup_list))
                    uncompleted_task_count += 1

                if submitted_task_count == completed_task_count+uncompleted_task_count:
                    break

            except Exception as e:
                self.log.error("unable to check snapshot result task. %s" % e)
                uncompleted_task_count += 1

            finally:
                # update task result to metadata
                # ----------------------------------------
                if task is not None:
                    self.log.info(("task result of %s." % task.name, task.result))

        '''
        try:
            for task_id, task in self.snapshot_tasks.iteritems():
                if not self.metafile.write(TASK_RESULT, {task.name: task.result}):
                    self.log.warning("unable to update metafile %s" % TASK_RESULT)
        except Exception as e:
            self.log.error("unable to write task result to metafile. %s" % e)
        '''

        # all snapshot tasks are collected, then save metadata.
        # ----------------------------------------------------------------------
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
            rbd_directories = {}    # temp store directory info

            # get backup type, diff or full backup base on configuration
            # ==================================================================
            self.backup_type = self._get_backup_type()
            if self.backup_type is None:
                return False

            # get last snapshot name from metafile. if False, set to full backup
            # this snapshot was created from last backup
            # ==================================================================
            backup_type = self.backup_type
            if backup_type != FULL:
                self.meta_last_snapshot = self.metafile.read(LAST_SNAPSHOT_NAME)
                if self.meta_last_snapshot is False:
                    self.log.warning("unable to read metafile %s. set backup type to full backup" % LAST_SNAPSHOT_NAME)
                    backup_type = FULL

            # create RBD export tasks
            # ==================================================================
            for rbd_info in self.backup_list:
                rbd_id = rbd_info['id']
                pool_name = rbd_info['pool_name']
                rbd_name = rbd_info['rbd_name']
                snap_list = rbd_info['snapshot_list']

                self.log.info("\ncreating RBD export task. rbd_id = %s" % rbd_id)

                # create rbd backup directory if not exist
                # check rbd backup directory, if no backup exist, set to full backup
                # ----------------------------------------
                rbd_path = self.backup_directory.add_directory(pool_name, rbd_name, full_path=True)
                if rbd_directories.has_key(rbd_path) is False:
                    rbd_directory = Directory(self.log, rbd_path)

                    if rbd_directory.path != rbd_path:
                        self.log.warning("unable to create the backup directory %s, skip this rbd backup" % rbd_path)
                        continue

                # get snapshot name from the snapshot tasks in this backup
                # this snapshot was created in previous stage
                # ----------------------------------------
                snapshot_task = self.snapshot_tasks[rbd_id]
                to_snap = snapshot_task.snap_name
                if not isinstance(to_snap, str):
                    self.log.warning("invalid snapshot name from snapshot task, skip this rbd backup")
                    continue

                # produce export destination file path
                # if unable to get snapshot name from last backup, do full backup
                # ----------------------------------------
                if backup_type == DIFF:
                    from_snap = self.meta_last_snapshot[rbd_id]    # ** get name snpashot created from last backup
                    file_count = rbd_directory.get_file_list(get_count=True)

                    if from_snap not in snap_list:
                        self.log.warning("snapshot %s is not exist in cluster, set to full backup." % from_snap)
                        backup_type = FULL
                    elif file_count == 0 or file_count is False:
                        self.log.info("no any backup file exist in %s, set to full backup." % rbd_path)
                        backup_type = FULL
                    else:
                        export_filename = ''.join(['from_', from_snap, '_to_', to_snap])
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
                self.log.info(("rbd export task setting:", task_info))

                # create RBDExportTask
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
                export_task.id = rbd_id
                self.export_tasks[rbd_id] = export_task
                self.log.info("created RBD export task. task name = %s" % export_task.name)


            #
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
        for rbd_info in self.backup_list:
            try:
                rbd_id = rbd_info['id']

                self.manager.add_task(self.export_tasks[rbd_id])
                submitted_task_count += 1
            except Exception as e:
                self.log.error("unable to submit export task (id = %s) to worker manager. %s" % (rbd_id, e))
                continue

        if submitted_task_count == 0:
            self.log.error("no any rbd export task submitted.")
            return False

        # Collect finished export tasks
        # ----------------------------------------------------------------------
        while True:
            try:
                task = None
                # retrieve finished task
                # ----------------------------------------
                task = self.manager.get_result_task()
                self.export_tasks[task.id] = task

                if task.task_status == COMPLETE:
                    self.log.info("%s is completed. spend %s seconds." % (task.name, task.elapsed_time))
                    completed_task_count += 1
                else:
                    self.log.warning("%s is not completed" % task.name)
                    uncompleted_task_count += 1

                if submitted_task_count == completed_task_count + uncompleted_task_count:
                    break

            except Exception as e:
                self.log.error("unable to check export result task. %s" % e)
                uncompleted_task_count += 1
                time.sleep(1)

            finally:
                # update task result to metadata
                # ----------------------------------------
                if task is not None:
                    self.log.info(("task result of %s." % task.name, task.result))

                    # update task result metadata
                    #if not self.metafile.update(TASK_RESULT, {task.name: task.result}):
                    #    self.log.warning("unable to update metafile %s" % TASK_RESULT)


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


            for rbd_info in self.backup_list:
                rbd_id = rbd_info['id']
                pool_name = rbd_info['pool_name']
                rbd_name = rbd_info['rbd_name']
                snap_list = rbd_info['snapshot_list']

                self.log.info("\ncreating RBD snapshot remove task. rbd_id = %s" % rbd_id)

                # get snapshot info from pool
                #
                pool = self.pool_list[pool_name]
                snap_info = pool.get_snap_info(rbd_name)


                # reuse previous snapshot task to delete snapshot by last backup
                snapshot_task = self.snapshot_tasks[rbd_id]

                # get number of snapshot in the rbd.
                if snap_purge:
                    self.log.info("purge all snapshots in rbd. rbd_id = %s" % rbd_id)
                    snapshot_task.action = PURGE
                else:
                    snap_count = len(snap_list) + 1

                    if snap_retain_count > snap_count:
                        self.log.info("snapshot count less then retain count. no removing required.\n"
                                      "%s snapshots exist in rbd %s." % (snap_count, rbd_name))
                        continue
                    else:
                        self.log.info("preparing snapshot delete ...")
                        # read snapshot info from metafile
                        # if no snapshot info found, just remove snapshot from last backup

                        export_task = self.export_tasks[rbd_id]

                        # todo: should change to find last snapshot name,
                        #
                        from_snap_m = self.meta_last_snapshot[rbd_id]
                        from_snap_t = export_task.from_snap
                        print from_snap_m, from_snap_t

                        snapshot_task.action = DELETE
                        snapshot_task.snap_name = from_snap


                task_info = dict()
                task_info['cluster'] = self.ceph.cluster_name
                task_info['pool'] = rbd_info['pool_name']
                task_info['rbd'] = rbd_info['rbd_name']
                task_info['action'] = SNAP_ACT[snapshot_task.action]
                task_info['snap_name'] = from_snap
                self.log.info(("rbd snapshot remove task setting:", task_info))

                self.snapshot_tasks[rbd_id] = snapshot_task

                self.log.info("created RBD snapshot remove task. task_name = %s" % snapshot_task)
                self.manager.add_task(snapshot_task)


                # update last snapshot name to metadata
                if not self.metafile.update(LAST_SNAPSHOT_NAME, {rbd_id: task.to_snap}):
                    self.log.warning("unable to update metafile %s" % LAST_SNAPSHOT_NAME)

            return True
        except Exception as e:
            self.log.error("unable to create snapshot task. %s" % e)
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            return False

    def start_rm_snapshot_task(self):
        self.log.info("\n(9) START RM SNAPSHOT TASK")
        return

    def delete_exceed_backup(self):
        self.log.info("\n(10) DELETE EXCEED BACKUP FILE")
        return

    def finalize(self):
        self.log.start_line(title="\n(11) FINALIZE RBD BACKUP", symbol_count=0)
        self.log.start_line(symbol="-", symbol_count=40)

        if self.manager is not None:
            self.manager.stop_worker()
        else:
            self.log.error("worker manager is invalid.")
            return

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

        # todo: update backup info metefile
        #

        retain_count = self.cfg.backup_retain_count

        summary = ['rbd0 backup successfully. exported backup size = 1000',
                   'rbd1 backup failed. ' ]

        self.log.info(("rbd backup completed.", summary))

        #snap_protect = self.cfg.backup_snapshot_protect
        #

        # do extra process to the snapshot and backup directory
        # to maintain number of snapshot and backup file


def main(argument_list):

    print("\n******** Start RBD Backup ********")

    rbdbackup = RBDBackup()

    try:
        print("Step 1 - read rbd backup argument list.")
        if rbdbackup.read_argument_list(argument_list) == False:
            return

        print("Step 2 - read config file options. (Logging will start after loging option read successfully.)")
        if rbdbackup.read_config_file() == False:
            return

        print("Step 3 - initialze backup directory.")
        if rbdbackup.initialize_backup_directory() == False:
            return

        print("Step 4 - read rbd list to backup.")
        if rbdbackup.read_backup_rbd_list() == False:
            return

        print("Step 5 - initialze worker.")
        if rbdbackup.initialize_backup_worker() == False:
            return

        print("Step 6 - initialze snapshot task.")
        if rbdbackup.initialize_snapshot_task() == False:
            return

        print("Step 7 - start snapshot task.")
        if rbdbackup.start_snapshot() == False:
            return

        print("Step 8 - initialize export task.")
        if rbdbackup.initialize_export_task() == False:
            return

        print("Step 9 - start export task.")
        if rbdbackup.start_export() == False:
            return

        print("Step 10 - initialize ")
        if rbdbackup.initialize_rm_snapshot_task() == False:
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
