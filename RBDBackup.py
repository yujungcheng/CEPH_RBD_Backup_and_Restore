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

        self.backup_list = []
        self.snapshot_tasks = {}
        self.export_tasks = {}

        # may not used...
        self.snapshot_rm_tasks = {}
        self.backup_rm_tasks = {}

        self.backup_type = None

        self.total_backup_full_size = 0
        self.total_backup_used_size = 0
        self.total_backup_rbd_count = 0

        self.ceph = Ceph()
        self.manager = None
        self.backup_directory =None
        self.metafile = None

        self.openstck_mapping_enabled = False
        self.cache_clean_enabled = False
        self.monitor_enabled = False

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
            self.log.blank_line(2)
            log_begin_line = "%s %s" %(start_log_title, self.backup_time)
            self.log.start_line(title=log_begin_line)
            self.log.set_logger(name='RBDBackup')
        except Exception as e:
            print("Error, fail to initialize logging. %s" % e)
            return False

        return True

    def _initialize_monitor(self):
        monitor = Monitor()
        return True

    def _update_metafile(self, task):
        try:
            # simplely combin pool_name and rbd_name as a key for storing metadata
            meta_key = "[%s]_[%s]" % (task.pool_name, task.rbd_name)
            meta_value = {meta_key: task.snap_name}

            self.metafile.update(NEW_SNAPSHOT_NAME, meta_value)

            '''
            if self.metafile.write('last_task', task.result):
                # write last snapshot name

                return True
            return False
            '''
            return True
        except Exception as e:
            self.log.error("unable to write task result to metafile. %s" % e)
            return False

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
                              "do full backup. backup_type=%s"
                              %(weekday, full_weekday, backup_type))
            elif weekday in incr_weekday:
                backup_type = DIFF
                self.log.info("today (%s) is in incremental backup weekdays %s.\n"
                              "do incremental backup. backup_type=%s"
                              %(weekday, incr_weekday, backup_type))
            else:
                self.log.info("no bacakup triggered on today(%s)." % weekday)

            return backup_type
        except Exception as e:
            self.log.error("unable to match backup type. %s"% e)
            return False

    def _get_backup_list(self):
        '''
        return a dict of dict which contain rbd image info
            { 'pool_name_rbd_name': {'id': str
                                     'pool_name': str,
                                     'rbd_name': str,
                                     'rbd_full_size': int,
                                     'rbd_used_size': int,
                                     'volume_name': str }, ... }
        '''
        def _pack_rbd_info(pool, rbd_name, volume_name=None):
            #rbd_stat = pool.get_rbd_stat(rbd_name)
            rbd_full_size = pool.get_rbd_size(rbd_name)
            rbd_used_size = pool.get_used_size(rbd_name)
            rbd_snap_list = pool.get_rbd_snap_list(rbd_name)
            rbd_features  = pool.get_rbd_features(rbd_name)

            if rbd_full_size is False:
                self.log.warning("unable to get full size of rbd image %s. skip backup of it." % rbd_name)
                return None
            if rbd_used_size is False:    # try method 1
                rbd_used_size = pool.get_used_size(rbd_name, method=1)
                if rbd_used_size is False:
                    self.log.warning("unable to get used size of rbd image %s. skip backup of it." % rbd_name)
                    return None
            if rbd_snap_list is False:
                self.log.warning("unable to get snapshot list of rbd image %s. skip backup of it." % rbd_name)
                return False
            if rbd_features is False:
                self.log.warning("unable to get features of rbd image %s. skip backup of it." % rbd_name)
                return False

            self.total_backup_full_size += rbd_full_size
            self.total_backup_used_size += rbd_used_size
            self.total_backup_rbd_count += 1

            #uuid_seed = ''.join([self.ceph.cluster_name, pool.pool_name, rbd_name])
            #rbd_uuid = uuid.uuid3(uuid.NAMESPACE_DNS, uuid_seed)

            rbd_id = "%s_%s_%s" % (self.ceph.cluster_name, pool.pool_name, rbd_name)
            return {'id': rbd_id,
                    'pool_name': pool.pool_name,
                    'rbd_name': rbd_name,
                    'rbd_full_size': rbd_full_size,
                    'rbd_used_size': rbd_used_size,
                    'volume_name': volume_name,
                    'snapshot_list': rbd_snap_list,
                    'features': rbd_features}

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

                yaml = Yaml(self.log, yaml_path)
                yaml_data = yaml.read(yaml_section)
                if yaml_data is not False:
                    openstack = OpenStack(self.log,
                                          yaml_data=yaml_data,
                                          distribution=distribution)
                    if openstack.set_cinder_client():
                        self.log.info("openstack cinder client connectted.")
                        volumes = openstack.get_cinder_volume()
                        pool = Pool(self.log, pool_name, self.ceph.conffile)
                        for volume_name, volume_id in volumes.iteritems():
                            rbd_info = _pack_rbd_info(pool, volume_id, volume_name)
                            if rbd_info is not False:
                                rbd_list.append(rbd_info)
                                #rbd_key = rbd_info['id']
                                #rbd_list[rbd_key] = rbd_info
            else:
                yaml_path = self.cfg.backup_yaml_filepath
                yaml_section = self.cfg.backup_yaml_section_name

                yaml = Yaml(self.log, yaml_path)
                yaml_data = yaml.read(yaml_section)
                if yaml_data is not False:
                    for pool_name, rbd_name_list in yaml_data.iteritems():
                        pool = Pool(self.log, pool_name, self.ceph.conffile)
                        for rbd_name in rbd_name_list:
                            rbd_info = _pack_rbd_info(pool, rbd_name)
                            if rbd_info is not False:
                                rbd_list.append(rbd_info)
                                #rbd_key = rbd_info['id']
                                #rbd_list[rbd_key] = rbd_info

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

            self.log.info("total %s rbd(s) in rbd backup list\n"
                          "total backup rbd full size = %s bytes\n"
                          "total backup rbd used size = %s bytes"
                          % (len(rbd_list),
                             self.total_backup_full_size,
                             self.total_backup_used_size))

            # todo: write backup list to metadata
            #
            backup_info = OrderedDict()
            backup_info['rbd_list'] = rbd_list
            backup_info['total_full_bytes'] = self.total_backup_full_size
            backup_info['total_used_bytes'] = self.total_backup_used_size

            self.metafile.update(BACKUP_INFO, backup_info, overwrite=False)
            self.metafile.save()

            return rbd_list
        except Exception as e:
            self.log.error("unable to get rbd image list for backup. %s" % e)
            return []

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
        if not cfg.read_backup_config():
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
        self.log.info("\n(2). READ RBD IMAGE LIST TO BACKUP")

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

            # print list order...
            # pool name, rbd name, used size

            return True
        except Exception as e:
            self.log.warning("sorting rbd backup list failed. %s" % e)

        self.backup_list = backup_list
        return True

    def initialize_backup_directory(self):
        ''' check the backup directory and initialize metafile
        create folder with {cluster name} in backup directory if not exist
        '''
        self.log.info("\n(1). INITIALIZE BACKUP DIRECTORY %s" % self.cfg.backup_path)

        path = self.cfg.backup_path
        directory = Directory(self.log, path)
        if directory.path is not None:

            # add backup cluster name folder in backup directory if not exist
            # ----------------------------------------------------------------
            try:
                cluster_path = directory.add_directory(self.ceph.cluster_name,
                                                       set_path=True,
                                                       check_size=True)
                self.backup_directory = directory
            except Exception as e:
                self.log.error("directory %s fail initialized. %s" % (path, e))
                return False

            # add metadata file which record cluster name and fsid
            # ----------------------------------------------------------------
            try:
                self.log.info("set metafile in directory %s" % cluster_path)
                metafile = Metafile(self.log, self.ceph.cluster_name, cluster_path)

                metafiles = [BACKUP_INFO, LAST_SNAPSHOT_NAME, NEW_SNAPSHOT_NAME, TASK_RESULT]
                if metafile.initialize(self.ceph.cluster_name, metafiles):
                    backup_info = OrderedDict()

                    backup_info['fsid'] = self.ceph.get_fsid()
                    backup_info['name'] = self.ceph.cluster_name
                    backup_info['time'] = self.backup_time

                    backup_info['backup_dir_avai_bytes'] = self.backup_directory.available_bytes
                    backup_info['backup_dir_used_bytes'] = self.backup_directory.used_bytes

                    #backup_info[CLUSTER_RBD_USED_SIZE] = 0
                    #backup_info[CLUSTER_RBD_FULL_SIZE] = 0

                    if metafile.write(BACKUP_INFO, backup_info):
                        if metafile.save() is not True:
                            return False

                    self.metafile = metafile
                    return True
                else:
                    return False
            except Exception as e:
                self.log.error("metafile fail initialized. %s" %e)
                return False
        else:
            self.log.error("direcory path %s is invalid." % cfg.backup_directory)
            return False

    def initialize_backup_worker(self):
        self.log.info("\n(3). INITIALIZE BACKUP WORKERS (child processes)")
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
        self.log.info("\n(4). INITIALIZE RBD SNAPSHOT TASKS")
        try:
            for rbd_info in self.backup_list:
                rbd_id = rbd_info['id']
                #print("%s, %s" % (backup_rbd['pool_name'], backup_rbd['rbd_name']))

                # todo: snapshot protect
                snapshot_task = RBDSnapshotTask(self.ceph.cluster_name,
                                                rbd_info['pool_name'],
                                                rbd_info['rbd_name'])
                snapshot_task.id = rbd_id
                self.snapshot_tasks[rbd_info['id']] = snapshot_task
                self.log.info("create snapshot task (%s)." % snapshot_task)

            self.log.info("total %s snapshot tasks created." % len(self.snapshot_tasks))
            return True
        except Exception as e:
            self.log.error("unable to create snapshot task. %s" % e)
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            sys.exit(2)
            return False

    def start_snapshot(self):
        self.log.info("\n(5). START RBD SNAPSHOT TASKS")

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
                else:
                    # remove this backup item from backup list if snapshot failed
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
                    self.log.debug(("task result of %s." % task.name, task.result))

                    # update task result to metadata
                    self.metafile.update(TASK_RESULT, {task.name: task.result})

                    # update new snapshot name to metadata
                    rbd_key = "%s_%s" % (task.pool_name, task.rbd_name)
                    self.metafile.update(NEW_SNAPSHOT_NAME, {rbd_key: task.snap_name})

        # all snapshot tasks are collected, then save metadata.
        # ----------------------------------------------------------------------
        self.metafile.save()
        self.log.info("%s submitted snapshot task.\n"
                      "%s completed snapshot task.\n"
                      "%s uncompleted snapshot task."
                      % (submitted_task_count,
                         completed_task_count,
                         uncompleted_task_count))
        return True

    def initialize_export_task(self):
        ''' this is export stage, snahshot tasks in snapshot stage must be done
            before do export task
        check backup type, to generate full or incremental backup
        to full backup:
            (stage 1)snapshot task -> (stage 2)rbd export task
        to incremental backup:
            (stage 1)snapshot task -> (stage 2)rbd export diff task

        '''
        self.log.info("\n(6). INITIALIZE RBD EXPORT TASKS")

        # (1) get backup type, diff or full backup base on configuration
        # ----------------------------------------------------------------------
        self.backup_type = self._get_backup_type()
        if self.backup_type is None:
            return False

        # (2) produce backup task list
        # ----------------------------------------------------------------------
        try:
            rbd_directories = {}
            rbd_yaml = {}

            # get snapshot name since last backup, get dict
            last_snapshot = self.metafile.read(LAST_SNAPSHOT_NAME)
            if last_snapshot is False:
                self.log.warning("unable to read metafile %s. set backup type to full backup" % LAST_SNAPSHOT_NAME)
                backup_type = FULL

            for rbd_info in self.backup_list:
                rbd_id = rbd_info['id']
                pool_name = rbd_info['pool_name']
                rbd_name = rbd_info['rbd_name']
                snap_list = rbd_info['snapshot_list']

                # 2.1 create rbd backup directory if not exist
                #     check rbd backup directory, if no backup exist, set full backup type
                # ----------------------------------------
                rbd_path = self.backup_directory.add_directory(pool_name, rbd_name)
                if rbd_directories.has_key(rbd_path) is False:
                    self.log.info("create directory for rbd %s. path=%s" % (rbd_name, rbd_path))

                    rbd_directory = Directory(self.log, rbd_path)
                    if rbd_directory.path != rbd_path:
                        self.log.warning("unable to create rbd backup directory %s, skip this rbd backup" % rbd_path)
                        continue
                    else:
                        rbd_directories[rbd_path] = rbd_directory

                        file_count = rbd_directory.get_file_list(get_count=True)
                        if file_count == 0 or file_count is False:
                            self.log.debug("no any backup file exist in %s. set backup type to full backup" % rbd_path)
                            backup_type = FULL
                        else:
                            backup_type = self.backup_type


                # 2.3 get the snapshot name from snapshot tasks
                # ----------------------------------------
                snapshot_task = self.snapshot_tasks[rbd_id]
                to_snap = snapshot_task.snap_name
                if to_snap == None:
                    raise Exception("undefined snapshot name from previous snapshot task.")
                # todo: check the snapshot is exist in cluster
                #

                # 2.4-1 do incremental backup
                # ----------------------------------------
                if backup_type == DIFF:
                    # get snapshot name of previous backup from metadata
                    # and check it exist in cluster, if not exist do full backup
                    #from_snap = self.metafile.read(LAST_SNAPSHOT_NAME, rbd_key)
                    from_snap = last_snapshot[rbd_id]

                    if from_snap not in snap_list:
                        backup_type = FULL

                    if from_snap == False or from_snap == '':
                        backup_type = FULL
                        self.log.warning("unable to find last snapshot of rbd %s. change backup type to full backup" % rbd_name)
                    else:
                        self.log.info("last snapshot of rbd %s is %s." %(rbd_name, from_snap))

                        export_filename = ''.join(['from_', from_snap, '_to_', to_snap])
                        export_destpath = os.path.join(rbd_path, export_filename)
                        export_task = RBDExportTask(self.ceph.cluster_name,
                                                    pool_name,
                                                    rbd_name,
                                                    export_destpath,
                                                    export_type=backup_type,
                                                    from_snap=from_snap,
                                                    to_snap=to_snap)

                # 2.4-2 do full backup
                # ----------------------------------------
                if backup_type == FULL:
                    export_filename = to_snap
                    export_destpath = "%s/%s" %(rbd_path, export_filename)
                    export_task = RBDExportTask(self.ceph.cluster_name,
                                                pool_name,
                                                rbd_name,
                                                export_destpath,
                                                export_type=backup_type,
                                                to_snap=to_snap)

                # 2.5 save created export tasks
                # ----------------------------------------
                export_task.id = rbd_id
                self.export_tasks[rbd_id] = export_task
                self.log.info("create rbd export task (%s)." %(export_task))

            return True
        except Exception as e:
            self.log.error("unable to create export task. %s" % e)
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            sys.exit(2)
            return False

    def start_export(self):
        self.log.info("\n(7). START RBD EXPORT TASKS")
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
                self.log.error("unable to submit export task(%s) to worker manager. %s" % (rbd_id, e))
                continue

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

                if submitted_task_count == completed_task_count+uncompleted_task_count:
                    break

            except Exception as e:
                self.log.error("unable to check export result task. %s" % e)
                uncompleted_task_count += 1

            finally:
                # update task result to metadata
                # ----------------------------------------
                if task is not None:
                    self.log.debug(("task result of %s." % task.name, task.result))

                    # update task result metadata
                    self.metafile.update(TASK_RESULT, {task.name: task.result})

                    # update last snapshot name to metadata
                    rbd_key = "%s_%s" % (task.pool_name, task.rbd_name)
                    self.metafile.update(LAST_SNAPSHOT_NAME, {rbd_key: task.to_snap})

        self.metafile.save()
        self.log.info("%s submitted export task.\n"
                      "%s completed export task.\n"
                      "%s uncompleted export task."
                      % (submitted_task_count,
                         completed_task_count,
                         uncompleted_task_count))
        return True

    def initialize_rm_snapshot_task(self):
        self.log.info("\n(8) INITIALIZE RM SNAPSHOT TASK")
        try:
            for rbd_info in self.backup_list:
                rbd_id = rbd_info['id']
                export_task = self.export_tasks[rbd_id]
                from_snap = export_task.from_snap

                # just modify the snapshot to delete snapshot from last backup
                snapshot_task = self.snapshot_tasks[rbd_id]
                snapshot_task.action = DELETE
                snapshot_task.snap_name = from_snap

                self.snapshot_tasks[rbd_id] = snapshot_task

                #self.manager.add_task(snapshot_task)

            return True
        except Exception as e:
            self.log.error("unable to create snapshot task. %s" % e)
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            sys.exit(2)
            return False

    def start_rm_snapshot_task(self):
        self.log.info("\n(9) START RM SNAPSHOT TASK")
        return

    def delete_exceed_backup(self):
        self.log.info("\n(10) DELETE EXCEED BACKUP FILE")
        return

    def finalize(self):
        self.log.info("\n(11) FINALIZE RBD BACKUP")

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
        #snap_retain_count = self.cfg.backup_snapshot_retain_count

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
            return abs

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
