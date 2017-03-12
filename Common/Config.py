#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ConfigParser import ConfigParser

import os

class RBDConfig(object):

    def __init__(self, path, verify=True, section_name=None):
        if os.path.exists(path):
            self.path = path
        else:
            print("Error, configuration file (%s) not exist." % path)

        try:
            self.config = ConfigParser()
            self.config.read(self.path)
        except Exception:
            print("Error, unable to read config file (%s)." % path)

        self.self = self
        self.section_name = section_name

    def __str__(self):
        if self.section_name is None:
            return None

        options = self.config._sections[self.section_name]
        return str(options)

    def _has_options(self, options):
        if not isinstance(options, list):
            return False
        for option in options:
            if not self.config.has_option(self.section_name, option):
                print("Warning, missing option (%s)." %(option))
                return False
        return True

    def _set_options(self, options):
        for option in options:
            value = self.config.get(self.section_name, option)
            setattr(self, option, value)
            #print option, value
        return True

    def _has_section_name(f):
        def check(*args, **kargs):
            self = args[0]
            if self.section_name == None:
                raise RuntimeError("Error, no section name specified.")
                return False
            return f(*args, **kargs)
        return check

    def check_in_section(self, section_name):
        if hasattr(self, 'config'):
            if self.config.has_section(section_name):
                self.section_name = section_name
                return True
        return False

    # directly set all options in specified section_name
    def set_all_options(self, section_name):
        if self.section_name != None:
            for option in self.config.items(self.section_name):
                key, value = option
                setattr(self, key, value)
            return True
        return False

    @_has_section_name
    def read_log_config(self):
        options = ['log_file',
                   'log_path',
                   'log_level',
                   'log_max_bytes',
                   'log_format_type',
                   'log_backup_count',
                   'log_delay']
        if self._has_options(options):
            # todo: option value verify
            value = self.config.get(self.section_name, 'log_file')
            if value is None:
                print("log_file is invalid")
                return False
            value = self.config.get(self.section_name, 'log_path')
            if not os.path.isabs(value):
                print("log_path is invalid")
                return False

            if self._set_options(options):
                return True
        print("Error, log options invalid.")
        return False

    @_has_section_name
    def read_ceph_config(self):
        options = ['ceph_conffile',
                   'ceph_cluster_name']
        if self._has_options(options):
            # todo: option value verify
            if self._set_options(options):
                return True
        print("Error, backup options invalid.")
        return False

    @_has_section_name
    def read_backup_config(self):
        options=['backup_path',
                 'backup_retain_count',
                 'backup_yaml_filepath',
                 'backup_yaml_section_name',
                 'backup_concurrent_worker_count',
                 'backup_small_size_first',
                 'backup_full_weekday',
                 'backup_incr_weekday']
        if self._has_options(options):
            # todo: option value verify
            #value = self.config.get(self.section_name, 'backup_full_weekday')

            if self._set_options(options):
                return True
        print("Error, backup options invalid.")
        return False

    @_has_section_name
    def read_snapshot_config(self):
        options=['snapshot_retain_count',
                 'snapshot_protect']
        if self._has_options(options):
            if self._set_options(options):
                return True
        print("Error, snapshot options invalid.")
        return False

    @_has_section_name
    def read_monitor_config(self):
        options=['monitor_interval',
                 'monitor_record_path',
                 'monitor_network_io',
                 'monitor_disk_io',
                 'monitor_memory_io']
        if self._has_options(options):
            # todo: option value verify
            if self._set_options(options):
                return True
        print("Error, monitor options invalid.")
        return False

    @_has_section_name
    def read_cache_config(self):
        options=['drop_cache_level',
                 'flush_file_system_buffer']
        if self._has_options(options):
            # todo: options verify
            if self._set_options(options):
                return True
        print("Error, cache clean options invalid.")
        return False

    @_has_section_name
    def read_openstack_config(self):
        options=['openstack_enable_mapping',
                 'openstack_yaml_filepath',
                 'openstack_section_name',
                 'openstack_distribution']

        if self._has_options(options):
            # todo: option value verify
            if self._set_options(options):
                    return True



class PoolConfig(object):
    def __init__(self):
        return
