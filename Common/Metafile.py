#!/usr/bin/env python
# -*- coding: utf-8 -*-

from Common.Constant import *
from Common.Yaml import Yaml

import os, sys, subprocess, traceback
import logging.handlers


class Metafile():
    def __init__(self, log, cluster_name, file_path, shm_path=METAFILE_SHM_PATH):
        self.log = log
        self.file_path = file_path
        self.shm_path = shm_path
        self.cluster_name = cluster_name

        # store all yaml object of metafiles
        self.metadata = {}

        self.log.info("set metafile in directory %s" % self.file_path)

    def initialize(self, cluster_name, metafiles=[]):
        self.log.info("initialize metafiles in %s" % self.shm_path)
        try:
            for filename in metafiles:
                metafile = "%s/%s.%s" % (self.file_path, self.cluster_name, filename)
                self.log.debug("set metafile %s" % metafile)
                yaml = Yaml(self.log, metafile)
                self.metadata[filename] = yaml
            return True
        except Exception as e:
            self.log.error("unable to initialize metafiles. %s" % e)
            return False

    def clear(self, metafiles=[]):
        self.log.info("clear metafiles %s" % metafiles)
        try:
            for filename in metafiles:
                if self.metadata.has_key(filename):
                    yaml = self.metadata[filename]
                    return yaml.clear(self.log)

        except Exception as e:
            self.log.error("unable to clear metafiles. %s" % e)
            return False

    def read(self, meta_file, section_name=None):
        ''' meta_file is filename of metadata, section_name is section name in metadata '''
        self.log.info("read data from  %s" % meta_file)
        try:
            path = "%s/%s.%s" % (self.file_path, self.cluster_name, meta_file)
            yaml = Yaml(self.log, path)
            if section_name is None:
                return yaml.read()
            return yaml.read(section_name)
        except Exception as e:
            self.log.error("unable to read metadata %s. %s" % (path, e))
            return False

    def write(self, meta_file, meta_data, default_flow_style=False, overwrite=False):
        self.log.info("writing data to %s" % meta_file)
        try:
            if self.metadata.has_key(meta_file):
                yaml = self.metadata[meta_file]
            else:
                metafile = "%s/%s.%s" % (self.shm_path, self.cluster_name, meta_file)
                yaml = Yaml(self.log, metafile)

            return yaml.write(section_data=meta_data,
                              default_flow_style=default_flow_style,
                              overwrite=overwrite)
        except Exception as e:
            self.log.error("unable to write metafile. %s" % e)
            exc_type,exc_meta_data,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_meta_data, exc_traceback, file=sys.stdout)
            return False

    def update(self, meta_file, meta_data, default_flow_style=False):
        self.log.info("updating data to %s" % meta_file)
        try:
            yaml = self.metadata[meta_file]

            for name, data in meta_data.iteritems():
                if not yaml.update(name, data, write=False):  # just update data in yaml class
                    self.log.debug("unable to update %s in metafile %s" % (name, meta_file))
                    return False

            return yaml.write(overwrite=True,
                              default_flow_style=default_flow_style)  # finally write data to file
        except Exception as e:
            self.log.error("unable to update metafile. %s" % e)
            return False
