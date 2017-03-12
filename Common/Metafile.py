#!/usr/bin/env python
# -*- coding: utf-8 -*-

from Common.Constant import *
from Common.Yaml import Yaml
import os, sys, subprocess
import logging.handlers


# use logging or yaml module to writing metadata message
class Metafile():
    def __init__(self, log, path, name='.metafile'):
        self.log = log
        self.path = path
        self.name = name
        self.metafile_path = "%s/%s" %(self.path, self.name)
        self.last_metadata = None

        # call initialize to set them
        self.section_name = None
        self.meta_format = None
        self.logger_meta = None
        self.yaml_meta = None

    def initialize(self, header,
                   maxBytes=1024*1024*10, count=10, meta_format=YAML):
        try:
            self.log.info("initialize metadata file %s\n"
                          "header = %s \n"
                          "format = %s " %
                          (self.metafile_path, header, meta_format))

            if meta_format == LOG:
                logger = logging.getLogger(self.name)
                logger.setLevel('DEBUG')
                h = logging.handlers.RotatingFileHandler(self.metafile_path,
                                                         maxBytes=maxBytes,
                                                         backupCount=count)
                h.setLevel('DEBUG')
                formatter = logging.Formatter('%(message)s')
                h.setFormatter(formatter)
                logger.addHandler(h)

                # write header log
                new_header = '\n%s' % header
                logger.debug(new_header)
                self.logger_meta = logger

            elif meta_format == YAML:
                ymml = Yaml(self.log, self.metafile_path)
                self.yaml_meta = ymml

            else:
                self.log.error("unknow metadata format")
                return False

            return True
        except Exception as e:
            self.log.error("unable to initialize metafile. %s" % e)
            return False

    def write(self, metadata):
        try:
            if self.meta_format is None:
                self.log.error("metafile is not initialized yet")
                return False

            if self.meta_format == LOG:
                self.logger_meta.debug(str(metadata).rstrip())
            elif self.meta_format == YAML:
                # todo: something wrong in here....
                #
                if self.yaml_meta.write(self.section_name, metadata) != True:
                    return False

            self.last_metadata = metadata
            return True
        except Exception as e:
            self.log.error("unable to write metadata. %s" % e)
            return False

    def get_last(self):
        metadata_cmd = "tail -n -1 %s" %(self.metafile)
        return os.system(metadata_cmd)
