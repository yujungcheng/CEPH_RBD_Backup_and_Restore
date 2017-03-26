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

        self.committed = False

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

            return yaml.write(section_data=meta_data, overwrite=overwrite)
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

            return yaml.write(overwrite=True, default_flow_style=default_flow_style)  # finally write data to file
        except Exception as e:
            self.log.error("unable to update metafile. %s" % e)
            return False


"""

# utilize Yaml class for storing metadata
class Metafile():
    def __init__(self, log, path, name=METAFILE_NAME, shm_path=METAFILE_SHM_PATH):
        self.log = log
        self.path = path
        self.name = name
        self.shm_path = shm_path

        self.metafile_path = "%s/%s" %(self.path, self.name)
        self.tempfile_path = "%s/%s" %(self.shm_path, self.name)

        # call initialize to set them
        self.tempfile = None
        self.metafile = None
        self.metadata = None

    def _merge_dicts(self, old_dict, new_dict):
        result = old_dict
        for meta_file, meta_data in new_dict.iteritems():
            result[meta_file] = meta_data

        return result

    def read(self, meta_file):

    def initialize(self, meta_file, metadata):
        ''' test read/write data to metadata file.
        '''
        try:
            self.log.info("initialize metadata file %s" % self.metafile_path)
            metafile = Yaml(self.log, self.metafile_path)

            # first try
            meta_file_data = metafile.read(meta_file)

            # if failed, write new section as meta_file and metadata as meta_data
            if meta_file_data is False:
                metafile.write(meta_file, metadata, overwrite=True)

            # second try, return False if failed
            meta_file_data = metafile.read(meta_file)
            if meta_file_data is False:
                return False

            if meta_file_data != metadata:
                return False

            # create tempfile in share memory and write same data
            tempfile = Yaml(self.log, self.tempfile_path)
            tempfile.write(meta_file, metadata)

            if tempfile.read(meta_file) == False:
                self.log.warning("unable to create tempfile in %s" % self.tempfile_path)

            # up to here, the metadata should be able to read/write in disk
            # so store the entire metadata in class
            self.metadata = metafile.yaml_data

            self.tempfile = tempfile
            self.metafile = metafile
            return True
        except Exception as e:
            self.log.error("unable to initialize metafile. %s" % e)
            return False

    def update(self, meta_file, metadata):
        '''
        directly set section data (metadata) in specific section name (meta_file)
        '''
        try:
            if self.metafile is None:
                self.log.error("metafile is not initialized yet")
                return False

            self.metadata[meta_file] = metadata

            # write metadata in shared memory
            if self.tempfile.update(meta_file, metadata) != True:
                self.log.warning("unable to write metadata in shared memory %s" % self.tempfile_path)

            return True
        except Exception as e:
            self.log.error("unable to write metadata. %s" % e)
            return False

    def add(self, meta_file, metadata):
        '''
        append new section data (metadata) in section name (meta_file)
            for dictionary data, merge two dictionaries
            for list data, merge two list and set item unique
            for string data, join them to a list
        '''
        try:
            self.log.info("add new metadata to tempfile %s" % self.metafile_path)
            #exist_data = self.tempfile.read(meta_file)

            new_data = None
            if self.metadata.has_meta_file(meta_file):
                exist_data = self.metadata[meta_file]

                if isinstance(exist_data, dict):
                    self.log.debug("merge dictonary metadata")
                    if isinstance(metadata, dict):
                        new_data = self._merge_dicts(exist_data, metadata)
                    else:
                        self.log.error("invalid metadata to merge dictionary")
                        return False
                elif isinstance(exist_data, str):
                    self.log.debug("merge string metadata")
                    if isinstance(metadata, str):
                        if exist_data != metadata:
                            new_data = [exist_data, metadata]
                    else:
                        self.log.error("invalid metadata type to merge string.")
                        return False
                elif isinstance(exist_data, list):
                    self.log.debug("merge list metadata")
                    if isinstance(metadata, list):
                        new_data = list(set(exist_data, metadata))
                    elif isinstance(metadata, str):
                        if metadata not in exist_data:
                            new_data = exist_data.append(metadata)
                    else:
                        self.log.error("invalid metadata type to merge list.")
                        return False
                else:
                    self.log.error("the data of %s in metafile is invalid" % meta_file)
                    return False

            else:
                new_data = metadata

            if new_data is not None:
                self.log.info(("merged new metadata:", new_data))
                self.metadata[meta_file] = new_data

                # write metadata in shared memory
                if self.tempfile.update(meta_file, metadata) != True:
                    self.log.warning("unable to write metadata in shared memory %s" % self.tempfile_path)

                return True
            else:
                self.log.error("unable to merge new metadata.")
                return False
        except Exception as e:
            self.log.error("unable to add metadata. %s" % e)
            return False

    def commit(self):
        ''' write metadata to metadata file
        '''
        try:
            self.log.info("commit metadata to metafile %s" % (self.metafile_path))
            if self.metafile.write(section_data=self.metadata, overwrite=True):
                return True

            self.log.error("unable to commit medatata to %s" % self.metafile_path)
            return False
        except Exception as e:
            self.log.error("unable to commit metadata. %s" % e)
            return False
"""
