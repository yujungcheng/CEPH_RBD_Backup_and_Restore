#!/usr/bin/env python
# -*- coding: utf-8 -*-

from Common.Constant import *
from Common.Yaml import Yaml
import os, sys, subprocess
import logging.handlers


class Metafile():
    def __init__(self, log, cluster_name, meta_path, shm_path=METAFILE_SHM_PATH):
        self.log = log
        self.meta_path = meta_path
        self.shm_path = shm_path
        self.cluster_name = cluster_name

        # store all yaml object of metafiles
        self.metadata = {}

        self.committed = False

    def _check_arguments(self, key, value):
        if isinstance(value, dict) is False or isinstance(key, str) is False:
            self.log.error("invalid data type of key or value.")
            return False
        return True

    def initialize(self, cluster_name, metafiles=[]):
        self.log.info("initialize metafiles in %s" % self.shm_path)
        try:
            for filename in metafiles:
                metafile = "%s/%s.%s" % (self.shm_path, self.cluster_name, filename)
                yaml = Yaml(self.log, metafile)
                if yaml.read():
                    self.metadata[filename] = yaml
                    self.log.debug("read metafile %s" % metafile)
                else:
                    self.log.error("unable to read metafile %s" % metafile)
                    return False
            return True
        except Exception as e:
            self.log.error("unable to initialize metafiles. %s" % e)
            return False

    def save(self, path=None):
        ''' save metafile to disk directory '''
        try:
            for key, data in self.metadata.iteritems():
                cmd = "cp -afpR %s %s" % (data.yaml_path, self.meta_path)
                self.log.debug("copy %s to %s" %(data.yaml_path, self.meta_path))
                os.system(cmd)

            return True
        except Exception as e:
            self.log.error("unable to commit metadata to %s. %s" % (self.meta_path, e))
            return False

    def read(self, key, sub_key=None):
        ''' key is filename of metadata, sub_key is section name in metadata '''
        try:
            path = "%s/%s.%s" % (self.meta_path, self.cluster_name, key)
            yaml = Yaml(self.log, path)
            if sub_key is None:
                return yaml.read()
            return yaml.read(sub_key)
        except Exception as e:
            self.log.error("unable to read metadata %s. %s" % (path, e))
            return False

    def write(self, key, value, overwrite=True):
        try:
            if self._check_arguments(key, value):
                if self.metadata.has_key(key):
                    yaml = self.metadata[key]
                else:
                    metafile = "%s/%s.%s" % (self.shm_path, self.cluster_name, key)
                    yaml = Yaml(self.log, metafile)

                yaml.write(value, overwrit=overwrite)
                self.log.debug("write %s to metafile %s" % (value, key))
                return True
            return False
        except Exception as e:
            self.log.error("unable to write metafile. %s" % e)
            return False

    def update(self, key, value):
        try:
            if self._check_arguments(key, value):
                yaml = self.metadata[key]
                for name, data in value.iteritems():
                    yaml_data = yaml.update(name, data)
                    self.log.debug(("update %s in metafile %s" % (name, key), data))
                return True
            return False
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
        for key, value in new_dict.iteritems():
            result[key] = value

        return result

    def read(self, key):

    def initialize(self, key, metadata):
        ''' test read/write data to metadata file.
        '''
        try:
            self.log.info("initialize metadata file %s" % self.metafile_path)
            metafile = Yaml(self.log, self.metafile_path)

            # first try
            key_data = metafile.read(key)

            # if failed, write new section as key and metadata as value
            if key_data is False:
                metafile.write(key, metadata, overwrite=True)

            # second try, return False if failed
            key_data = metafile.read(key)
            if key_data is False:
                return False

            if key_data != metadata:
                return False

            # create tempfile in share memory and write same data
            tempfile = Yaml(self.log, self.tempfile_path)
            tempfile.write(key, metadata)

            if tempfile.read(key) == False:
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

    def update(self, key, metadata):
        '''
        directly set section data (metadata) in specific section name (key)
        '''
        try:
            if self.metafile is None:
                self.log.error("metafile is not initialized yet")
                return False

            self.metadata[key] = metadata

            # write metadata in shared memory
            if self.tempfile.update(key, metadata) != True:
                self.log.warning("unable to write metadata in shared memory %s" % self.tempfile_path)

            return True
        except Exception as e:
            self.log.error("unable to write metadata. %s" % e)
            return False

    def add(self, key, metadata):
        '''
        append new section data (metadata) in section name (key)
            for dictionary data, merge two dictionaries
            for list data, merge two list and set item unique
            for string data, join them to a list
        '''
        try:
            self.log.info("add new metadata to tempfile %s" % self.metafile_path)
            #exist_data = self.tempfile.read(key)

            new_data = None
            if self.metadata.has_key(key):
                exist_data = self.metadata[key]

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
                    self.log.error("the data of %s in metafile is invalid" % key)
                    return False

            else:
                new_data = metadata

            if new_data is not None:
                self.log.info(("merged new metadata:", new_data))
                self.metadata[key] = new_data

                # write metadata in shared memory
                if self.tempfile.update(key, metadata) != True:
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
