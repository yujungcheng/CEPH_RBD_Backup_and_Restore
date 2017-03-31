#!/usr/bin/env python
# -*- coding: utf-8 -*-
# apt-get install python-pip
# pip install pyyaml
# or
# apt-get install python-yaml

import os, sys, yaml, glob, traceback

from collections import  OrderedDict


class Yaml(object):

    def __init__(self, log, path, width=1024, indent=4):
        self.log = log
        self.yaml_path = path
        self.width = width
        self.indent = indent

        self.sync = False
        self.yaml_data = OrderedDict()
        self.temp_yaml_data = OrderedDict()

        self.log.info("initialize YAML file, path = %s" % path)

    def clear(self):
        self.log.info("clear yaml data. %s" % self.yaml_path)
        try:
            with open(self.yaml_path, 'w') as yaml_file:
                self._dump('', yaml_file)
                return True

        except Exception as e:
            self.log.error("unable to clear data. %s" %e)
            return False

    def read(self, section_name=None):
        self.log.info("read yaml section data. section_name = %s" % section_name)
        try:
            if not self.sync:
                if os.path.exists(self.yaml_path):
                    yaml_file = open(self.yaml_path, 'r')
                    yaml_data = yaml.load(yaml_file, Loader=yaml.CLoader)
                    self.yaml_data = OrderedDict(yaml_data)
                    yaml_file.close()
                    self.sync = True
                else:
                    self.log.error("yaml file does not exist. path = %s" % self.yaml_path)
                    self.sync = False
                    return False

            if section_name is None:
                self.log.debug(("yaml data: ", self.yaml_data))
                return self.yaml_data
            if section_name in self.yaml_data:
                self.log.debug(("yaml data, section = %s" % section_name, self.yaml_data[section_name]))
                return self.yaml_data[section_name]

        except Exception as e:
            self.log.warning("unable to read yaml data. %s)" % e)
            self.sync = False
            return False

    def update(self, section_name, section_data, write=True):
        ''' update metadata in this class, write to file if write set to True
            if you have multiple section to update in the yaml but only want to write one time,
            you can use this method with write=Flase. It will just update section data
            in the classe variable only until you set write=True at last update call.
            or just simply call write() method to sync data to ymal file.
        '''
        self.log.info("update section data in yaml file. section_name=%s, write=%s" % (section_name, write))
        try:

            if self.sync is True:
                self.yaml_data = self.read()

            if isinstance(section_data, OrderedDict):
                self.log.warning("convert OrderedDict to regular dict.")
                self.yaml_data[section_name] = dict(section_data)
            else:
                self.yaml_data[section_name] = section_data

            self.sync = False

            if write:
                return self.write(overwrite=True)

            return True
        except Exception as e:
            self.log.error("unable to update yaml section data. %s" %e)
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            return False

    def write(self, section_name=None, section_data=None, default_flow_style=False, overwrite=False):
        ''' append new data in yaml file
        '''
        self.log.info("write data to file %s, overwrite = %s" % (self.yaml_path, overwrite))

        try:
            if overwrite:
                write_mode = 'w'
                #os.system("echo -n > %s" % self.yaml_path)
            else:
                write_mode = 'a'

            #yaml_data = OrderedDict()
            yaml_data = self.yaml_data

            if section_name is None and section_data is None:
                yaml_data = self.yaml_data  # replace all data

            elif section_name is not None and section_data is not None:
                yaml_data[section_name] = section_data  # update data, section_data should be string, list, or dict
                self.yaml_data[section_name] = section_data

            elif section_name is None:
                yaml_data = section_data    # section_data should OrderedDict or dict
                self.yaml_data = section_data

            else:
                yaml_data[section_name] = "" # clear data of a section
                self.yaml_data[section_name] = ""

            # append the yaml data to the yaml file
            with open(self.yaml_path, write_mode) as yaml_file:
                #yaml.dump(yaml_data, yaml_file, default_flow_style=default_flow_style)

                # take care list or dict type of data value
                if isinstance(yaml_data, list):
                    for list_data in yaml_data:
                        self._dump(list_data, yaml_file, default_flow_style=default_flow_style)
                        self.log.debug(("write data:",  list_data))
                elif isinstance(yaml_data, dict):
                    for key, value in yaml_data.iteritems():
                        dict_data = {key: value}
                        self._dump(dict_data, yaml_file, default_flow_style=default_flow_style)
                        self.log.debug(("write section: %s" % key, value))
                else:
                    self._dump(yaml_data, yaml_file, default_flow_style=default_flow_style)
                    self.log.debug(("write data:", value))

                self.sync = True
                return True

            self.sync = False
            self.log.error("unable to open yaml file %s" % self.yaml_path)
            return False
        except Exception as e:
            self.log.error("unable to write yaml section data. %s" %e)
            exc_type,exc_value,exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
            return False

    def _dump(self, data, yaml_file, default_flow_style=False):
        yaml.safe_dump(data, yaml_file, indent=self.indent,
                                        width=self.width,
                                        default_flow_style=default_flow_style)
