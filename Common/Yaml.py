#!/usr/bin/env python
# -*- coding: utf-8 -*-
# apt-get install python-pip
# pip install pyyaml
# or
# apt-get install python-yaml

import os, sys, yaml, glob

class Yaml(object):

    def __init__(self, log, path):
        self.log = log
        self.yaml_path = path
        self.yaml_data = None
        self.log.info("initialize YAML file, path = %s." % path)

    def read(self, section_name=None):
        if os.path.exists(self.yaml_path):
            try:
                self.log.info("read yaml section data. section_name = %s" % section_name)
                yaml_file = open(self.yaml_path, 'r')
                self.yaml_data = yaml.load(yaml_file, Loader=yaml.CLoader)
                yaml_file.close()

                # if section name is None, return all data in yaml file
                if section_name is None:
                    return self.yaml_data

                '''
                print self.yaml_path
                print section_name
                print self.yaml_data
                '''

                if section_name in self.yaml_data:
                    self.log.debug(("yaml section data. (%s)" % section_name, self.yaml_data[section_name]))
                    return self.yaml_data[section_name]
                else:
                    self.log.info("no section name (%s) found in yaml file" % section_name)
                    return False
            except Exception as e:
                self.log.warning("unable to read yaml section data from section_name %s. %s)" %(section_name, e))
                return False
        else:
            self.log.error("yaml file does not exist. (%s)" % self.yaml_path)
            return False

    def update(self, section_name, section_data, default_flow_style=False):
        ''' update exist data in yaml file
        '''
        self.log.debug("update section data in yaml file. section_name=%s" % section_name)
        try:
            # read all data in yaml file, the data should represent as dictionary data type
            yaml_file = open(self.yaml_path, 'r')
            yaml_data = yaml.load(yaml_file, Loader=yaml.CLoader)
            yaml_file.close()

            #old_section_data = yaml_data[section_name]

            # update the dictionary
            yaml_data[section_name] = section_data

            # write the dictionary to yaml file
            with open(self.yaml_path, 'w') as yaml_file:
                yaml.safe_dump(yaml_data, yaml_file, indent=4,
                                                     width=512,
                                                     default_flow_style=default_flow_style)
                self.log.debug(("update section data successfully.", section_data))

                self.yaml_data = yaml_data
                return True

            return False
        except Exception as e:
            self.log.error("unable to update yaml section data. %s" %e)
            return False

    def write(self, section_name=None, section_data=None, default_flow_style=False, overwrite=False):
        ''' append new data in yaml file
        '''
        self.log.debug("write data to file %s." % self.yaml_path)
        try:
            if overwrite:
                write_mode = 'w'
            else:
                write_mode = 'a'

            if section_name is None and section_data is None:
                self.log.warning("no data to write.")
                return False

            if section_name is not None and section_data is not None:
                # pack to an dictionary format
                yaml_data = {section_name: section_data}
            elif section_name is None:
                yaml_data = section_data
            else:
                yaml_data = {}
                yaml_data[section_name] = ""

            # append the yaml data to the yaml file
            with open(self.yaml_path, write_mode) as yaml_file:
                #yaml.dump(yaml_data, yaml_file, default_flow_style=default_flow_style)
                yaml.safe_dump(yaml_data, yaml_file, indent=4,
                                                     width=255,
                                                     default_flow_style=default_flow_style)
                self.log.debug(("write yaml data successfully. mode=%s" % write_mode, yaml_data))
                return True

            return False
        except Exception as e:
            self.log.error("unable to write yaml section data. %s" %e)
            return False
