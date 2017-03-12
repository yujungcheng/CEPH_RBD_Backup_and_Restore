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

    def read(self, section_name):
        if os.path.exists(self.yaml_path):
            try:
                self.log.info("read yaml section data. (%s)" % section_name)
                yaml_file = open(self.yaml_path, 'r')
                self.yaml_data = yaml.load(yaml_file, Loader=yaml.CLoader)
                yaml_file.close()

                if section_name in self.yaml_data:
                    self.log.debug(("yaml section data. (%s)" % section_name, self.yaml_data[section_name]))
                    return self.yaml_data[section_name]
                else:
                    self.log.info("no section name (%s) found in yaml file" % section_name)
                    return False
            except Exception as e:
                self.log.error("unable to read yaml section data from section_name %s. %s)" %(section_name, e))
                return False
        else:
            self.log.error("yaml file does not exist. (%s)" % self.yaml_path)
            return False

    def update(self, section_name, section_data, default_flow_style=False):
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
                yaml.dump(yaml_data, yaml_file, default_flow_style=default_flow_style)
                self.log.debug("update section data to yaml file %s" % self.yaml_path)

                self.yaml_data = yaml_data
                return True

            return False
        except Exception as e:
            self.log.error("unable to update yaml section data. %s" %e)
            return False

    def write(self, section_name, section_data, default_flow_style=False):
        self.log.debug("write yaml format data to file %s." % self.path)
        try:
            # pack to an dictionary format
            yaml_data = {section_name: section_data}

            # append the yaml_data to the yaml file
            with open(self.yaml_path, 'w') as yaml_file:
                yaml.dump(yaml_data, yaml_file, default_flow_style=default_flow_style)
                self.log.debug("write section data to yaml file %s" % self.yaml_path)
                return True

            return False
        except Exception as e:
            self.log.error("unable to write yaml section data. %s" %e)
            return False
