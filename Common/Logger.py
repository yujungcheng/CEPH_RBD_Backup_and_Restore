#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
import logging.handlers
import inspect

# A wrapper class of logging
class Logger(object):
    def __init__(self, cfg):
        '''
        the cfg has to be object of Config class.
        todo: make it accept to config logging generally
        '''
        self.file = cfg.log_file
        self.path = cfg.log_path
        self.level = cfg.log_level
        self.max_bytes = cfg.log_max_bytes
        self.format_type = cfg.log_format_type
        self.backup_count = cfg.log_backup_count
        self.delay = cfg.log_delay

        self.logger = None
        self.file_path = os.path.join(cfg.log_path, cfg.log_file)
        self.formats = {'0': '[%(asctime)s] [%(levelname)s] %(message)s',
                        '1': '[%(asctime)s] [%(levelname)s] %(message)s',
                        '2': '[%(asctime)s] %(message)s',
                        '3': '%(message)s'}

        self.stage = None

        self.log_option_dict = {'file': self.file,
                                'path': self.path,
                                'level': self.level,
                                'max_bytes': self.max_bytes,
                                'format_type': self.format_type,
                                'backup_count': self.backup_count,
                                'delay': self.delay}
    def __str__(self):
        return str(self.log_option_dict)

    def set_stage(self, stage):
        self.logger.debug("set logger stage to %s" % stage)
        #self.stage = stage

    def _get_space(self, indent_count):
        return " " * indent_count

    def set_logger(self, name='logger'):
        try:
            # initialize logging directory
            os.system("mkdir -p %s" % self.path)

            # create logger
            logger = logging.getLogger(name)
            logger.setLevel(self.level)

            # create handler for rotating file
            log_handler = logging.handlers.RotatingFileHandler(
                          self.file_path,
                          maxBytes=self.max_bytes,
                          backupCount=self.backup_count,
                          delay=self.delay)
            log_handler.setLevel(self.level)

            # create formatter
            log_format = self.formats[self.format_type]
            formatter = logging.Formatter(log_format)

            # set formatter to handler and add handler to logger
            log_handler.setFormatter(formatter)
            logger.addHandler(log_handler)

            self.logger = logger

            self.info(("Logger initialized.", self.log_option_dict))
            return True
        except Exception as e:
            print e
            return False

    def get_logger(self):
        return self.logger

    def blank_line(self, line_count=1):
        cmd = "echo '' >> %s" % self.file_path
        for i in range(line_count):
            os.system(cmd)
        return True

    def start_line(self, title='', symbol='*', symbol_count=16):
        symbol_str = symbol * symbol_count
        start_line = "%s%s%s" % (symbol_str, title, symbol_str)
        os.system("echo '%s' >> %s" %(start_line, self.file_path))
        return True

    def _convert_msg(self, msg):
        try:
            n_msg = ''
            if isinstance(msg, dict):
                for key, value in msg.iteritems():
                    #n_value = self._convert_msg(value)

                    n_msg = "".join([n_msg, '\n',str(key), ' = ', str(value)])
                return n_msg[1:]
            elif isinstance(msg, list):
                for item in msg:
                    n_msg = "".join([n_msg, '\n', str(item)])
                return n_msg[1:]
            elif isinstance(msg, tuple):
                # use carefully
                for item in msg:
                    t_msg = self._convert_msg(item)
                    n_msg = "".join([n_msg, '\n', str(t_msg)])
                return n_msg[1:]

            #self.logger.warning("unknown log message %s, type=%s, convert to string." %(msg, type(msg)))
            return str(msg)
        except Exception as e:
            self.logger.error("fail to convert log message type. %s" % e)
            return str(msg)

    def _indent_msg(self, msg, space_count=25):
        if isinstance(msg, str):
            n_msg = msg
        else:
            n_msg = self._convert_msg(msg)

        if n_msg[:1] == '\n':
            n_msg = n_msg[1:]
            self.blank_line()

        if self.format_type == '2':
            n_space_count = 25
        else:
            n_space_count = space_count

        if self.stage is not None:
            n_msg = ''.join(['[', str(self.stage), '] ', n_msg])
            n_space_count += len(str(self.stage))+3

        if self.format_type == '0':
            frame = inspect.stack()[2]
            module = inspect.getmodulename(frame[1])

            n_msg = ''.join(['[', module, '] ', n_msg])
            n_space_count += len(module)+4




        line_list = str(n_msg).splitlines()

        space_indent = self._get_space(n_space_count)
        line_count = len(line_list)
        for i in range(1, line_count):
            line_list[i] = space_indent.join(['\n', line_list[i]])

        return "".join(line_list)

    def info(self, log_msg):
        indent_msg = self._indent_msg(log_msg, space_count=32)
        self.logger.info(indent_msg)

    def error(self, log_msg):
        indent_msg = self._indent_msg(log_msg, space_count=33)
        self.logger.error(indent_msg)

    def warning(self, log_msg):
        indent_msg = self._indent_msg(log_msg, space_count=35)
        self.logger.warning(indent_msg)

    def debug(self, log_msg):
        indent_msg = self._indent_msg(log_msg, space_count=33)
        self.logger.debug(indent_msg)
