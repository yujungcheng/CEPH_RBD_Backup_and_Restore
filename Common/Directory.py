import os, subprocess, datetime, time

# for manage directory that rbd export to
# require command: du, find, awk

import os, sys, subprocess

class Directory(object):
    def __init__(self, log, path):
        self.log = log

        self.path = None
        self.available_bytes = 0
        self.used_bytes = 0
        self.retain_count = 1

        # verify and create the path
        if os.path.isfile(path):
            self.log.error("the path is a regular file. %s" % path)
        else:
            if not os.path.isdir(path):
                self.log.info("create the directory %s." % path)
                os.system("mkdir -p %s" % path)
            self.path = path

    def check_size(self):
        try:
            self.get_available_size()
            self.get_used_size()
            self.log.info("check size of directory path %s\n"
                          "available bytes = %s\n"
                          "used bytes = %s \n" % (
                          self.path,
                          self.available_bytes,
                          self.used_bytes))
            return True
        except Exception as e:
            self.log.error("unable to check size of %s. %s" %(self.path, e))
            return False

    def _exec_cmd(self, cmd):
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        output = p.communicate()[0]
        return_code = p.returncode
        self.log.debug((cmd, output))
        if return_code == 0:
            return output

        print cmd, return_code
        return ''

    def get_available_size(self):
        try:
            cmd = "df -k --output=avail %s | tail -1" % self.path
            self.available_bytes = int(self._exec_cmd(cmd)) * 1024
            return self.available_bytes
        except:
            self.log.error("Get path '%s' available bytes failed!" % (
                self.path))
            return False

    def get_used_size(self):
        try:
            cmd = "du -sk %s | awk '{print $1}'" % self.path
            self.used_bytes = int(self._exec_cmd(cmd)) * 1024
            return self.used_bytes
        except:
            self.log.error("Get path '%s' used bytes failed!" % (
                self.path))
            return False

    def get_file_list(self, get_count=False):
        try:
            cmd = "find %s -type f" % self.path
            return_str = str(self._exec_cmd(cmd))
            self.file_list = return_str.splitlines()
            if get_count:
                return len(self.file_list)
            return self.file_list
        except Exception as e:
            self.log.error("Get number of file in %s failed! %s" % (self.path, e))
            return False

    def get_directory_list(self, get_count=False):
        try:
            cmd = "find %s -type d -not -path %s" % (self.path, self.path)
            return_str = str(self._exec_cmd(cmd))
            self.directory_list = return_str.splitlines()
            if get_count:
                return len(self.directory_list)
            return self.directory_list
        except Exception as e:
            self.log.error("get sub directory list in %s" % (self.path, e))
            return False

    def add_directory(self, *args):
        try:
            sub_path = ''
            for directory in args:
                sub_path = os.path.join(sub_path, directory)
            full_path = os.path.join(self.path, sub_path)

            if os.path.isdir(full_path):
                return full_path
            elif os.path.isfile(full_path):
                self.log.error("the %s is a file in %s" %(full_path, self.path))
                return False
            else:
                self.log.info("add directory %s in %s" %(sub_path, self.path))
                cmd = "mkdir -p %s" %(full_path)
                self._exec_cmd(cmd)
                return full_path
        except Exception as e:
            self.log.error("create directory in %s failed. %s" % (self.path, e))
            return False

    def set_path(self, path, check=True):
        self.log.info("set directory path to %s" % path)
        self.path = path
        if check:
            self.check_size()

    def del_directory(self, name):
        try:
            full_path = os.path.join(self.path, name)
            cmd = "rm -rf %s" %(full_path)
            self._exec_cmd(cmd)
            return True
        except Exception as e:
            self.log.error("unable to delete directory %s" % name)
            return False

    def find_file(self, name):
        file_path = os.phat.join(self.path, name)
        if os.path.isfile(file_path):
            return True
        else:
            return False
