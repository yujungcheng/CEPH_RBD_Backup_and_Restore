#!/usr/bin/env python
# -*- coding: utf-8 -*-


# default config variable
# ------------------------------------------------------------------------------
DEFAULT_BACKUP_CONFIG_FILE = './Config/backup.conf'
DEFAULT_BACKUP_CONFIG_SECTION = 'ceph'
DEFAULT_BACKUP_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

# task status
# ------------------------------------------------------------------------------
INITIAL  = 0
EXECUTE  = 1
COMPLETE = 2
ERROR    = 3


# worker status
# ------------------------------------------------------------------------------
READY    = 0
WAIT     = 1
STOP     = 2
RUN      = 3
REST     = 4

# rbd export type
# ------------------------------------------------------------------------------
FULL = 0
DIFF = 1

# snapshot operation type
# ------------------------------------------------------------------------------
CREATE = 0
DELETE = 1

# shared memory for metafile
# ------------------------------------------------------------------------------
METAFILE_SHM_PATH = '/run/shm'

# metadata filenames
# ------------------------------------------------------------------------------
BACKUP_INFO        = '.meta.backup_info'
LAST_SNAPSHOT_NAME = '.meta.last_snapshot_name'
NEW_SNAPSHOT_NAME  = '.meta.this_snapshot_name'
TASK_RESULT        = '.meta.task_result'
