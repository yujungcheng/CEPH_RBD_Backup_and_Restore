#!/usr/bin/env python
# -*- coding: utf-8 -*-


# default config variable
# ------------------------------------------------------------------------------
DEFAULT_BACKUP_CONFIG_FILE = './Config/backup.conf'
DEFAULT_BACKUP_CONFIG_SECTION = 'ceph'
DEFAULT_BACKUP_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
DEFAULT_TASK_TIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f'

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
EXPORT_TYP = ['full', 'diff']

# snapshot operation type
# ------------------------------------------------------------------------------
CREATE = 0
DELETE = 1
PURGE  = 2
SNAP_ACT = ['create', 'delete', 'purge']

# shared memory for metafile
# ------------------------------------------------------------------------------
METAFILE_SHM_PATH = '/run/shm'

# metadata filenames
# ------------------------------------------------------------------------------
BACKUP_INFO        = 'meta.backup_info'

RBD_LIST        = 'meta.rbd_list'
SNAPSHOT_LIST      = 'meta.snapshot_list'

LAST_SNAPSHOT_NAME = 'meta.last_snapshot_name'
NEW_SNAPSHOT_NAME  = 'meta.this_snapshot_name'
MAINTAIN_SNAPSHOT_NAME = 'meta.trace_snapshot_name'

TASK_RESULT        = 'meta.task_result'
