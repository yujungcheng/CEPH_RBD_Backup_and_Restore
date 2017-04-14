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
INITIAL  = 1
EXECUTE  = 2
COMPLETE = 3
ERROR    = 4

# worker status
# ------------------------------------------------------------------------------
READY    = 1
WAIT     = 2
STOP     = 3
RUN      = 4
REST     = 5

# rbd export type
# ------------------------------------------------------------------------------
FULL = 0
DIFF = 1
EXPORT_TYP = ['full', 'diff']

# snapshot operation type
# ------------------------------------------------------------------------------
CREATE = 0
REMOVE = 1
PURGE  = 2
SNAP_ACT = ['create', 'remove', 'purge']

# shared memory for metafile, unused
# ------------------------------------------------------------------------------
METAFILE_SHM_PATH = '/run/shm'

# metadata filenames
# ------------------------------------------------------------------------------
BACKUP_INFO                 = 'meta.backup_info'
RBD_INFO_LIST               = 'meta.rbd_info_list'
RBD_SNAPSHOT_MAINTAIN_LIST  = 'meta.rbd_snapshot_maintain_list'
RBD_BACKUP_CIRCULATION_LIST = 'meta.rbd_backup_circulation_list'
