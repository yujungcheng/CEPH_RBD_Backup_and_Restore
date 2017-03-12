#!/usr/bin/env python
# -*- coding: utf-8 -*-

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


# rbd backup ymal section name
# ------------------------------------------------------------------------------
LAST_SNAPSHOT_NAME = 'last_rbd_snapshot_name'
FULL_EXPORT_LIST = 'full_export_list'
DIFF_EXPORT_LIST = 'diff_export_list'


# rbd export type
# ------------------------------------------------------------------------------
FULL = 0
DIFF = 1


# metadata format
# ------------------------------------------------------------------------------
YAML = 0
LOG  = 1
