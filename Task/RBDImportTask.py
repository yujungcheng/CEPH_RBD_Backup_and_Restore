#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time, datetime

from Common.Constant import *
from Common.BaseTask import BaseTask


class RBDImportTask(BaseTask)
    def __init__(self, cluster_name, pool_name, rbd_name, export_destpath,
                 export_type=FULL, from_snap=None, to_snap=None, rbd_id=None):
        super(RBDExportTask, self).__init__()

        pass
