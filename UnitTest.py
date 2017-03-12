#!/usr/bin/env python
# -*- coding: utf-8 -*-

import rados
from rbd import RBD, Image



cluster = rados.Rados(conffile='')
cluster.connect()
ioctx = cluster.open_ioctx("rbd")

rbd = RBD()

image = Image(ioctx, "rbd0")
print("zzzzzzzzzzz")
size = image.size()
print("xxxxxxxxxxx")
image.close()


print size
