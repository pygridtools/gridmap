#!/usr/bin/env python

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# Written (W) 2011-2012 Christian Widmer
# Copyright (C) 2011-2012 Max-Planck-Society

import time

def wait_a_bit(n):
    """
    computes factorial of n
    """

    time.sleep(20)

    ret = 1
    for i in xrange(n):
        ret=ret*(i+1)

    return ret


