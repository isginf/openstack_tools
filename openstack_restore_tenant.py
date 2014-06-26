#!/usr/bin/python
#
# Backup all data and metadata of an Openstack tenant (or project or
# whatever it's called) into a directory
# Parallel backup edition
#
# Copyright 2014 ETH Zurich, ISGINF, Bastian Ballmann
# Email: bastian.ballmann@inf.ethz.ch
# Web: http://www.isg.inf.ethz.ch
#
# This is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# It is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License.
# If not, see <http://www.gnu.org/licenses/>.


#
# Loading modules
#

import os
import sys
from openstack_lib import restore_keystone


#
# MAIN PART
#

# Check if we got enough params
if len(sys.argv) < 2:
    print sys.argv[0] + " <tenant_id>"
    sys.exit(1)

tenant_id = sys.argv[1]

# dont buffer stdout
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

restore_keystone(tenant_id)
