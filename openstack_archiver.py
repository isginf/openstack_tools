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
import atexit
import keystoneclient.v2_0.client as keystone_client
from openstack_lib import get_keystone_client, backup_keystone, backup_nova, backup_glance, backup_cinder
from openstack_lib import get_backup_base_path, ensure_dir_exists, cleanup_nova_backup, cleanup_glance_backup


#
# MAIN PART
#

if __name__ == '__main__':
    # Check if we got enough params
    if len(sys.argv) < 2:
        print sys.argv[0] + " <tenant_id/_name>"
        sys.exit(1)

    # dont buffer stdout
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

    # Get keystone client
    keystone = get_keystone_client()

    # Retrieve tenant object
    tenant = None

    try:
        tenant = keystone.tenants.find(name=sys.argv[1])
    except (keystone_client.exceptions.NotFound, keystone_client.exceptions.NoUniqueMatch):
        tenant = keystone.tenants.get(sys.argv[1])

    ensure_dir_exists(get_backup_base_path(tenant.id))

    # Check that admin user is in the tenant we want to backup
    # otherwise add him
    if not filter(lambda x: x.username == os.environ['OS_USERNAME'], tenant.list_users()):
        tenant.add_user(keystone.users.find(name = os.environ['OS_USERNAME']),
                        keystone.roles.find(name = 'admin'))

    # Backup all stuff
    backup_keystone(tenant)
    backup_nova(tenant)
    backup_glance(tenant)
    backup_cinder(tenant)

    # Clean up at the end
    atexit.register(lambda: cleanup_nova_backup(tenant))
    atexit.register(cleanup_glance_backup)
