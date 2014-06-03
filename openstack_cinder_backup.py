#!/usr/bin/python
#
# Backup all cinder volumes which names start with backupme
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

# TODO: needs to be able to delete images on disk that have been deleted in cinder

#
# Loading modules
#

import os
import sys
from time import sleep
from multiprocessing import Pool, TimeoutError
import keystoneclient.v2_0.client as keystone_client
from cinderclient.exceptions import ClientException, BadRequest, Unauthorized
from openstack_lib import get_keystone_client, get_cinder_client, get_backup_base_path, ensure_dir_exists
from openstack_lib import backup_cinder_volume, wait_for_glance_upload_to_finish, attach_volume
import openstack_lib


#
# Subroutines
#
def backup_tenant(tenant):
    cinder = get_cinder_client(tenant)
    volumes = cinder.volumes.list()

    if len(volumes) == 0:
       return (tenant.id, None)

    # Check that admin user is in the tenant we want to backup
    # otherwise add him
    if not filter(lambda x: x.username == os.environ['OS_USERNAME'], tenant.list_users()):
        keystone = get_keystone_client()
        tenant.add_user(keystone.users.find(name = os.environ['OS_USERNAME']),
                        keystone.roles.find(name = 'admin'))

    backups = {}
    attached = {}

    ensure_dir_exists(os.path.join(get_backup_base_path(tenant), "cinder"))

    for volume in volumes:
        if volume.display_name.startswith("backupme"):
            (backup_id, backup_name) = backup_cinder_volume(tenant, volume)

            if backup_id:
                backups[backup_id] = backup_name

                if len(volume.attachments) > 0:
                    attached[backup_id] = {}
                    attached[backup_id]['vm'] = volume.attachments[0]['server_id']
                    attached[backup_id]['volume'] = volume.id
                    attached[backup_id]['device'] = volume.attachments[0]['device']

    wait_for_glance_upload_to_finish(backups, tenant, output_dir="")

    # Reattach volumes
    for info in attached.values():
        attach_volume(tenant, info['volume'], info['vm'], info['device'])

    return tenant.id, True


#
# MAIN PART
#

# dont buffer stdout
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

# Get keystone client
keystone = get_keystone_client()

openstack_lib.BACKUP_BASE_PATH = "/var/cinder_backup"

for tenant in keystone.tenants.list():
    backup_tenant(tenant)
