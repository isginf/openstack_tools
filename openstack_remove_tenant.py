#!/usr/bin/python
#
# Remove all data of an Openstack tenant (or project or whatever it's called)
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
import time
import string
from multiprocessing import Pool, TimeoutError
from novaclient.exceptions import Conflict as NovaConflict
import keystoneclient.v2_0.client as keystone_client
import novaclient.v1_1.client as nova_client
import glanceclient as glance_client
import cinderclient.client as cinder_client
from cinderclient.exceptions import ClientException, BadRequest
from glanceclient.exc import HTTPNotFound


#
# Configuration
#

admin_pass = ""
auth_url = "http://127.0.0.1:35357/v2.0"
vm_shutdown_timeout = 30


#
# Subroutines
#

def remove_glance_images(tenant):
    """
    Delete all glance images
    Params: tenant object
    """
    glance_endpoint = keystone.service_catalog.url_for(service_type='image',
                                                       endpoint_type='publicURL')
    glance = glance_client.Client('2',glance_endpoint, token=keystone.auth_token)
    image_ids = (img.id for img in glance.images.list() if img.owner == tenant.id and not img.visibility == 'public')

    for img in image_ids:
        print "Removing image " + img

        try:
            glance.images.delete(img)
        except HTTPNotFound:
            print "Could not find image " + img


def remove_nova_vms(tenant):
    """
    Delete all nova vms
    Params: tenant object
    """
    stopping_vms = []
    nova = nova_client.Client(username='admin', api_key=admin_pass, auth_url=auth_url, project_id=tenant.name)

    for vm in nova.servers.list():
        if string.lower(vm.status) == 'active':
            print "Stopping vm " + vm.name
            vm.stop()
            stopping_vms.append(vm.id)

    if len(stopping_vms) > 0:
        print "Waiting " + str(vm_shutdown_timeout) + " seconds for vms to shutdown"
        time.sleep(vm_shutdown_timeout)

    for vm in nova.servers.list():
        print "Removing vm " + vm.name
        vm.delete()


def remove_cinder_volumes(tenant):
    """
    Delete all cinder volumes
    Params: tenant object
    """
    cinder = cinder_client.Client('1', 'admin', admin_pass, tenant.name, auth_url)

    for volume in cinder.volumes.list():
        if volume.status == 'in-use':
            try:
                volume.detach()
            except ClientException, e:
                print "Volume " + volume.display_name + " could not be detached!\n" + str(e) + "\n"

        print "Removing volume " + volume.display_name

        try:
            volume.delete()
        except ClientException, e:
            print "Could not remove volume " + volume.display_name + " " + str(e)


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

    # Get keystone client and tenant
    keystone = keystone_client.Client(auth_url=auth_url, username="admin", password=admin_pass, tenant_name="admin")
    tenant = None

    try:
        tenant = keystone.tenants.find(name=sys.argv[1])
    except (keystone_client.exceptions.NotFound, keystone_client.exceptions.NoUniqueMatch):
        tenant = keystone.tenants.get(sys.argv[1])

    # Delete all stuff
    remove_glance_images(tenant)
    remove_nova_vms(tenant)
    remove_cinder_volumes(tenant)

    # If a user with the same name of tenant exists delete it too
    try:
        user = keystone.users.find(name=tenant.name)
        user.delete()
    except (keystone_client.exceptions.NotFound, keystone_client.exceptions.NoUniqueMatch):
        pass

    tenant.delete()
