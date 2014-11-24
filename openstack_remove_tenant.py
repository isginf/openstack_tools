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
from multiprocessing import Pool, TimeoutError
from novaclient.exceptions import Conflict as NovaConflict
import keystoneclient.v2_0.client as keystone_client
import novaclient.v1_1.client as nova_client
import glanceclient as glance_client
import cinderclient.client as cinder_client
from cinderclient.exceptions import ClientException, BadRequest
from glanceclient.exc import HTTPNotFound
from neutronclient.neutron import client as neutron_client
from neutronclient.common.exceptions import NeutronClientException


#
# Configuration
#

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
    image_ids = (img.id for img in glance.images.list() if img.owner == tenant.id and not img.visibility == 'public' and not img.status == 'deleted')

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
    nova = nova_client.Client(username=os.environ["OS_USERNAME"],
                              api_key=os.environ["OS_PASSWORD"],
                              auth_url=os.environ["OS_AUTH_URL"],
                              project_id=tenant.name)

    for vm in nova.servers.list():
        if vm.status.lower() == 'active':
            print "Stopping vm " + vm.name
            
            try:
                vm.stop()
                stopping_vms.append(vm.id)
            except NovaConflict, e:
                print "Could not stop vm " + vm.name + ": " + str(e)

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
    cinder = cinder_client.Client('1',
                                  os.environ["OS_USERNAME"],
                                  os.environ["OS_PASSWORD"],
                                  tenant.name,
                                  os.environ["OS_AUTH_URL"])

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


def remove_neutron_networks(tenant):
    """
    Delete all neutron ports, subnets, networks and routers
    Params: tenant object
    """
    neutron = neutron_client.Client('2.0',
                                    username=os.environ["OS_USERNAME"],
                                    password=os.environ["OS_PASSWORD"],
                                    tenant_name=tenant.name,
                                    auth_url=os.environ["OS_AUTH_URL"])

    try:
        # Remove security groups and their rules
        for security_group_rule in neutron.list_security_group_rules(tenant_id=tenant.id)['security_group_rules']:
            neutron.delete_security_group_rule(security_group_rule['id'])

        for security_group in neutron.list_security_groups(tenant_id=tenant.id)['security_groups']:
            for security_group_rule in security_group['security_group_rules']:
                neutron.delete_security_group_rule(security_group_rule['id'])

            print "Deleting security group " + str(security_group['id'])
            neutron.delete_security_group(security_group['id'])
    except NeutronClientException, e:
        print "Neutron command failed. " + str(e)

    try:
        # Remove floating ips
        for floating_ip in neutron.list_floatingips(tenant_id=tenant.id)['floatingips']:
            print "Deleting floating ip " + str(floating_ip['id'])
            neutron.delete_floatingip(floating_ip['id'])

        # Remove router interfaces
        for router in neutron.list_routers(tenant_id=tenant.id)['routers']:
            for port in neutron.list_ports(device_id=router['id'])['ports']:
                for subnet in port['fixed_ips']:
                    # not an interface to the external net
                    if not neutron.show_network(neutron.show_subnet(subnet['subnet_id'])['subnet']['network_id'])['network']['router:external']:
                        print "Deleting router interface " + port['id']
                        neutron.remove_interface_router(str(router['id']), {'subnet_id': subnet['subnet_id']})

        # Remove remaining ports
        for port in neutron.list_ports(tenant_id=tenant.id)['ports']:
            print "Deleting port " + port['id']
            neutron.delete_port(port['id'])

        # Remove quotas
        for quota in neutron.list_quotas(tenant_id=tenant.id)['quotas']:
            print "Deleting quota " + str(quota['id'])
            neutron.delete_quota(quota['id'])

        # Remove networks and their subnets
        for network in neutron.list_networks(tenant_id=tenant.id)['networks']:
            for subnet in network['subnets']:
                print "Deleting subnet " + subnet
                neutron.delete_subnet(subnet)

            print "Deleting network " + network['name']
            neutron.delete_network(network['id'])

        # Remove router
        for router in neutron.list_routers(tenant_id=tenant.id)['routers']:
            print "Deleting router " + router['name']
            neutron.delete_router(router['id'])
    except NeutronClientException, e:
        print "Neutron command failed. " + str(e)


#
# MAIN PART
#

if __name__ == '__main__':
    # Check if we got enough params
    if len(sys.argv) < 2:
        print sys.argv[0] + " <tenant_id/_name> [subsystem]"
        sys.exit(1)

    # dont buffer stdout
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

    subsystems = {'glance': remove_glance_images,
                  'nova': remove_nova_vms,
                  'cinder': remove_cinder_volumes,
                  'neutron': remove_neutron_networks}

    # Get keystone client and tenant
    keystone = keystone_client.Client(auth_url=os.environ["OS_AUTH_URL"],
                                      username=os.environ["OS_USERNAME"],
                                      password=os.environ["OS_PASSWORD"],
                                      tenant_name="admin")
    tenant = None

    try:
        tenant = keystone.tenants.find(name=sys.argv[1])
    except (keystone_client.exceptions.NotFound, keystone_client.exceptions.NoUniqueMatch):
        try:
            tenant = keystone.tenants.get(sys.argv[1])
        except keystone_client.exceptions.NotFound:
            print "Tenant " + sys.argv[1] + " does not exist"
            sys.exit(1)

    # Check that admin user is in the tenant we want to backup
    # otherwise add him
    if not filter(lambda x: x.username == os.environ['OS_USERNAME'], tenant.list_users()):
        tenant.add_user(keystone.users.find(name = os.environ['OS_USERNAME']),
                        keystone.roles.find(name = 'admin'))

    # Remove only one subsystem?
    if len(sys.argv) > 2:
        subsystem_func = subsystems.get(sys.argv[2])

        if subsystem_func:
            subsystem_func(tenant)
        else:
            print "Unknown subsystem " + sys.argv[2]

    # Delete all stuff
    else:
        remove_glance_images(tenant)
        remove_nova_vms(tenant)
        remove_cinder_volumes(tenant)
        remove_neutron_networks(tenant)

        # If a user with the same name of tenant exists delete it too
        try:
            user = keystone.users.find(name=tenant.name)
            user.delete()
        except (keystone_client.exceptions.NotFound, keystone_client.exceptions.NoUniqueMatch):
            pass

        tenant.delete()
