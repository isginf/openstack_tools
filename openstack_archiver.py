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
import time
import json
import atexit
from multiprocessing import Pool, TimeoutError
from novaclient.exceptions import Conflict as NovaConflict
import keystoneclient.v2_0.client as keystone_client
import novaclient.v1_1.client as nova_client
from nova.compute import task_states
import glanceclient as glance_client
import cinderclient.client as cinder_client
from cinderclient.exceptions import ClientException, BadRequest
from glanceclient.exc import HTTPNotFound


#
# Configuration
#

admin_pass = ""
auth_url = "http://127.0.0.1:35357/v2.0"
glance_backup_prefix = "os_bkp_"
nova_snapshot_timeout = 10
nova_snapshot_tries = 600
glance_download_timeout = 600
cinder_backup_timeout = 10
cinder_backup_tries = 600


#
# Subroutines
#

def dump_openstack_obj(obj, out_file=None):
    """
    Unfortunately due to circular references openstack objects cannot
    be serialized automatically therefore we strip some stuff and
    export it as json
    If no output filename is given the json is returned as string
    Parameter: object to dump, output filename (optional)
    """
    dump = {}

    for (k, v) in obj.__dict__.items():
        if not k.startswith("_") and k != "manager":
            dump[k] = v

    if out_file:
        fh = open(out_file, "w")

        try:
            json.dump(dump, fh)
        except TypeError:
            pass

        fh.write("\n")
        fh.close()
    else:
        output = ""

        try:
            output = json.dumps(dump)
        except TypeError:
            pass

        return output


# Check if a directory exists and otherwise create it
# Parameter: directory name
def ensure_dir_exists(dir):
    """
    Create directory if it doesnt exist
    Params: directory name
    """
    if not os.path.exists(dir):
        os.mkdir(dir)


#
# KEYSTONE
#
def backup_keystone_user(backup_base_path, user):
    """
    Backup user meta data into a json file
    Params: backup directory name, user object
    """
    print "Backing up metadata of user " + user.name
    return dump_openstack_obj(user, os.path.join(backup_base_path, "keystone", "user_" + user.name + ".json"))


def backup_keystone(backup_base_path, tenant):
    """
    Backup all keystone data
    Params: backup directory name, tenant object
    """
    ensure_dir_exists(os.path.join(backup_base_path, "keystone"))

    print "Backing up metadata of tenant " + tenant.name
    dump_openstack_obj(tenant, os.path.join(backup_base_path, "keystone", "project.json"))
    [backup_keystone_user(backup_base_path, user) for user in tenant.list_users()]


#
# NOVA
#
def nova_check_snapshot_upload(params):
    """
    Check if a nova snapshot upload has finished
    Params: tupel of snapshot id, vm name
    Returns: True for success, False for failure or None for not finished
    """
    glance = get_glance_client()
    snapshot_id = params[0]
    vm_name = params[1]

    try:
        backup_image = glance.images.get(snapshot_id)
        print "Snapshot of " + vm_name + " " + backup_image.status

        if backup_image.status.lower() == 'active':
            return (snapshot_id, True)
    except glance_client.exc.HTTPNotFound, e:
        print "\nFailed to backup image of vm " + vm_name + "\n" + str(e) + "\n"
        return (snapshot_id, False)

    return (snapshot_id, None)


def wait_for_nova_snapshot_to_finish(backups):
    """
    Wait until all nova snapshot have finished (or failed)
    Params: dictionary of snapshot id and vm name
    """
    pool = Pool()
    glance = get_glance_client()
    snapshot_tries = nova_snapshot_tries

    while 1:
        jobs = pool.map_async(nova_check_snapshot_upload, backups.items())

        try:
            for (backup_id, success) in jobs.get(nova_snapshot_timeout):
                if success:
                    download_glance_image(backup_id, os.path.join(backup_base_path, "nova", "vm_" + backups[backup_id] + ".img"))
                    glance.images.delete(backup_id)

                if success == False:
                    del backups[backup_id]
                    glance.images.delete(backup_id)
        except HTTPNotFound:
            if backups.get(backup_id):
                del backups[backup_id]
        except TimeoutError:
            print "Got timeout"

        if len(backups) == 0 or snapshot_tries == 0:
           break
        else:
           snapshot_tries -= 1
           time.sleep(1)


def backup_nova_vm(srv):
    """
    Save vm meta data as json file and make a snapshot of the given vm
    Params: nova server object
    """
    print "Backing up metadata of vm " + srv.name
    dump_openstack_obj(srv, os.path.join(backup_base_path, "nova", "vm_" + srv.name + ".json"))

    print "Creating backup image of vm " + srv.name

    try:
        backup_image_id = srv.create_image(glance_backup_prefix + srv.name)
        return backup_image_id
    except NovaConflict, e:
        print "\nERROR creating snapshot of vm " + srv.name + "\n" + str(e) + "\n"
        return (None, None)


def backup_nova(backup_base_path, tenant):
    """
    Backup all nova data
    Params: backup directory name, tenant object
    """
    backups = {}
    nova = nova_client.Client(username='admin', api_key=admin_pass, auth_url=auth_url, project_id=tenant.name)
    glance = get_glance_client()

    ensure_dir_exists(os.path.join(backup_base_path, "nova"))

    for srv in nova.servers.list():
        backup_image_id = backup_nova_vm(srv)
        backups[backup_image_id] = srv.name

    # wait for snapshots to finish
    wait_for_nova_snapshot_to_finish(backups)


def cleanup_nova_backup():
    """
    On exit reset all active vms that are still in task image uploading
    """
    nova = nova_client.Client(username='admin', api_key=admin_pass, auth_url=auth_url, project_id=tenant.name)
    pool = Pool()
    vm_ids = (vm.id for vm in nova.servers.list() if getattr(vm, 'OS-EXT-STS:task_state') == task_states.IMAGE_UPLOADING and \
                                                     vm.status.lower() == 'active')

    jobs = pool.map_async(lambda vm: vm.reset_state('active'), vm_ids)
    jobs.get()


#
# GLANCE
#
def get_glance_client():
    """
    Return an instance of a glance client
    """
    glance_endpoint = keystone.service_catalog.url_for(service_type='image',
                                                       endpoint_type='publicURL')
    return glance_client.Client('2',glance_endpoint, token=keystone.auth_token)


def download_glance_image(image_id, output_file):
    """
    Download a glance image specified by image_id and save it into output_file
    Params: image_id, output_file name
    """
    glance = get_glance_client()
    print "Downloading image " + image_id
    fh = open(output_file, "wb")

    for chunk in glance.images.data(image_id):
        fh.write(chunk)

    fh.close()

def backup_glance_image(img):
    """
    Dump meta data of glance image in a json file and store the image in another file
    Params: image object
    """
    print "Backing up metadata of glance image " + img.name
    dump_openstack_obj(img, os.path.join(backup_base_path, "glance", img.name + ".json"))
    download_glance_image(img.id, os.path.join(backup_base_path, "glance", img.name + ".img"))


def backup_glance(backup_base_path, tenant):
    """
    Backup all glance data
    Params: backup directory name, tenant object
    """
    ensure_dir_exists(os.path.join(backup_base_path, "glance"))
    glance = get_glance_client()
    pool = Pool()

    jobs = pool.map_async(backup_glance_image, glance.images.list())
    jobs.get()


def cleanup_glance_backup_images():
    """
    At exit remove all glance images which names start with our backup prefix
    """
    glance = get_glance_client()
    pool = Pool()
    image_ids = (img.id for img in glance.images.list() if img.name.startswith(glance_backup_prefix))

    jobs = pool.map_async(glance.images.delete, image_ids)
    jobs.get()



#
# CINDER
#
def detach_volume(volume):
    """
    Detach the given cinder volume
    Params: volume object
    """
    if volume.status == 'in-use':
        try:
            volume.detach()
        except ClientException, e:
            print "ERROR volume " + volume.display_name + " could not be detached!\n" + str(e) + "\n"
            return False
    return True

def backup_cinder_volume(volume):
    """
    Save volume meta data as json file and trigger a backup of the volume
    Params: volume object
    """
    backup = None
    cinder = cinder_client.Client('1', 'admin', admin_pass, tenant.name, auth_url)

    if volume.status != "error_restoring":
        print "Backing up metadata of cinder volume " + volume.display_name
        dump_openstack_obj(volume, os.path.join(backup_base_path, "cinder", "vol_" + volume.display_name + ".json"))

        if detach_volume(volume):
            print "Backing up volume " + volume.display_name

            try:
                backup = cinder.backups.create(volume.id)
            except BadRequest, e:
                print "ERROR volume " + volume.display_name + " could not be backuped!\n" + str(e) + "\n"
            except ClientException, e:
                print "ERROR volume " + volume.display_name + " could not be backuped!\n" + str(e) + "\n"

    return backup


def backup_cinder(backup_base_path, tenant):
    """
    Backup all cinder data
    Params: backup directory name, tenant object
    """
    backups = []
    ensure_dir_exists(os.path.join(backup_base_path, "cinder"))
    cinder = cinder_client.Client('1', 'admin', admin_pass, tenant.name, auth_url)

    for volume in cinder.volumes.list():
        backup = backup_cinder_volume(volume)

        if backup:
            backups.append(backup.id)

    wait_for_cinder_backups_to_finish(backups)


def cinder_check_volume_backup(params):
    """
    Check if a backup of a volume has finished
    Params: tupel of index, volume backup id
    Returns: True for success, False for failure or None for not finished
    """
    index = params[0]
    backup_id = params[1]
    cinder = cinder_client.Client('1', 'admin', admin_pass, tenant.name, auth_url)

    try:
        backup = cinder.backups.get(backup_id)

        if backup.status != 'error' and backup.status != 'creating':
            return (index, backup_id, True)
        elif backup.status == 'error':
            return (index, backup_id, False)
    except ClientException, e:
        print "Could not get status of cinder volume " + str(backup_id) + " " + str(e)
        return (index, backup_id, False)

    return (index, backup_id, None)


def wait_for_cinder_backups_to_finish(backups):
    """
    Wait until all cinder backups have finished or failed
    Params: list of cinder backup ids
    """
    pool = Pool()
    glance = get_glance_client()
    backup_tries = cinder_backup_tries

    while 1:
        jobs = pool.map_async(cinder_check_volume_backup, backups)

        try:
            for (i, backup_id, success) in jobs.get(cinder_backup_timeout):
                if success:
                    print "Backup of volume " + backup_id + " finished."
                    del backups[i]
                elif success == False:
                    print "ERROR Backup of volume " + backup_id + " failed"
                    del backups[i]
        except TimeoutError:
            print "Got timeout"

        if len(backups) == 0 or backup_tries == 0:
           break
        else:
           backup_tries -= 1
           time.sleep(1)


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
    keystone = keystone_client.Client(auth_url=auth_url, username="admin", password=admin_pass, tenant_name="admin")

    # Retrieve tenant object
    tenant = None

    try:
        tenant = keystone.tenants.find(name=sys.argv[1])
    except (keystone_client.exceptions.NotFound, keystone_client.exceptions.NoUniqueMatch):
        tenant = keystone.tenants.get(sys.argv[1])

    # Create main backup directory and start all backups
    backup_base_path = '/local/openstack_backup/' + tenant.id
    ensure_dir_exists(backup_base_path)

    # Backup all stuff
    backup_keystone(backup_base_path, tenant)
    backup_nova(backup_base_path, tenant)
    backup_glance(backup_base_path, tenant)
    backup_cinder(backup_base_path, tenant)

    # Clean up at the end
    atexit.register(cleanup_nova_backup)
    atexit.register(cleanup_glance_backup_images)
