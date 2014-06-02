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
import json
import atexit
import functools
from time import sleep
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

glance_backup_prefix = "os_bkp"
glance_upload_timeout = 900
glance_upload_wait = 900
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
def backup_nova_vm(srv):
    """
    Save vm meta data as json file and make a snapshot of the given vm
    Params: nova server object
    """
    bad_status = ['Error', 'image_uploading']
    print "Backing up metadata of vm " + srv.name
    dump_openstack_obj(srv, os.path.join(backup_base_path, "nova", "vm_" + srv.name + ".json"))

    # reset vm if it's in a bad state for image uploading
    if srv.status in bad_status or getattr(srv, 'OS-EXT-STS:task_state') in bad_status:
        print "Vm " + srv.name + " in bad state " + srv.status + " (" + getattr(srv, 'OS-EXT-STS:task_state') + "). Resetting."
        srv.reset_state('active')
        sleep(1)

    print "Creating backup image of vm " + srv.name

    try:
        backup_id = srv.create_image(glance_backup_prefix + "_" + tenant.name + "_" + srv.name)
        return backup_id
    except NovaConflict, e:
        print "\nERROR creating snapshot of vm " + srv.name + "\n" + str(e) + "\n"
        return None


def backup_nova(backup_base_path, tenant):
    """
    Backup all nova data
    Params: backup directory name, tenant object
    """
    backups = {}
    nova = nova_client.Client(username=os.environ["OS_USERNAME"],
                              api_key=os.environ["OS_PASSWORD"],
                              auth_url=os.environ["OS_AUTH_URL"],
                              project_id=tenant.name)
    glance = get_glance_client()

    ensure_dir_exists(os.path.join(backup_base_path, "nova"))

    for srv in nova.servers.list():
        backup_image_id = backup_nova_vm(srv)

        if backup_image_id:
            backups[backup_image_id] = srv.name

    # wait for snapshots to finish
    wait_for_glance_upload_to_finish(backups, output_dir="nova")


def cleanup_nova_backup():
    """
    On exit reset all active vms that are still in task image uploading
    """
    nova = nova_client.Client(username=os.environ["OS_USERNAME"],
                              api_key=os.environ["OS_PASSWORD"],
                              auth_url=os.environ["OS_AUTH_URL"],
                              project_id=tenant.name)
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


def glance_check_upload(params, output_dir):
    """
    Check if an upload to glance has finished
    If one has finished start a download immediately
    Params: tupel of image id, name for output, output directory name relative to backup_base_path
    Returns: True for success, False for failure or None for not finished
    """
    glance = get_glance_client()
    image_id = params[0]
    display_name = params[1]

    try:
        backup_image = glance.images.get(image_id)
        print "Upload of " + display_name + " is " + backup_image.status

        if backup_image.status.lower() == 'active':
            download_glance_image(image_id, os.path.join(backup_base_path, output_dir, display_name + ".img"))
            return (image_id, True)
    except glance_client.exc.HTTPNotFound, e:
        print "\nFailed to get status of image " + display_name + "\n" + str(e) + "\n"
        return (image_id, False)

    return (image_id, None)


# No functools.partial with multiprocess on 2.6 :(
# (see http://bugs.python.org/issue5228)
def nova_glance_check_upload(params):
    return glance_check_upload(params, "nova")

def cinder_glance_check_upload(params):
    return glance_check_upload(params, "cinder")

def wait_for_glance_upload_to_finish(backups, output_dir):
    """
    Wait until all glance uploads have finished (or failed)
    Params: dictionary of snapshot id and vm name
    """
    pool = Pool()
    check_func = None
    glance = get_glance_client()
    upload_wait = glance_upload_wait

    if output_dir == "nova":
        check_func = nova_glance_check_upload
    else:
        check_func = cinder_glance_check_upload

    while 1:
        try:
            jobs = pool.map_async(check_func, backups.items())

            for (backup_id, success) in jobs.get(glance_upload_timeout):
                if success:
                    glance.images.delete(backup_id)

                if success == False:
                    del backups[backup_id]
                    glance.images.delete(backup_id)
        except HTTPNotFound:
            if backups.get(backup_id):
                del backups[backup_id]
        except TimeoutError:
            print "Got timeout"

        if len(backups) == 0 or upload_wait == 0:
           break
        else:
           upload_wait -= 1
           sleep(3)


def download_glance_image(image_id, output_file):
    """
    Download a glance image specified by image_id and save it into output_file
    Params: image_id, output_file name
    """
    glance = get_glance_client()

    print "Downloading image " + image_id
    fh = open(output_file, "wb")

    try:
        for chunk in glance.images.data(image_id):
            fh.write(chunk)

    except HTTPNotFound, e:
        print "Error downloading image " + image_id + ": " + str(e)
    finally:
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
    backup_id = None
    backup_name = None
    cinder = cinder_client.Client('1',
                                  os.environ["OS_USERNAME"],
                                  os.environ["OS_PASSWORD"],
                                  tenant.name,
                                  os.environ["OS_AUTH_URL"])

    print "Backing up metadata of cinder volume " + volume.display_name
    dump_openstack_obj(volume, os.path.join(backup_base_path, "cinder", "vol_" + volume.display_name + ".json"))

    if detach_volume(volume):
        print "Backing up volume " + volume.display_name

        try:
            resp = cinder.volumes.upload_to_image(volume, True, glance_backup_prefix + "_" + tenant.name + "_" + volume.display_name, "bare", "raw")
            backup_id = resp[1]['os-volume_upload_image']['image_id']
            backup_name = resp[1]['os-volume_upload_image']['image_name']
        except BadRequest, e:
            print "ERROR volume " + volume.display_name + " could not be backuped!\n" + str(e) + "\n"
        except ClientException, e:
            print "ERROR volume " + volume.display_name + " could not be backuped!\n" + str(e) + "\n"

    return (backup_id, backup_name)


def backup_cinder(backup_base_path, tenant):
    """
    Backup all cinder data
    Params: backup directory name, tenant object
    """
    backups = {}
    ensure_dir_exists(os.path.join(backup_base_path, "cinder"))
    cinder = cinder_client.Client('1',
                                  os.environ["OS_USERNAME"],
                                  os.environ["OS_PASSWORD"],
                                  tenant.name,
                                  os.environ["OS_AUTH_URL"])

    for volume in cinder.volumes.list():
        (backup_id, backup_name) = backup_cinder_volume(volume)

        if backup_id:
            backups[backup_id] = backup_name

    wait_for_glance_upload_to_finish(backups, output_dir="cinder")


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
    keystone = keystone_client.Client(auth_url=os.environ["OS_AUTH_URL"],
                                      username=os.environ["OS_USERNAME"],
                                      password=os.environ["OS_PASSWORD"],
                                      tenant_name=os.environ["OS_TENANT_NAME"])

    # Retrieve tenant object
    tenant = None

    try:
        tenant = keystone.tenants.find(name=sys.argv[1])
    except (keystone_client.exceptions.NotFound, keystone_client.exceptions.NoUniqueMatch):
        tenant = keystone.tenants.get(sys.argv[1])

    # Create main backup directory and start all backups
    backup_base_path = '/local/openstack_backup/' + tenant.id
    ensure_dir_exists(backup_base_path)

    # Check that admin user is in the tenant we want to backup
    # otherwise add him
    if not filter(lambda x: x.username == os.environ['OS_USERNAME'], tenant.list_users()):
        tenant.add_user(keystone.users.find(name = os.environ['OS_USERNAME']),
                        keystone.roles.find(name = 'admin'))

    # Backup all stuff
    backup_keystone(backup_base_path, tenant)
    backup_nova(backup_base_path, tenant)
    backup_glance(backup_base_path, tenant)
    backup_cinder(backup_base_path, tenant)

    # Clean up at the end
    atexit.register(cleanup_nova_backup)
    atexit.register(cleanup_glance_backup_images)
