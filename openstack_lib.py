#
# This file contains functions used by INF Openstack tools
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
import functools
from time import sleep
from pickle import PicklingError
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

GLANCE_BACKUP_PREFIX = "os_bkp"
GLANCE_UPLOAD_TIMEOUT = 900
GLANCE_DOWNLOAD_TIMEOUT = 600
CINDER_BACKUP_TIMEOUT = 10
CINDER_BACKUP_TRIES = 600
BACKUP_BASE_PATH = '/var/openstack_backup/'


#
# Subroutines
#

def get_backup_base_path(tenant_id):
    """
    Return the base directory for the backup
    Params: tenant id
    """
    return os.path.join(BACKUP_BASE_PATH, tenant_id)


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
def get_keystone_client():
    """
    Returns a keystone client object
    """
    return keystone_client.Client(auth_url=os.environ["OS_AUTH_URL"],
                                  username=os.environ["OS_USERNAME"],
                                  password=os.environ["OS_PASSWORD"],
                                  tenant_name=os.environ["OS_TENANT_NAME"])


def backup_keystone_user(tenant, user):
    """
    Backup user meta data into a json file
    Params: tenant object, user object
    """
    print "Backing up metadata of user " + user.name

    return dump_openstack_obj(user, os.path.join(get_backup_base_path(tenant.id),
                                                 "keystone",
                                                 "user_" + user.name + ".json"))


def backup_keystone(tenant):
    """
    Backup all keystone data
    Params: tenant object
    """
    backup_path = os.path.join(get_backup_base_path(tenant.id), "keystone")
    ensure_dir_exists(backup_path)

    print "Backing up metadata of tenant " + tenant.name
    dump_openstack_obj(tenant, os.path.join(backup_path, "project.json"))
    [backup_keystone_user(tenant, user) for user in tenant.list_users()]


#
# NOVA
#
def get_nova_client(tenant):
    """
    Instantiate and return a nova client
    Params: tenant object
    """
    return nova_client.Client(username=os.environ["OS_USERNAME"],
                              api_key=os.environ["OS_PASSWORD"],
                              auth_url=os.environ["OS_AUTH_URL"],
                              project_id=tenant.name)


def backup_nova_vm(tenant, srv):
    """
    Save vm meta data as json file and make a snapshot of the given vm
    Params: tenant object, nova server object
    """
    bad_status = ['Error', 'image_uploading']
    print "Backing up metadata of vm " + srv.name
    dump_openstack_obj(srv, os.path.join(get_backup_base_path(tenant.id), "nova", "vm_" + srv.name + ".json"))

    # reset vm if it's in a bad state for image uploading
    if srv.status in bad_status or getattr(srv, 'OS-EXT-STS:task_state') in bad_status:
        print "Vm " + srv.name + " in bad state " + srv.status + " (" + getattr(srv, 'OS-EXT-STS:task_state') + "). Resetting."
        srv.reset_state('active')
        sleep(1)

    print "Creating backup image of vm " + srv.name

    try:
        backup_id = srv.create_image(GLANCE_BACKUP_PREFIX + "_" + tenant.name + "_" + srv.name)
        return backup_id
    except NovaConflict, e:
        print "\nERROR creating snapshot of vm " + srv.name + "\n" + str(e) + "\n"
        return None


def backup_nova(tenant):
    """
    Backup all nova data
    Params: tenant object
    """
    backups = {}
    nova = get_nova_client(tenant)
    glance = get_glance_client()

    ensure_dir_exists(os.path.join(get_backup_base_path(tenant.id), "nova"))

    for srv in nova.servers.list():
        backup_image_id = backup_nova_vm(tenant, srv)

        if backup_image_id:
            backups[backup_image_id] = (tenant.id, srv.name)

    # wait for snapshots to finish
    wait_for_glance_upload_to_finish(backups, tenant, output_dir = "nova")


def cleanup_nova_backup(tenant):
    """
    On exit reset all active vms that are still in task image uploading
    Params: tenant object
    """
    nova = get_nova_client(tenant)
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
    keystone = get_keystone_client()
    glance_endpoint = keystone.service_catalog.url_for(service_type='image',
                                                       endpoint_type='publicURL')
    return glance_client.Client('2',glance_endpoint, token=keystone.auth_token)


def glance_check_upload(params, output_dir):
    """
    Check if an upload to glance has finished
    If one has finished start a download immediately
    Params: tupel of image id, tenant id, name for output
    Returns: True for success, False for failure or None for not finished
    """
    glance = get_glance_client()
    image_id = params[0]
    tenant_id = params[1][0]
    display_name = params[1][1]

    try:
        backup_image = glance.images.get(image_id)
        print "Upload of " + display_name + " is " + backup_image.status

        if backup_image.status.lower() == 'active':
            download_glance_image(image_id, os.path.join(get_backup_base_path(tenant_id), output_dir, display_name + ".img"))
            return (image_id, True)
    except glance_client.exc.HTTPNotFound, e:
        print "\nFailed to get status of image " + display_name + "\n" + str(e) + "\n"
        return (image_id, False)

    return (image_id, None)


# No functools.partial with multiprocess on 2.6 :(
# (see http://bugs.python.org/issue5228)
#def create_glance_check_upload(tenant, output_dir):
#    def myclosure(params):
#        return glance_check_upload(params, tenant, output_dir)
#    return myclosure

def nova_glance_check_upload(params):
    return glance_check_upload(params, "nova")

def cinder_glance_check_upload(params):
    return glance_check_upload(params, "cinder")

def wait_for_glance_upload_to_finish(backups, tenant, output_dir):
    """
    Wait until all glance uploads have finished (or failed)
    Params: dictionary of image id, display name, output_dir tenant object,
            output directory name relative to backup base path
    """
    pool = Pool()
    check_func = None
    glance = get_glance_client()
    upload_wait = GLANCE_UPLOAD_TIMEOUT

    if output_dir == "nova":
        check_func = nova_glance_check_upload
    else:
        check_func = cinder_glance_check_upload

    while 1:
        try:
            results = pool.map(check_func, backups.items())

            for (backup_id, success) in results:
                if success:
                    del backups[backup_id]
                    glance.images.delete(backup_id)

                if success == False:
                    del backups[backup_id]
                    glance.images.delete(backup_id)
        except HTTPNotFound:
            if backups.get(backup_id):
                del backups[backup_id]
        except PicklingError, e:
            print "Got error " + str(e)
        except TimeoutError:
            pass
        except KeyboardInterrupt:
            pool.terminate()

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
    except PicklingError, e:
        print "Error saving image " + image_id + ": " + str(e)
        return False
    except HTTPNotFound, e:
        print "Error downloading image " + image_id + ": " + str(e)
        return False
    finally:
        fh.close()

    return True


def backup_glance_image(params):
    """
    Dump meta data of glance image in a json file and store the image in another file
    Params: tupel of tenant_id, glance image id
    """
    tenant_id = params[0]
    img_id = params[1]

    backup_path = os.path.join(get_backup_base_path(tenant_id), "glance")

    glance = get_glance_client()
    img = glance.images.get(img_id)
    print "Backing up metadata of glance image " + img.name

    dump_openstack_obj(img, os.path.join(backup_path, img.name + ".json"))
    download_glance_image(img.id, os.path.join(backup_path, img.name + ".img"))

    return True


def backup_glance(tenant):
    """
    Backup all glance data
    Params: tenant object
    """
    ensure_dir_exists(os.path.join(get_backup_base_path(tenant.id), "glance"))
    glance = get_glance_client()
    pool = Pool()

    try:
        jobs = pool.map_async(backup_glance_image, [(tenant.id, img.id) for img in glance.images.list()])
        jobs.get()
    except KeyboardInterrupt:
        pool.terminate()


def cleanup_glance_backup():
    """
    At exit remove all glance images which names start with our backup prefix
    """
    glance = get_glance_client()
    pool = Pool()
    image_ids = (img.id for img in glance.images.list() if img.name.startswith(GLANCE_BACKUP_PREFIX))

    jobs = pool.map_async(glance.images.delete, image_ids)
    jobs.get()



#
# CINDER
#
def get_cinder_client(tenant):
    """
    Instantiate and return a cinder client object
    Params: tenant object
    """
    return cinder_client.Client('1',
                                os.environ['OS_USERNAME'],
                                os.environ['OS_PASSWORD'],
                                tenant.name,
                                os.environ['OS_AUTH_URL'])

def attach_volume(tenant, volume_id, vm_id, device):
    """
    Attach a cinder volume as device to the given vm
    Params: tenant object, volume id, vm id, device name
    """
    cinder = get_cinder_client(tenant)

    try:
        volume = cinder.volumes.get(volume_id)
        volume.attach(vm_id, device)
    except BadRequest,e :
        print "Error volume " + volume.display_name + " could not be attached on vm " + vm_id + " as device " + device + "\n" + str(e) + "\n"

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


def backup_cinder_volume(tenant, volume):
    """
    Save volume meta data as json file and trigger a backup of the volume
    Params: tenant, volume object
    """
    backup_id = None
    backup_name = None
    cinder = get_cinder_client(tenant)

    print "Backing up metadata of cinder volume " + volume.display_name
    dump_openstack_obj(volume, os.path.join(get_backup_base_path(tenant.id), "cinder", "vol_" + volume.display_name + ".json"))

    if detach_volume(volume):
        print "Backing up volume " + volume.display_name

        try:
            resp = cinder.volumes.upload_to_image(volume,
                                                  True,
                                                  GLANCE_BACKUP_PREFIX + "_" + tenant.name + "_" + volume.display_name,
                                                  "bare",
                                                  "raw")
            backup_id = resp[1]['os-volume_upload_image']['image_id']
            backup_name = resp[1]['os-volume_upload_image']['image_name']
        except BadRequest, e:
            print "ERROR volume " + volume.display_name + " could not be backuped!\n" + str(e) + "\n"
        except ClientException, e:
            print "ERROR volume " + volume.display_name + " could not be backuped!\n" + str(e) + "\n"

    return (backup_id, backup_name)


def backup_cinder(tenant):
    """
    Backup all cinder data
    Params: tenant object
    """
    backups = {}
    ensure_dir_exists(os.path.join(get_backup_base_path(tenant.id), "cinder"))
    cinder = get_cinder_client(tenant)

    for volume in cinder.volumes.list():
        (backup_id, backup_name) = backup_cinder_volume(tenant, volume)

        if backup_id:
            backups[backup_id] = (tenant.id, backup_name)

    wait_for_glance_upload_to_finish(backups, tenant, output_dir="cinder")
