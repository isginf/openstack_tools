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
import json
from glob import glob
from time import sleep
from pickle import PicklingError
from copy import deepcopy
from multiprocessing import Pool, TimeoutError
from novaclient.exceptions import Conflict as NovaConflict
import keystoneclient.v2_0.client as keystone_client
from keystoneclient.openstack.common.apiclient.exceptions import Conflict as KeystoneConflict
from keystoneclient.openstack.common.apiclient.exceptions import NotFound as KeystoneNotFound
import novaclient.v1_1.client as nova_client
from nova.compute import task_states
import glanceclient as glance_client
import cinderclient.client as cinder_client
from cinderclient.exceptions import ClientException as CinderClientException
from cinderclient.exceptions import BadRequest as CinderBadRequest
from glanceclient.exc import HTTPNotFound as GlanceNotFound
from glanceclient.exc import HTTPInternalServerError as GlanceInternalServerError


#
# Configuration
#

GLANCE_BACKUP_PREFIX = "os_bkp"
GLANCE_UPLOAD_TIMEOUT = 900
GLANCE_DOWNLOAD_TIMEOUT = 600
CINDER_BACKUP_TIMEOUT = 10
CINDER_BACKUP_TRIES = 600
BACKUP_BASE_PATH = '/var/openstack_backup/'
INITIAL_PASSWORD = "youknowgodisnotagoodpassword"


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
    if isinstance(obj, str) or isinstance(obj, list) or isinstance(obj, dict):
        dump = obj
    else:
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


def load_openstack_obj(json_file):
    """
    Read openstack object json file into dictionary
    Parameters: path to json file
    Returns dictionary
    """
    data = None

    try:
        fh = open(json_file)
        data = json.loads(fh.read())
        fh.close()
    except IOError,e:
        print "Cannot read file " + tenant_file + " " + str(e)

    return data


def ensure_dir_exists(dir):
    """
    Create directory if it doesnt exist
    Params: directory name
    """
    if not os.path.exists(dir):
        os.mkdir(dir)


def wait_for_action_to_finish(all_items, wait_timeout, check_func):
    """
    Wait until an action on all items has finished (or failed)
    Param: dictionary of all_items with image id as key and value of tenant id and display name as tupel
    Param: timeout in seconds/3
    Param: function to check if action has finished
    """
    pool = Pool()
    my_items = deepcopy(all_items)
    my_wait_timeout = deepcopy(wait_timeout)

    while 1:
        try:
            results = [check_func(x) for x in my_items.items()]

            for (item_id, success) in results:
                if success:
                    del my_items[item_id]

                # Got exception
                elif success == False:
                    del my_items[item_id]
        except (GlanceNotFound, GlanceInternalServerError):
            if my_items.get(item_id):
                del my_items[item_id]
        except TimeoutError:
            pass
        except KeyboardInterrupt:
            pool.terminate()

        if len(my_items) == 0 or my_wait_timeout == 0:
            break
        else:
            my_wait_timeout -= 1
            sleep(3)


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

    dump_openstack_obj(user, os.path.join(get_backup_base_path(tenant.id),
                                          "keystone",
                                          "user_" + user.name + ".json"))

    for role in user.list_roles(tenant.id):
        print "Storing role " + role.name + " for user " + user.name
        dump_openstack_obj(role, os.path.join(get_backup_base_path(tenant.id),
                                              "keystone",
                                              "role_" + user.name + "_" + role.name + ".json"))


def restore_keystone_user(params):
    """
    Restore a keystone user and it's roles
    Params: tupel of tenant_id, absolute name to user json file, absolute name to backup path
    """
    tenant_id = params[0]
    user_data = load_openstack_obj(params[1])
    backup_path = params[2]
    user = None

    if user_data:
        keystone = get_keystone_client()

        try:
            user = keystone.users.create(user_data['username'],
                                  INITIAL_PASSWORD,
                                  user_data['email'],
                                  tenant_id,
                                  user_data['enabled'])
            print "Restored user " + user_data['username']
        except KeystoneConflict, e:
            print "User " + user_data['username'] + " already exists"
            user = keystone.users.find(name=user_data['username'])

        for role_file in glob(os.path.join(backup_path, 'role_*.json')):
            try:
                role_data = load_openstack_obj(role_file)
                role = keystone.roles.find(name=role_data['name'])
                keystone.roles.add_user_role(user, role, tenant_id)
                print "Added user " + user.name + " to tenant with role " + role.name
            except KeystoneConflict, e:
                pass
            except KeystoneNotFound, e:
                print "Role " + role_data['name'] + " cannot be found " + str(e)


def backup_keystone(tenant):
    """
    Backup all keystone data
    Params: tenant object
    """
    backup_path = os.path.join(get_backup_base_path(tenant.id), "keystone")
    ensure_dir_exists(backup_path)

    print "Backing up metadata of tenant " + tenant.name
    dump_openstack_obj(tenant, os.path.join(backup_path, "tenant.json"))
    [backup_keystone_user(tenant, user) for user in tenant.list_users()]


def restore_keystone_tenant(tenant_data):
    """
    Restoring a keystone tenant
    Params: dictionary of tenant data
    Returns: tenant object
    """
    keystone = get_keystone_client()
    tenant = None

    try:
        tenant = keystone.tenants.create(tenant_data['name'],
                                         tenant_data['description'],
                                         tenant_data['enabled'])
        print "Restored tenant " + tenant_data['name']
    except KeystoneConflict, e:
        print "Tenant " + tenant_data['name'] + " already exists"
        tenant = keystone.tenants.find(name=tenant_data['name'])

    return tenant


def restore_keystone(tenant_id):
    """
    Restore all keystone stuff
    Params: tenant_id
    """
    backup_path = os.path.join(get_backup_base_path(tenant_id), "keystone")
    tenant_file = os.path.join(backup_path, "tenant.json")
    tenant_data = None
    tenant = None

    if os.path.exists(backup_path):
        tenant_data = load_openstack_obj(tenant_file)

        if tenant_data:
            tenant = restore_keystone_tenant(tenant_data)

            map(restore_keystone_user,
                [(tenant.id, user_file, backup_path) for user_file in glob(os.path.join(backup_path, 'user_*.json'))])
    else:
        print "ERROR " + backup_path + " does not exist!"

    return tenant


#
# NOVA
#
def get_nova_client(tenant_id):
    """
    Instantiate and return a nova client
    Params: tenant id
    """
    keystone = get_keystone_client()
    tenant = keystone.tenants.get(tenant_id)
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
    nova = get_nova_client(tenant.id)
    glance = get_glance_client()
    output_dir = os.path.join(get_backup_base_path(tenant.id), "nova")
    ensure_dir_exists(output_dir)

    for srv in nova.servers.list():
        backup_image_id = backup_nova_vm(tenant, srv)

        if backup_image_id:
            backups[backup_image_id] = (tenant.id, srv.id + "_" + srv.name)

    # wait for snapshots to finish
    wait_for_action_to_finish(backups, GLANCE_UPLOAD_TIMEOUT, nova_glance_check_upload)

    # Download images from glance and delete them afterwards
    pool = Pool()
    pool.map(download_nova_glance_image, backups.items())
    pool.map(glance_delete, backups.keys())


def nova_check_vm_got_created(params):
    """
    Check if a nova vm was successfully created
    Params: tupel of vm id, tenant id
    Returns: True for success, False for failure or None for not finished
    """
    vm_id = params[0]
    tenant_id = params[1]
    nova = get_nova_client(tenant_id)

    try:
        vm = nova.servers.get(vm_id)
        print "Status of volume " + vm.name + " is " + vm.status

        if vm.status.upper() == "ACTIVE":
            return (vm_id, True)
    except NovaConflict, e:
        print "Failed to get status of vm " + vm_id + "\n" + str(e)
        return (vm_id, False)

    return (vm_id, None)


def restore_nova_vm(params):
    """
    Restore a single vm
    Params: tuple of new tenant_id, path to vm json file, backup dir
    """
    new_tenant_id = params[0]
    vm_file = params[1]
    backup_path = params[2]
    vm_data = load_openstack_obj(vm_file)
    bkp_img_name = "vm_" + vm_data['name']
    vm_img_file = os.path.join(backup_path, vm_data['id'] + "_" + vm_data['name'] + '.img')

    nova = get_nova_client(new_tenant_id)
    glance = get_glance_client()

    print "Uploading image " + bkp_img_name
    glance_img = glance.images.create(container_format="bare",
                                      disk_format="qcow2",
                                      name=bkp_img_name,
                                      visibility="public")
    glance.images.upload(glance_img.id, open(vm_img_file, 'rb'))

    vm = nova.servers.create(vm_data['name'],
                             glance_img.id,
                             vm_data['flavor']['id'])

    wait_for_action_to_finish({vm.id: (new_tenant_id,)},
                              GLANCE_DOWNLOAD_TIMEOUT,
                              nova_check_vm_got_created)

    print "Restored vm " + vm_data['name']
    glance.images.delete(glance_img.id)


def restore_nova(old_tenant_id, new_tenant):
    """
    Restore all nova stuff
    Params: old tenant_id, new tenant object
    """
    backup_path = os.path.join(get_backup_base_path(old_tenant_id), "nova")
    map(restore_nova_vm,
        [(new_tenant.id, vm_file, backup_path) for vm_file in glob(os.path.join(backup_path, '*.json'))])


def cleanup_nova_backup(tenant):
    """
    On exit reset all active vms that are still in task image uploading
    Params: tenant object
    """
    nova = get_nova_client(tenant.id)
    pool = Pool()
    vm_ids = (vm.id for vm in nova.servers.list() if getattr(vm, 'OS-EXT-STS:task_state') == task_states.IMAGE_UPLOADING and \
                                                     vm.status.lower() == 'active')

    pool.map(lambda vm: vm.reset_state('active'), vm_ids)


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
            return (image_id, True)
    except (GlanceNotFound, GlanceInternalServerError), e:
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

def glance_delete(image_id):
    glance = get_glance_client()
    return glance.images.delete(image_id)

def download_nova_glance_image(params):
    image_id = params[0]
    tenant_id = params[1][0]
    display_name = params[1][1]
    output_dir = os.path.join(get_backup_base_path(tenant_id), "nova")
    download_glance_image(image_id, os.path.join(output_dir, display_name + ".img"))

def download_cinder_glance_image(params):
    image_id = params[0]
    tenant_id = params[1][0]
    display_name = params[1][1]
    output_dir = os.path.join(get_backup_base_path(tenant_id), "cinder")
    download_glance_image(image_id, os.path.join(output_dir, display_name + ".img"))


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
    except GlanceNotFound, e:
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

    dump_openstack_obj(img, os.path.join(backup_path, img.id + "_" + img.name + ".json"))
    download_glance_image(img.id, os.path.join(backup_path, img.id + "_" + img.name + ".img"))

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
        pool.map(backup_glance_image, [(tenant.id, img.id) for img in glance.images.list() if img.owner == tenant.id])
    except KeyboardInterrupt:
        pool.terminate()


def glance_image_exists(img_name):
    """
    Check if a glance image with the same name already exists
    Parameters: image name
    Returns boolean
    """
    glance = get_glance_client()
    return filter(lambda x: x.name == img_name, glance.images.list())


def restore_glance_image(params):
    """
    Restore a glance image
    Params: tupel of tenant_id, absolute name to image json file, absolute name to backup path
    """
    tenant_id = params[0]
    img_file = params[1]
    backup_path = params[2]
    img_data = load_openstack_obj(img_file)
    glance = get_glance_client()

    del img_data['owner']
    del img_data['updated_at']
    del img_data['file']
    del img_data['id']
    if img_data.get('size'): del img_data['size']
    if img_data.get('checksum'): del img_data['checksum']
    del img_data['created_at']
    del img_data['schema']
    del img_data['status']

    if not glance_image_exists(img_data['name']):
        glance.images.create(**img_data)
        print "Created image " + img_data['name']


def restore_glance(tenant_id):
    """
    Restore all glance stuff
    Params: tenant_id
    """
    backup_path = os.path.join(get_backup_base_path(tenant_id), "glance")

    pool = Pool()
    pool.map(restore_glance_image,
             [(tenant_id, img_file, backup_path) for img_file in glob(os.path.join(backup_path, '*.json'))])


def cleanup_glance_backup():
    """
    At exit remove all glance images which names start with our backup prefix
    """
    glance = get_glance_client()
    pool = Pool()
    image_ids = (img.id for img in glance.images.list() if img.name.startswith(GLANCE_BACKUP_PREFIX) and img.status != "deleted")

    pool.map(glance_delete, image_ids)



#
# CINDER
#
def get_cinder_client(tenant_name):
    """
    Instantiate and return a cinder client object
    Params: tenant name
    """
    return cinder_client.Client('1',
                                os.environ['OS_USERNAME'],
                                os.environ['OS_PASSWORD'],
                                tenant_name,
                                os.environ['OS_AUTH_URL'])

def attach_volume(tenant, volume_id, vm_id, device):
    """
    Attach a cinder volume as device to the given vm
    Params: tenant object, volume id, vm id, device name
    """
    cinder = get_cinder_client(tenant.name)

    try:
        volume = cinder.volumes.get(volume_id)
        volume.attach(vm_id, device)
    except CinderBadRequest,e :
        print "Error volume " + volume.display_name + " could not be attached on vm " + vm_id + " as device " + device + "\n" + str(e) + "\n"

def detach_volume(volume):
    """
    Detach the given cinder volume
    Params: volume object
    """
    if volume.status == 'in-use':
        try:
            volume.detach()
        except CinderClientException, e:
            print "ERROR volume " + volume.display_name + " could not be detached!\n" + str(e) + "\n"
            return False
    return True


def backup_cinder_volume(params):
    """
    Save volume meta data as json file and trigger a backup of the volume
    Params: tuple of tenant_id, tenant_name, volume_id
    """
    tenant_id = params[0]
    tenant_name = params[1]
    volume_id = params[2]
    backup_id = None
    backup_name = None
    cinder = get_cinder_client(tenant_name)
    volume = cinder.volumes.get(volume_id)

    print "Backing up metadata of cinder volume " + volume.display_name
    dump_openstack_obj(volume, os.path.join(get_backup_base_path(tenant_id), "cinder", "vol_" + volume_id + "_" + volume.display_name + ".json"))

    if detach_volume(volume):
        print "Backing up volume " + volume.display_name

        try:
            resp = cinder.volumes.upload_to_image(volume,
                                                  True,
                                                  GLANCE_BACKUP_PREFIX + "_" + volume.id + "_" + tenant_name + "_" + volume.display_name,
                                                  "bare",
                                                  "raw")
            backup_id = resp[1]['os-volume_upload_image']['image_id']
            backup_name = resp[1]['os-volume_upload_image']['image_name']
        except CinderBadRequest, e:
            print "ERROR volume " + volume.display_name + " could not be backuped!\n" + str(e) + "\n"
        except CinderClientException, e:
            print "ERROR volume " + volume.display_name + " could not be backuped!\n" + str(e) + "\n"

    return (backup_id, backup_name)


def backup_cinder(tenant):
    """
    Backup all cinder data
    Params: tenant object
    """
    backups = {}
    backup_params = []
    ensure_dir_exists(os.path.join(get_backup_base_path(tenant.id), "cinder"))
    cinder = get_cinder_client(tenant.name)
    glance = get_glance_client()
    pool = Pool()

    for volume in cinder.volumes.list():
        backup_params.append((tenant.id, tenant.name, volume.id))

    results = pool.map(backup_cinder_volume, backup_params)

    for result in results:
        if result[0]:
            backups[result[0]] = (tenant.id, result[1])

    wait_for_action_to_finish(backups, GLANCE_UPLOAD_TIMEOUT, cinder_glance_check_upload)

    # Download images from glance and delete them afterwards
    pool.map(download_cinder_glance_image, backups.items())
    pool.map(glance_delete, backups.keys())


def cinder_check_volume_got_created(params):
    """
    Check if a cinder volume was successfully created
    Params: tupel of image id, tenant name
    Returns: True for success, False for failure or None for not finished
    """
    vol_id = params[0]
    tenant_name = params[1][0]
    cinder = get_cinder_client(tenant_name)

    try:
        vol = cinder.volumes.get(vol_id)
        print "Status of volume " + vol.display_name + " is " + vol.status

        if vol.status.lower() == "available":
            return (vol_id, True)
    except CinderClientException, e:
        print "Failed to get status of volume " + vol_id + "\n" + str(e)
        return (vol_id, False)

    return (vol_id, None)


def restore_cinder_volume(params):
    """
    Restore a cinder volume
    Params: tuple of tenant id, tenant name, path to json file, backup directory
    """
    tenant_id = params[0]
    tenant_name = params[1]
    vol_file = params[2]
    backup_path = params[3]

    glance = get_glance_client()
    cinder = get_cinder_client(tenant_name)

    vol_data = load_openstack_obj(vol_file)
    bkp_img_name = GLANCE_BACKUP_PREFIX + "_" + vol_data['id'] + "_" + tenant_name + "_" + vol_data['display_name']
    vol_img_file = os.path.join(backup_path, bkp_img_name + ".img")

    print "Uploading image " + bkp_img_name
    glance_img = glance.images.create(container_format="bare",
                                      disk_format="qcow2",
                                      name=bkp_img_name,
                                      visibility="public")
    glance.images.upload(glance_img.id, open(vol_img_file, 'rb'))

    # Make cinder volume from glance image and delete it afterwards
    vol = cinder.volumes.create(size=vol_data['size'],
                          display_name=vol_data['display_name'],
                          display_description=vol_data['display_description'],
                          project_id=tenant_id,
                          imageRef=glance_img.id,
                          availability_zone=vol_data['availability_zone'],
                          metadata=vol_data['metadata'])

    wait_for_action_to_finish({vol.id: (tenant_name,)},
                              GLANCE_DOWNLOAD_TIMEOUT,
                              cinder_check_volume_got_created)

    print "Created volume " + vol_data['display_name']
    glance.images.delete(glance_img.id)


def restore_cinder(old_tenant_id, new_tenant):
    """
    Restore all cinder stuff
    Params: id of old tenant (used for backup on disk), new tenant object
    """
    backup_path = os.path.join(get_backup_base_path(old_tenant_id), "cinder")

    map(restore_cinder_volume,
        [(old_tenant_id, new_tenant.name, vol_file, backup_path) for vol_file in glob(os.path.join(backup_path, '*.json'))])
