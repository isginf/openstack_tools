#!/usr/bin/python

#
# Automatically migrate all vms of a given hypervisor to other nova compute nodes
#
# Copyright 2014 ETH Zurich, ISGINF, Bastian Ballmann
# E-Mail: bastian.ballmann@inf.ethz.ch
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


###[ Loading modules ]###

import os
import sys
import time
import shutil
import logging
from datetime import datetime
import novaclient.exceptions
import novaclient.v1_1.client as nvclient
from multiprocessing import Pool
from openstack_lib import get_nova_client, get_keystone_client, wait_for_action_to_finish, nova_check_migration


###[ Configuration ]###

live_migration = False
block_migration = False
migration_timeout = 180
final_wait_timeout = 300
nova_dir="/var/lib/nova"
log_level = logging.DEBUG

if len(sys.argv) == 2 and (sys.argv[1] == "--help" or sys.argv[1] == "-h"):
  print sys.argv[0] + " [hypervisor]"
  sys.exit(1)
elif len(sys.argv) < 2:
  hostname = os.uname()[1]
else:
  hostname = sys.argv[1]


###[ Subroutines ]###

offline_migrations = []
resume_vms = []
log = logging.getLogger('openstack_migrator')
logging.basicConfig(
    filename = os.path.join(nova_dir, "openstack_migrator.log"),
    filemode = "a",
    level = log_level)

# dont buffer stdout
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

def log_prefix():
    return "[%s] %s: " %(datetime.now().strftime("%d.%m.%Y %H:%M:%S"), os.uname()[1])


# get hypervisor object by its hostname
def get_hypervisor_for_host(hostname):
  try:
    hypervisor = nova.hypervisors.search(hostname, servers=True)[0]
  except Exception:
    hypervisor = None

  return hypervisor


# get all vm objects for a hypervisor
def get_vms_of_hypervisor(hypervisor):
  return map(lambda x: nova.servers.get(x.get('uuid')), hypervisor.servers)


# migrate a vm online or offline depending on its status
def migrate((tenant_id,vm_id)):
  nova = get_nova_client(tenant_id)
  vm = nova.servers.get(vm_id)

  if vm.status == "MIGRATING" or vm.status == "VERIFY_RESIZE":
      log.debug("%s vm %s is in state %s skipping migration" % (log_prefix(), vm.name, vm.status))
      return 0

  log.debug("%s Vm info %s" %(log_prefix(), vm._info))

  # if a resize dir exists in instances dir and vm is not currently
  # migrating remove it first
  resize_dir = os.path.join(nova_dir, "instances", vm.id + "_resize")
  if os.path.isdir(resize_dir):
      log.debug("%s Removing old instance resize dir %s" %(log_prefix(), resize_dir))
      shutil.rmtree(resize_dir)

  try:
    vm.lock()

    if vm.status == "SHUTOFF":
      log.info("%s offline migraion of vm %s" % (log_prefix(), vm.name))
      vm.migrate()
    else:
      vm.reset_state(state="active")
      vm = nova.servers.get(vm.id)

      if live_migration:
        log.info("%s live migraion of vm %s" % (log_prefix(), vm.name))
        vm.live_migrate(block_migration=block_migration)
      else:
        log.info("%s stopping vm %s" % (log_prefix(), vm.name,))
        vm.stop()
        time.sleep(5)
        log.info("%s offline migration of vm %s" % (log_prefix(), vm.name))
        vm = nova.servers.get(vm.id)
        vm.migrate()
    print "Migration of vm %s started.\n" % (vm.name,)
  except Exception, e:
    log.error("%s Migration of vm %s failed!\n%s" % (log_prefix(), vm.name, str(e)))
    print "Migration of vm %s failed!\n%s\n" % (vm.name, str(e))
    log.debug("%s Vm info %s" % (log_prefix(), vm._info))
  finally:
    vm.unlock()


def migrate_all_vms_of_hypervisor(hypervisor):
  vms = get_vms_of_hypervisor(hypervisor)
  vm_ids = map(lambda(vm): (tenant.id, vm.id), vms)
  pool = Pool()
  pool.map(migrate, vm_ids)
  #map(lambda vm: migrate(vm), vms)
  waiting_for_migrations = {}

  for vm in vms:
    waiting_for_migrations[vm.id] = (tenant.id, vm.name)

    if vm.status == "SHUTOFF":
      offline_migrations.append(vm)
    elif vm.status != "SHUTOFF" and not live_migration:
      offline_migrations.append(vm)
      resume_vms.append(vm)

  wait_for_action_to_finish(waiting_for_migrations, migration_timeout/3, nova_check_migration)



###[ MAIN PART ]###

# get nova client and hypervisor objects
keystone = get_keystone_client()
tenant = keystone.tenants.find(name=os.environ['OS_TENANT_NAME'])
nova = get_nova_client(tenant.id)
hypervisor = get_hypervisor_for_host(hostname)

if not hypervisor:
  print "Hypervisor " + hostname + " cannot be found"
  sys.exit(1)

# check if there are any vms, trigger live migration and wait for their completion
if hasattr(hypervisor, "servers"):
    migrate_all_vms_of_hypervisor(hypervisor)
else:
  log.info("%s Hypervisor %s serves no vms" % (log_prefix(), hostname))
  print "Hypervisor " + hostname + " serves no vms"


# Are there any vm left that were not migrateable? Try another time
hypervisor = get_hypervisor_for_host(hostname)

if hypervisor and hasattr(hypervisor, "servers"):
    migrate_all_vms_of_hypervisor(hypervisor)

    # still vms left? shut em down and migrate offline
    hypervisor = get_hypervisor_for_host(hostname)

    if hypervisor and hasattr(hypervisor, "servers"):
        for vm in get_vms_of_hypervisor(hypervisor):
            log.debug("%s Resetting state to active" % log_prefix())
            vm.reset_state(state="active")
            vm = nova.servers.get(vm.id)
            vm.stop()

        migrate_all_vms_of_hypervisor(hypervisor)


# offline migrated machines sometimes stay in state VERIFY_RESIZE, reset them
for vm in offline_migrations:
  log.debug("%s Resetting state of offline migrated vm %s" % (log_prefix(), vm.name))
  vm.reset_state(state="active")
  vm = nova.servers.get(vm.id)
  vm.stop()

# resume vms must be started
# sometimes vms hang in state resize therefore we reset and "stop" them before starting
for vm in resume_vms:
  log.info("%s starting vm %s" %(log_prefix(), vm.name))
  vm.start()

# All done. Cleanup.
logging.shutdown()
