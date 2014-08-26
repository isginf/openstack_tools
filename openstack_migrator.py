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

waiting_for_migrations = []
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


# migrate a vm online or offline depending on its status
def migrate(vm):
  global waiting_for_migrations
  global offline_migrations

  if vm.status == "MIGRATING" or vm.status == "VERIFY_RESIZE":
      log.debug("%s vm %s is in state %s skipping migration" % (log_prefix(), vm.name, vm.status))
      return 0

  sys.stdout.write("Migration of " + vm.name + " ")
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

      offline_migrations.append(vm)
    else:
      vm.reset_state(state="active")
      vm = nova.servers.get(vm.id)

      if live_migration:
        log.info("%s live migraion of vm %s" % (log_prefix(), vm.name))
        vm.live_migrate(block_migration=block_migration)
      else:
        log.info("%s stopping vm %s" % (log_prefix(), vm.name,))
        vm.stop()
        resume_vms.append(vm)
        time.sleep(5)
        log.info("%s offline migration of vm %s" % (log_prefix(), vm.name))
        vm = nova.servers.get(vm.id)
        vm.migrate()
    waiting_for_migrations.append(vm.id)
    sys.stdout.write("started\n")
  except Exception, e:
    log.error("%s Migration of vm %s failed!\n%s" % (log_prefix(), vm.name, str(e)))
    sys.stdout.write("failed!\n" + str(e) + "\n")
    log.debug("%s Vm info %s" % (log_prefix(), vm._info))
  finally:
    vm.unlock()


# wait unitl all vms in waiting_for_migration are not on this hypervisor anymore
# or until the hypervisor has no vms left at all
def wait_for_migrations_to_complete():
  global waiting_for_migrations
  timeout_not_reached = migration_timeout

  if waiting_for_migrations:
    log.info("%s Waiting for migrations to finish ..." % log_prefix())
    sys.stdout.write("\nWaiting for migrations to finish ...")

    while waiting_for_migrations and timeout_not_reached:
      sys.stdout.write(".")
      log.debug("%s %d seconds left" % (log_prefix(), timeout_not_reached))
      hypervisor = get_hypervisor_for_host(hostname)

      if not hypervisor or not hasattr(hypervisor, "servers"):
        waiting_for_migrations = []
      elif hypervisor and hasattr(hypervisor, "servers"):
        existing_vms = map(lambda x: x.get('uuid'), hypervisor.servers)
        waiting_for_migrations = filter(lambda x: x in existing_vms, waiting_for_migrations)

      timeout_not_reached -= 10
      time.sleep(10)
  sys.stdout.write("\n")


###[ MAIN PART ]###

# get nova client and hypervisor objects
nova = nvclient.Client(os.environ['OS_USERNAME'],
                       os.environ['OS_PASSWORD'],
                       os.environ['OS_TENANT_NAME'],
                       os.environ['OS_AUTH_URL'])
hypervisor = get_hypervisor_for_host(hostname)

if not hypervisor:
  print "Hypervisor " + hostname + " cannot be found"
  sys.exit(1)

# check if there are any vms, trigger live migration and wait for their completion
if hasattr(hypervisor, "servers"):
  map(lambda x: migrate(nova.servers.get(x.get('uuid'))), hypervisor.servers)
  wait_for_migrations_to_complete()
else:
  log.info("%s Hypervisor %s serves no vms" % (log_prefix(), hostname))
  print "Hypervisor " + hostname + " serves no vms"


# Are there any vm left that were not migrateable?
hypervisor = get_hypervisor_for_host(hostname)

if hypervisor and hasattr(hypervisor, "servers"):
  map(lambda vm: migrate(nova.servers.get(vm.get('uuid'))),
      hypervisor.servers)
  wait_for_migrations_to_complete()

  # still vms left? shut em down and migrate offline
  # try to migrate again after timeout
  hypervisor = get_hypervisor_for_host(hostname)

  if hypervisor and hasattr(hypervisor, "servers"):
    log.info("%s There are still vms to migrate. Waiting %d seconds..." % (log_prefix(), final_wait_timeout))
    print "\nThere are still vms to migrate. Waiting %d seconds..." % final_wait_timeout
    time.sleep(final_wait_timeout)

    for vm_dict in hypervisor.servers:
      vm = nova.servers.get(vm_dict.get('uuid'))
      log.debug("%s Resetting state to active" % log_prefix())
      vm.reset_state(state="active")
      vm = nova.servers.get(vm_dict.get('uuid'))

      log.info("%s Stopping machine %s" % (log_prefix(), vm.name))
      print "Stopping machine %s" % vm.name

      try:
        vm.stop()
      except Exception,e:
        log.error("%s Stopping failed! %s" % (log_prefix(), e))
        log.debug("%s Vm info %s" % (log_prefix(), vm._info))

      time.sleep(30)

      try:
        print "Offline migration of machine %s" % vm.name
        log.info("%s Offline migration of machine %s" % (log_prefix(), vm.name))
        vm.migrate()

        offline_migrations.append(vm)
      except Exception, e:
        log.error("%s Got exception %s" % (log_prefix(), e))
        log.debug("%s Vm info %s" % (log_prefix(), vm._info))

# offline migrated machines stay in state VERIFY_RESIZE, reset them
for vm in offline_migrations:
  log.debug("%s Resetting state of offline migrated vm %s" % (log_prefix(), vm.name))
  vm.reset_state(state="active")
  vm = nova.servers.get(vm.id)
  vm.stop()

# resume vms must be started
# sometimes vms hang in state resize therefore we reset and "stop" them before starting
for vm in resume_vms:
  log.info("%s starting vm %s" %(log_prefix(), vm.name))
  vm.reset_state(state="active")
  vm.stop()
  vm = nova.servers.get(vm.id)
  vm.start()

# All done. Cleanup.
logging.shutdown()
