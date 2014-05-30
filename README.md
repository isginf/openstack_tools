Openstack tools
================

This is a collection of Openstack tools we use to manage the private cloud at D-INFK of
the ETH Zurich.

- openstack_archiver.py to archivate all data of a project
- openstack_cinder_backup.py to periodically backup cinder volumes which names start with backupme
- openstack_migrator to automatically migrate all vms to other hypervisors on node shutdown
- openstack_remove_tenant to delete all data that belongs to a specific project


License
=======

Copyright 2014 ETH Zurich, ISGINF, Bastian Ballmann
E-Mail: bastian.ballmann@inf.ethz.ch
Web: http://www.isg.inf.ethz.ch

This is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

It is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License.
If not, see <http://www.gnu.org/licenses/>.
